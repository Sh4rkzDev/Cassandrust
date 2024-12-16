use std::{
    collections::HashMap,
    io::BufReader,
    net::TcpStream,
    sync::{Arc, RwLock},
};

use db::Context;
use inc::{read_inc_frame, Body, FrameType};
use native::{
    client::{ConsistencyLevel, STARTUP},
    server::{
        create_error_response, create_ready_response, create_response_frame,
        create_result_response, read_request, ColumnSpec, DataTypeFlags, ErrorCode, RowMetadata,
        Rows as NativeRows, RowsMetadaFlagsMask, ERROR, READY, RESULT,
    },
};
use shared::{get_keyspace, get_keyspace_name};

use crate::{
    connections::{hinted::add_hint, node::send_message, read_repair::handle_read_repair},
    partitioner::murmur3::{Partitioner, ALL_NODES},
};

pub(crate) type Row = Vec<String>;
pub(crate) type Rows = Vec<Row>;

pub fn handle_connection(
    mut stream: TcpStream,
    partitioner: &Partitioner,
    ctx: Arc<RwLock<Context>>,
) {
    let mut stream_clone = stream.try_clone().unwrap();
    let mut reader = BufReader::new(&mut stream_clone);
    let frame = read_request(&mut reader).unwrap();
    if frame.header.opcode != STARTUP {
        let error = create_error_response(
            ErrorCode::ProtocolError,
            "Connection not started with startup message",
            None,
        );
        let response = create_response_frame(ERROR, frame.header.stream, error).unwrap();
        response.write(&mut stream).unwrap();
    }
    create_response_frame(READY, frame.header.stream, create_ready_response())
        .unwrap()
        .write(&mut stream)
        .unwrap();

    println!("Waiting for query...");
    let frame = read_request(&mut reader).unwrap();
    let (mut query, table) = frame.body.get_query().unwrap();

    let key;
    if query.is_ddl() {
        key = vec![ALL_NODES.to_string()];
    } else {
        let binding = query.get_keys();
        let read_guard = ctx.read().unwrap();
        let schema = read_guard
            .get_table_schema(&get_keyspace_name().unwrap(), &table)
            .unwrap();
        drop(read_guard);
        let primary_key = schema.get_primary_key();
        let mut keys = binding
            .iter()
            .filter(|(col, _)| {
                primary_key.get_partition_key().contains(col)
                    || primary_key.get_clustering_key().contains(col)
            })
            .collect::<Vec<_>>();

        if keys.len()
            != primary_key.get_partition_key().len() + primary_key.get_clustering_key().len()
        {
            let error =
                create_error_response(ErrorCode::Invalid, "Primary key columns not provided", None);
            let response = create_response_frame(ERROR, frame.header.stream, error).unwrap();
            response.write(&mut stream).unwrap();
            return;
        }
        keys.sort_by(|(a, _), (b, _)| {
            if primary_key.get_partition_key().contains(&a) {
                return std::cmp::Ordering::Less;
            } else if primary_key.get_partition_key().contains(&b) {
                return std::cmp::Ordering::Greater;
            } else {
                return std::cmp::Ordering::Equal;
            }
        });
        key = keys.iter().map(|(_, v)| v.clone()).collect();
    };

    let mut all_rows = Vec::new();
    let nodes: Vec<_> = partitioner
        .get_nodes(&key[0])
        .unwrap()
        .into_iter()
        .cloned()
        .collect();
    let mut acks = 0;
    // Add last_update column to compare the results and return the most recent one and update the rest
    // by Read Repair.
    // For SELECT queries, the last_update column is the last one in the result, so we can just slice it
    query.add_col("last_update", &chrono::Utc::now().to_rfc3339());

    for node in &nodes {
        if partitioner.is_me(node) {
            println!("Query received is for me");
            let mut ctx_write = ctx.write().unwrap();
            let res = query.process(&get_keyspace().join(table.clone()), &mut *ctx_write);
            drop(ctx_write);

            if res.is_err() {
                let error = create_error_response(
                    ErrorCode::AlreadyExists,
                    &res.unwrap_err().to_string(),
                    Some(HashMap::from([
                        ("keyspace".to_string(), get_keyspace_name().unwrap()),
                        ("table".to_string(), table.clone()),
                    ])),
                );
                let response = create_response_frame(ERROR, frame.header.stream, error).unwrap();
                response.write(&mut stream).unwrap();
                return;
            }

            all_rows.push(res.unwrap());
            acks += 1;
            continue;
        }
        println!("Forwarding query to {}", node.ip_address);
        let frame_type = FrameType::Query;
        let query_clone = query.clone();
        let body = Body::Query(inc::query::Query {
            query: query_clone.clone(),
            table: table.clone(),
        });
        let Ok(mut stream) = TcpStream::connect((&node.ip_address[..], node.port + 1)) else {
            println!("Failed to connect to node {}", node.ip_address);
            if query_clone.is_not_select() {
                add_hint(
                    &ctx.read().unwrap().node_dir,
                    &node.ip_address,
                    &frame.body.get_query_str().unwrap(),
                );
            }
            continue;
        };
        send_message(&mut stream, frame_type, &body).unwrap();
        let res = read_inc_frame(&mut stream).unwrap();
        if let (FrameType::Result, Body::Result(result)) = res {
            all_rows.push(result.rows);
            acks += 1;
        } else {
            println!("Invalid frame type after query: {:?}", res);
        }
    }

    let cl = frame.body.get_consistency().unwrap();
    if (acks < cl.to_u16() && cl != &ConsistencyLevel::Quorum && cl != &ConsistencyLevel::All)
        || (acks < 3 && cl == &ConsistencyLevel::All)
    {
        let error = create_error_response(
            ErrorCode::ServerError,
            "Not enough nodes responded to query",
            None,
        );
        let response = create_response_frame(ERROR, frame.header.stream, error).unwrap();
        response.write(&mut stream).unwrap();
        return;
    }

    let rows = if let Some(mut rows) = compare_responses(all_rows.clone(), cl) {
        remove_last_update(&mut rows);
        Some(rows)
    } else {
        None
    };
    query.remove_col("last_update");
    let (opcode, result) = (
        RESULT,
        create_result_response(vec_to_rows(
            rows.clone(),
            &query.get_cols(),
            &table,
            ctx.clone(),
        )),
    );
    let res_frame = create_response_frame(opcode, frame.header.stream, result).unwrap();
    res_frame.write(&mut stream).unwrap();

    if rows.is_some() {
        query.add_col("last_update", &chrono::Utc::now().to_rfc3339());
        handle_read_repair(
            &table,
            query.get_cols(),
            nodes.as_slice(),
            all_rows.iter().map(|rows| rows.clone().unwrap()).collect(),
            query.get_keys(),
            partitioner,
            ctx,
        );
    }
}

fn vec_to_rows(
    rows: Option<Rows>,
    cols: &[String],
    table: &str,
    ctx: Arc<RwLock<Context>>,
) -> Option<NativeRows> {
    match rows {
        Some(some_rows) => {
            let read_guard = ctx.read().unwrap();
            let cols_specs = cols
                .iter()
                .map(|col_name| {
                    ColumnSpec::new(
                        col_name.clone(),
                        DataTypeFlags::from_schema_type(
                            read_guard
                                .get_table_schema(&get_keyspace_name().unwrap(), table)
                                .unwrap()
                                .get_schema_type(&col_name)
                                .unwrap(),
                        ),
                    )
                })
                .collect();
            drop(read_guard);
            let metadata = RowMetadata::new(
                RowsMetadaFlagsMask::GlobalTablesSpec as i32,
                cols.len() as i32,
                Some((get_keyspace_name().unwrap(), table.to_string())),
                Some(cols_specs),
            )
            .unwrap();
            Some(NativeRows::new(metadata, some_rows.len() as i32, some_rows))
        }
        None => None,
    }
}

fn compare_responses(responses: Vec<Option<Rows>>, cl: &ConsistencyLevel) -> Option<Rows> {
    let valid_responses: Vec<Rows> = responses
        .into_iter()
        .filter_map(|response| response)
        .collect();
    if valid_responses.is_empty() {
        return None;
    }
    match cl {
        ConsistencyLevel::Any | ConsistencyLevel::One => valid_responses.into_iter().next(),
        ConsistencyLevel::Two | ConsistencyLevel::Three | ConsistencyLevel::Quorum => {
            let target = if cl == &ConsistencyLevel::Quorum {
                (valid_responses.len() / 2) + 1
            } else {
                cl.to_u16() as usize
            };
            let mut response_count: HashMap<Rows, usize> = HashMap::new();
            for response in valid_responses.iter() {
                *response_count.entry(response.clone()).or_insert(0) += 1;
                if response_count[response] >= target {
                    return Some(response.clone());
                }
            }
            let mut res = valid_responses[0].clone();
            for response in valid_responses.iter().skip(1) {
                for (idx, row) in response.iter().enumerate() {
                    if row.last().unwrap() > &res[idx].last().unwrap() {
                        res[idx] = row.clone();
                    }
                }
            }
            Some(res)
        }
        ConsistencyLevel::All => {
            let first_response = &valid_responses[0];
            if valid_responses.iter().all(|r| r == first_response) {
                Some(first_response.clone())
            } else {
                let mut res = valid_responses[0].clone();
                for response in valid_responses.iter().skip(1) {
                    for (idx, row) in response.iter().enumerate() {
                        if row.last().unwrap() > &res[idx].last().unwrap() {
                            res[idx] = row.clone();
                        }
                    }
                }
                Some(res)
            }
        }
    }
}

fn remove_last_update(rows: &mut Rows) {
    for row in rows.iter_mut() {
        row.pop();
    }
}
