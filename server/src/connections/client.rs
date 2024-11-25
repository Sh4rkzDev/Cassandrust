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
        Rows, RowsMetadaFlagsMask, ERROR, READY, RESULT,
    },
};
use shared::{get_keyspace, get_keyspace_name};

use crate::{
    connections::node::send_message,
    partitioner::murmur3::{Partitioner, ALL_NODES},
};

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

    let key = match query.is_ddl() {
        true => vec![ALL_NODES.to_string()],
        false => {
            let binding = query.get_keys();
            let read_guard = ctx.read().unwrap();
            binding
                .iter()
                .filter(|(col, _)| {
                    read_guard
                        .get_table_schema(&get_keyspace_name().unwrap(), &table)
                        .unwrap()
                        .get_primary_key()
                        .get_partition_key()
                        .contains(col)
                })
                .map(|(_, val)| val.clone())
                .collect::<Vec<_>>()
        }
    };

    println!("Key: {:?}", key);
    let mut all_rows = Vec::new();
    let nodes = partitioner.get_nodes(&key[0]).unwrap();
    let mut acks = 0;

    for node in nodes {
        if partitioner.is_me(node) {
            println!("Query received is for me");
            let mut ctx_write = ctx.write().unwrap();
            let res = query.process(&get_keyspace().join(table.clone()), &mut *ctx_write);

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

            drop(ctx_write);
            all_rows.push(res.unwrap());
            acks += 1;
            continue;
        }
        println!("Forwarding query to {}", node.id);
        let frame_type = FrameType::Query;
        let body = Body::Query(inc::query::Query {
            query: query.clone(),
            table: table.clone(),
        });
        let mut stream = TcpStream::connect((&node.ip_address[..], node.port + 1)).unwrap();
        send_message(&mut stream, frame_type, &body).unwrap();
        let res = read_inc_frame(&mut stream).unwrap();
        if let (FrameType::Result, Body::Result(result)) = res {
            all_rows.push(result.rows);
            acks += 1;
        } else {
            panic!("Invalid frame type"); // TODO
        }
    }

    let cl = frame.body.get_consistency().unwrap();
    if acks < cl.to_u16() && cl != &ConsistencyLevel::Quorum && cl != &ConsistencyLevel::All {
        let error = create_error_response(
            ErrorCode::ServerError,
            "Not enough nodes responded to query",
            None,
        );
        let response = create_response_frame(ERROR, frame.header.stream, error).unwrap();
        response.write(&mut stream).unwrap();
        return;
    }

    let rows = compare_responses(all_rows, cl);
    let (opcode, result) = match rows {
        Ok(rows) => (
            RESULT,
            create_result_response(vec_to_rows(rows, &query.get_cols(), &table, ctx)),
        ),
        Err(e) => (
            ERROR,
            create_error_response(ErrorCode::ServerError, &e.to_string(), None),
        ),
    };
    let res_frame = create_response_frame(opcode, frame.header.stream, result).unwrap();
    res_frame.write(&mut stream).unwrap();
}

fn vec_to_rows(
    rows: Option<Vec<Vec<String>>>,
    cols: &[String],
    table: &str,
    ctx: Arc<RwLock<Context>>,
) -> Option<Rows> {
    match rows {
        Some(some_rows) => {
            let cols_specs = cols
                .iter()
                .map(|col_name| {
                    ColumnSpec::new(
                        col_name.clone(),
                        DataTypeFlags::from_schema_type(
                            ctx.read()
                                .unwrap()
                                .get_table_schema(&get_keyspace_name().unwrap(), table)
                                .unwrap()
                                .get_schema_type(&col_name)
                                .unwrap(),
                        ),
                    )
                })
                .collect();
            let metadata = RowMetadata::new(
                RowsMetadaFlagsMask::GlobalTablesSpec as i32,
                cols.len() as i32,
                Some((get_keyspace_name().unwrap(), table.to_string())),
                Some(cols_specs),
            )
            .unwrap();
            Some(Rows::new(metadata, some_rows.len() as i32, some_rows))
        }
        None => None,
    }
}

fn compare_responses(
    responses: Vec<Option<Vec<Vec<String>>>>,
    cl: &ConsistencyLevel,
) -> std::io::Result<Option<Vec<Vec<String>>>> {
    let valid_responses: Vec<Vec<Vec<String>>> = responses
        .into_iter()
        .filter_map(|response| response)
        .collect();
    if valid_responses.is_empty() {
        return Ok(None);
    }
    match cl {
        ConsistencyLevel::Any | ConsistencyLevel::One => Ok(valid_responses.into_iter().next()),
        ConsistencyLevel::Two | ConsistencyLevel::Three => {
            let target = cl.to_u16();
            let mut response_count: HashMap<Vec<Vec<String>>, usize> = HashMap::new();
            for response in valid_responses.iter() {
                *response_count.entry(response.clone()).or_insert(0) += 1;
                if response_count[response] == target as usize {
                    return Ok(Some(response.clone()));
                }
            }
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Not enough nodes accomplished the query for the given consistency level",
            ))
        }
        ConsistencyLevel::Quorum => {
            let quorum = (valid_responses.len() / 2) + 1;
            let mut response_count: HashMap<Vec<Vec<String>>, usize> = HashMap::new();
            for response in valid_responses.iter() {
                *response_count.entry(response.clone()).or_insert(0) += 1;
                if response_count[response] >= quorum {
                    return Ok(Some(response.clone()));
                }
            }
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Not enough nodes accomplished the query for the given consistency level",
            ))
        }
        ConsistencyLevel::All => {
            let first_response = &valid_responses[0];
            if valid_responses.iter().all(|r| r == first_response) {
                Ok(Some(first_response.clone()))
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Not all nodes accomplished the query",
                ))
            }
        }
    }
}
