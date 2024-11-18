use std::{
    io::BufReader,
    net::TcpStream,
    sync::{Arc, RwLock},
};

use db::Context;
use inc::{read_inc_frame, Body, FrameType};
use native::{
    client::STARTUP,
    server::{
        create_error_response, create_ready_response, create_response_frame,
        create_result_response, read_request, ColumnSpec, DataTypeFlags, ErrorCode, RowMetadata,
        Rows, RowsMetadaFlagsMask, ERROR, READY, RESULT,
    },
};
use shared::{get_keyspace, get_keyspace_name};

use crate::{connections::node::send_message, partitioner::murmur3::Partitioner};

pub fn handle_connection(
    mut stream: TcpStream,
    partitioner: &Partitioner,
    ctx: Arc<RwLock<Context>>,
) {
    let mut stream_clone = stream.try_clone().unwrap();
    let mut reader = BufReader::new(&mut stream_clone);
    let frame = read_request(&mut reader).unwrap();
    println!("Frame received: {:?}", frame);
    if frame.header.opcode != STARTUP {
        let error = create_error_response(
            ErrorCode::ProtocolError,
            "Connection not started with startup message",
        );
        let response = create_response_frame(ERROR, frame.header.stream, error).unwrap();
        println!("Error frame: {:?}", response);
        response.write(&mut stream).unwrap();
    }
    create_response_frame(READY, frame.header.stream, create_ready_response())
        .unwrap()
        .write(&mut stream)
        .unwrap();

    println!("Waiting for query...");
    let frame = read_request(&mut reader).unwrap();
    let mut query = frame.body.get_query().unwrap();
    println!("Frame received: {:?}", frame);

    let binding = query.0.get_keys();
    let key = binding
        .iter()
        .filter(|key| {
            ctx.read()
                .unwrap()
                .get_table_schema(&get_keyspace_name().unwrap(), &query.1)
                .unwrap()
                .get_primary_key()
                .get_partition_key()
                .contains(key)
        })
        .collect::<Vec<_>>();

    let rows;

    if partitioner.is_for_me(&key[0]) {
        println!("Query received is for me");
        let mut ctx_write = ctx.write().unwrap();
        let res = query
            .0
            .process(&get_keyspace().join(query.1.clone()), &mut *ctx_write)
            .unwrap();
        drop(ctx_write);
        rows = vec_to_rows(res, &query.0.get_cols(), &query.1, ctx);
    } else {
        println!("Forwarding query to another node");
        let node = partitioner.get_node(&key[0]).unwrap();
        let frame_type = FrameType::Query;
        let query = frame.body.get_query().unwrap();
        let body = Body::Query(inc::query::Query {
            query: query.0.clone(),
            table: query.1.clone(),
        });
        let mut stream = TcpStream::connect((&node.ip_address[..], node.port + 1)).unwrap();
        send_message(&mut stream, frame_type, &body).unwrap();
        let res = read_inc_frame(&mut stream).unwrap();
        if let (FrameType::Result, Body::Result(result)) = res {
            rows = vec_to_rows(result.rows, &query.0.get_cols(), &query.1, ctx)
        } else {
            panic!("Invalid frame type"); // TODO
        }
    }
    let result = create_result_response(rows);
    let res_frame = create_response_frame(RESULT, frame.header.stream, result).unwrap();
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
