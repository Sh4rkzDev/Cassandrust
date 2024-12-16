use std::{
    net::TcpStream,
    sync::{Arc, RwLock},
};

use db::Context;
use inc::{Body, FrameType};
use query::process_query;
use shared::get_keyspace;

use crate::partitioner::{murmur3::Partitioner, node::Node};

use super::{client::Rows, node::send_message};

pub(crate) fn handle_read_repair(
    table: &str,
    cols: Vec<String>,
    nodes: &[Node],
    responses: Vec<Rows>,
    keys: Vec<(String, String)>,
    partitioner: &Partitioner,
    ctx: Arc<RwLock<Context>>,
) {
    let updates = get_nodes_updates(&responses);
    for (idx, node) in nodes.iter().enumerate() {
        if updates[idx].is_empty() {
            continue;
        }
        let query_str = format!(
            "UPDATE {table} SET {} WHERE {} ",
            updates[idx]
                .iter()
                .map(|row| {
                    let mut set = Vec::new();
                    for (i, col) in cols.iter().enumerate() {
                        set.push(format!("{} = {}", col, row[i]));
                    }
                    set.join(", ")
                })
                .collect::<Vec<String>>()
                .join(", "),
            keys.iter()
                .map(|(col, val)| format!("{} = {}", col, val))
                .collect::<Vec<String>>()
                .join(" AND "),
        );
        println!(
            "Read repairing query for {}: {}",
            node.ip_address, query_str
        );
        let mut query = process_query(&query_str).unwrap();
        if partitioner.is_me(node) {
            query
                .0
                .process(
                    &get_keyspace().join(table.to_owned()),
                    &mut ctx.write().unwrap(),
                )
                .unwrap();
        } else {
            let body = Body::Query(inc::query::Query {
                table: table.to_string(),
                query: query.0,
            });
            let Ok(mut stream) = TcpStream::connect((&node.ip_address[..], node.port + 1)) else {
                println!(
                    "Failed to connect to node {} for read repairing.",
                    node.ip_address
                );
                continue;
            };
            send_message(&mut stream, FrameType::Query, &body).unwrap();
        }
    }
}

fn get_nodes_updates(responses: &[Rows]) -> Vec<Rows> {
    let mut updates = vec![Rows::new(); responses.len()];
    for i in 0..responses[0].len() {
        let mut max = &responses[0][i];
        for res in responses.iter() {
            if res[i].last().unwrap() > max.last().unwrap() {
                max = &res[i];
            }
        }
        // assumes there is only one row for simplicity, but it can be extended to multiple rows
        for (idx, response) in responses.iter().enumerate() {
            if response[0].last().unwrap() != max.last().unwrap() {
                updates[idx].push(max.clone());
            }
        }
    }
    updates
}
