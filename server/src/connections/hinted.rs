use std::{
    fs::File,
    io::{BufRead, BufReader},
    net::TcpStream,
    path::Path,
};

use inc::{Body, FrameType};
use query::process_query;

use super::node::send_message;

pub(crate) fn has_hints(node_dir: &Path, node: &str) -> bool {
    let hints_dir = node_dir.join("hints");
    hints_dir.join(node).with_extension("txt").exists()
}

pub(crate) fn handle_hinted_handoff(node_dir: &Path, peer_id: &str, peer_addr: &str) {
    let hints_dir = node_dir.join("hints");
    let node_hints = hints_dir.join(peer_id).with_extension("txt");
    if !node_hints.exists() {
        println!("No hints for {peer_id}");
        return;
    }
    let reader = BufReader::new(std::fs::File::open(&node_hints).unwrap());
    let mut stream = TcpStream::connect(peer_addr).unwrap();
    for hint in reader.lines() {
        let hint = hint.unwrap();
        if hint.is_empty() {
            continue;
        }
        let query = process_query(&hint).unwrap();
        let body = Body::Query(inc::query::Query {
            table: query.1,
            query: query.0,
        });
        match send_message(&mut stream, FrameType::Query, &body) {
            Ok(_) => {}
            Err(e) => {
                println!("Failed to send hinted handoff to {}: {}", peer_id, e);
                return;
            }
        }
    }
    std::fs::remove_file(node_hints).unwrap();
}

pub(crate) fn add_hint(node_dir: &Path, node: &str, query_str: &str) {
    let node_hints = node_dir.join("hints").join(node).with_extension("txt");
    if !node_hints.exists() {
        File::create(&node_hints).unwrap();
    }
    std::fs::write(&node_hints, query_str.to_owned() + "\n").unwrap();
}
