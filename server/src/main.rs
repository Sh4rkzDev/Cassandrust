use std::{
    net::TcpListener,
    sync::{Arc, RwLock},
    thread,
};

use chrono::Local;
use clap::Parser;
use connections::{
    client::handle_connection, gossip::manager::GossipManager, node::handle_internode_communication,
};
use db::initialize_context;
use partitioner::murmur3::Partitioner;
use shared::{get_keyspace, get_workspace, set_keyspace};

mod connections;
mod partitioner;

#[derive(Parser)]
struct Node {
    #[arg(short = 'n', long = "node")]
    ip: Option<String>,
}

fn main() {
    let node = Node::parse();
    if node.ip.is_none() {
        eprintln!(
            "Ip is mandatory. Usage: server -n <ip>, or you can use --help for more information."
        );
        std::process::exit(1);
    }

    let partitioner = Arc::new(Partitioner::read_config(node.ip.unwrap()));
    let node_dir = get_workspace().join("data");
    let ctx = Arc::new(RwLock::new(initialize_context(&node_dir).unwrap()));
    set_keyspace(node_dir.join("sim"));

    let node_listener = TcpListener::bind("0.0.0.0:9043").unwrap();
    let ctx_clone = Arc::clone(&ctx);
    let manager = Arc::new(RwLock::new(GossipManager::new(
        &partitioner.self_node,
        &partitioner.ring,
    )));
    thread::spawn(move || {
        set_keyspace(node_dir.join("sim"));
        handle_internode_communication(node_listener, ctx_clone, manager);
    });

    let listener = TcpListener::bind("0.0.0.0:9042").unwrap();
    println!(
        "Server up and listening at {}",
        Local::now().format("%Y-%m-%d %H:%M:%S")
    );
    // generate_sample_keys_and_hashes(50);
    while let Ok(stream) = listener.accept() {
        let partitioner = std::sync::Arc::clone(&partitioner);
        let ctx_clone = Arc::clone(&ctx);
        let ks = get_keyspace();
        let ks_clone = ks.clone();
        thread::spawn(move || {
            set_keyspace(ks_clone);
            handle_connection(stream.0, &partitioner, ctx_clone);
        });
    }
}
