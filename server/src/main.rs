use std::{
    net::TcpListener,
    sync::{Arc, RwLock},
    thread,
};

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
    #[arg(short = 'p', long = "port")]
    port: Option<u16>,
}

fn main() {
    let node = Node::parse();
    if node.port.is_none() {
        eprintln!("Port is mandatory. Usage: server -p <port>, or you can use --help for more information.");
        std::process::exit(1);
    }

    let partitioner = Arc::new(Partitioner::read_config(node.port.unwrap()));
    let node_dir = get_workspace()
        .join("store")
        .join(&partitioner.self_node.id);
    let ctx = Arc::new(RwLock::new(initialize_context(&node_dir).unwrap()));
    set_keyspace(node_dir.join("sim"));

    let node_listener = TcpListener::bind(format!("127.0.0.1:{}", node.port.unwrap() + 1)).unwrap();
    let ctx_clone = Arc::clone(&ctx);
    let manager = Arc::new(RwLock::new(GossipManager::new(
        &partitioner.self_node,
        &partitioner.ring,
    )));
    thread::spawn(move || {
        set_keyspace(node_dir.join("sim"));
        handle_internode_communication(node_listener, ctx_clone, manager);
    });

    let listener = TcpListener::bind(format!("127.0.0.1:{}", node.port.unwrap())).unwrap();
    println!("Server listening on port {}", node.port.unwrap());
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
