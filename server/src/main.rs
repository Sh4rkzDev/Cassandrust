use std::{net::TcpListener, thread};

use clap::Parser;
use connections::connection::handle_connection;
use partitioner::murmur3::Partitioner;

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
    let partitioner = Partitioner::read_config();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", node.port.unwrap())).unwrap();
    println!("Server listening on port {}", node.port.unwrap());
    while let Ok(stream) = listener.accept() {
        thread::spawn(move || {
            handle_connection(stream.0);
        });
    }
}
