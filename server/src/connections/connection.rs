use std::{io::BufReader, net::TcpStream};

use native::server::read_request;

pub fn handle_connection(stream: TcpStream) {
    let mut reader = BufReader::new(stream);
    let frame = read_request(&mut reader).unwrap();
    println!("Frame received: {:?}", frame);
}
