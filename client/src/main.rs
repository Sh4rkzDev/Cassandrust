use std::{
    io::stdin,
    net::{SocketAddr, TcpStream},
};

use native::client::{create_request, QUERY, STARTUP};

fn main() {
    let addr: SocketAddr = "127.0.0.1:9042".parse().unwrap();
    let mut stream = TcpStream::connect(addr).unwrap();

    println!("Enter a query:");
    let mut buffer = String::new();
    stdin().read_line(&mut buffer).unwrap();

    let frame = create_request(QUERY, 1, Some(&buffer)).unwrap();

    frame.write(&mut stream).unwrap();

    println!("Frame sent: {:?}", frame);
}
