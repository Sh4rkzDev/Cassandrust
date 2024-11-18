use std::{
    io::{stdin, BufReader},
    net::{SocketAddr, TcpStream},
};

use native::client::{create_request, read_response, QUERY, READY, STARTUP};

pub(crate) fn handle_input() {
    let mut input = String::new();
    println!("Enter a command:");
    while stdin().read_line(&mut input).is_ok() {
        let parts: Vec<&str> = input.split_whitespace().collect();
        match parts[0] {
            "CONNECT" => handle_connection(parts),
            _ => {
                println!("Invalid command");
            }
        }
        println!("Enter a command:");
    }
}

fn handle_connection(parts: Vec<&str>) {
    let addr = if parts.len() == 2 {
        match parts[1].parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(_) => {
                println!("Invalid address");
                return;
            }
        }
    } else {
        "127.0.0.1:9042".parse().unwrap()
    };
    let mut stream = TcpStream::connect(addr).unwrap();
    let frame = create_request(STARTUP, 1, None).unwrap();

    frame.write(&mut stream).unwrap();
    println!("Frame sent: {:?}", frame);

    let mut reader = BufReader::new(&mut stream);
    let frame = read_response(&mut reader).unwrap();
    drop(reader);
    println!("Frame received: {:?}", frame);
    if frame.header.opcode != READY {
        println!("Server not ready");
        return;
    }

    println!("Enter a query:");
    let mut buffer = String::new();
    stdin().read_line(&mut buffer).unwrap();

    if buffer == "DISCONNECT\n" {
        return;
    }

    let frame = create_request(QUERY, 1, Some(&buffer)).unwrap();

    frame.write(&mut stream).unwrap();

    println!("Frame sent: {:?}", frame);

    let mut reader = BufReader::new(&mut stream);
    let res_frame = read_response(&mut reader).unwrap();
    println!("Rows: {:?}", res_frame.body.get_rows());
}
