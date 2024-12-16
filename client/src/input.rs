use std::{
    io::{stdin, BufReader},
    net::{SocketAddr, TcpStream},
};

use native::{
    client::{create_request, read_response, ConsistencyLevel, QUERY, READY, RESULT, STARTUP},
    server::ERROR,
};

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
        input.clear();
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
    let Ok(mut stream) = TcpStream::connect(addr) else {
        println!("Failed to connect to ip: {addr}");
        return;
    };
    let frame = create_request(STARTUP, 1, None, None).unwrap();

    frame.write(&mut stream).unwrap();

    let mut reader = BufReader::new(&mut stream);
    let frame = read_response(&mut reader).unwrap();
    drop(reader);
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

    let mut cl = String::new();
    println!("Enter consistency level (ONE, TWO, THREE, QUORUM, ALL):");
    stdin().read_line(&mut cl).unwrap();

    let consistency = ConsistencyLevel::from_str(cl.trim()).unwrap_or_else(|_| {
        println!("Invalid consistency level, using ONE");
        ConsistencyLevel::One
    });

    let frame = create_request(QUERY, 1, Some(&buffer), Some(consistency)).unwrap();

    frame.write(&mut stream).unwrap();

    let mut reader = BufReader::new(&mut stream);
    let res_frame = read_response(&mut reader).unwrap();
    match res_frame.header.opcode {
        RESULT => {
            if let Some(rows) = res_frame.body.get_rows() {
                println!("Rows:");
                for row in rows {
                    println!("\t{}", row.join(", "));
                }
            } else {
                println!("No rows returned");
            }
        }
        ERROR => {
            println!("Error: {:?}", res_frame.body.get_error().unwrap());
        }
        _ => {
            println!("Invalid response!");
        }
    }
    println!("");
}
