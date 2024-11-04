use std::{io::BufReader, net::TcpStream};

use native::native_protocol::native::read_frame;

pub fn handle_connection(stream: TcpStream) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream);
    let frame = read_frame(&mut reader)?;
    println!("Frame received: {:?}", frame);
    Ok(())
}
