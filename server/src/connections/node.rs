use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    sync::{Arc, RwLock},
    thread,
};

use db::Context;
use inc::{read_inc_frame, result::Result, write_inc_frame, Body, FrameType};
use shared::{get_keyspace, set_keyspace};

pub(crate) fn handle_internode_communication(socket: TcpListener, ctx: Arc<RwLock<Context>>) {
    println!("Handling internode communication");
    let ks = get_keyspace();

    while let Ok(stream) = socket.accept() {
        let ctx_clone = Arc::clone(&ctx);
        let ks_clone = ks.clone();
        thread::spawn(move || {
            set_keyspace(ks_clone);
            handle_connection(stream.0, ctx_clone);
        });
    }
}

fn handle_connection(mut stream: TcpStream, ctx: Arc<RwLock<Context>>) {
    let frame = read_inc_frame(&mut stream).unwrap();
    match frame {
        (FrameType::Query, Body::Query(mut query)) => {
            println!("Query received: {:?}", query);
            let res = query
                .query
                .process(&get_keyspace().join(query.table), &mut ctx.write().unwrap())
                .unwrap();
            send_message(
                &mut stream,
                FrameType::Result,
                &Body::Result(Result { rows: res }),
            )
            .unwrap();
        }
        (FrameType::Result, Body::Result(result)) => {
            println!("Result received: {:?}", result);
        }
        _ => {
            println!("Invalid frame type");
        }
    }
}

pub(crate) fn send_message<W: Write>(
    writer: &mut W,
    frame_type: FrameType,
    body: &Body,
) -> std::io::Result<()> {
    write_inc_frame(writer, frame_type, body)
}
