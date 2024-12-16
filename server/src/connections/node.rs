use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    sync::{Arc, RwLock},
    thread,
};

use db::Context;
use inc::{read_inc_frame, result::Result, write_inc_frame, Body, FrameType};
use shared::{get_keyspace, set_keyspace};

use crate::connections::gossip::handler::handle_gossip;

use super::gossip::{manager::GossipManager, starter::gossip_starter};

pub(crate) fn handle_internode_communication(
    socket: TcpListener,
    ctx: Arc<RwLock<Context>>,
    manager: Arc<RwLock<GossipManager>>,
) {
    let ks = get_keyspace();
    let manager_clone = Arc::clone(&manager);
    let node_dir = ctx.read().unwrap().node_dir.clone();
    thread::spawn(move || {
        gossip_starter(manager_clone, &node_dir);
    });

    while let Ok(stream) = socket.accept() {
        let ctx_clone = Arc::clone(&ctx);
        let ks_clone = ks.clone();
        let manager_clone = Arc::clone(&manager);
        thread::spawn(move || {
            set_keyspace(ks_clone);
            handle_connection(stream.0, ctx_clone, manager_clone);
        });
    }
}

fn handle_connection(
    mut stream: TcpStream,
    ctx: Arc<RwLock<Context>>,
    manager: Arc<RwLock<GossipManager>>,
) {
    let frame = read_inc_frame(&mut stream).unwrap();
    match frame {
        (FrameType::Query, Body::Query(mut query)) => {
            println!("Received query from internode: '{:?}'", query.query);
            let res = query
                .query
                .process(&get_keyspace().join(query.table), &mut ctx.write().unwrap())
                .unwrap();
            send_message(
                &mut stream,
                FrameType::Result,
                &Body::Result(Result { rows: res }),
            )
            .unwrap_or_else(|_| {});
        }
        (FrameType::Syn, Body::Syn(syn)) => {
            println!("Received gossip");
            handle_gossip(syn, stream, manager, &ctx.read().unwrap().node_dir);
        }
        (FrameType::Hinted, Body::Hinted(mut hinted)) => {
            println!("Starting hinted handoff process");
            for query in hinted.queries.iter_mut() {
                query
                    .query
                    .process(
                        &get_keyspace().join(query.table.clone()),
                        &mut ctx.write().unwrap(),
                    )
                    .unwrap();
            }
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
