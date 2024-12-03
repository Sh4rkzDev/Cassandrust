use std::{
    io,
    net::{SocketAddr, TcpStream},
    path::Path,
    sync::{Arc, RwLock},
    thread,
    time::Duration,
};

use inc::{
    gossip::{peer::Peer, syn::Syn},
    read_inc_frame, Body, FrameType,
};
use rand::seq::SliceRandom;

use crate::connections::{
    hinted::{handle_hinted_handoff, has_hints},
    node::send_message,
};

use super::manager::GossipManager;

/// Start the gossip process. This will send a SYN message to 3 random peers every 5 seconds.
///
/// **MUST** be run in a separate thread and called only once.
pub(crate) fn gossip_starter(manager: Arc<RwLock<GossipManager>>, node_dir: &Path) {
    loop {
        // id, ip:port
        let selected_peers = {
            let manager_read = manager.read().unwrap();
            manager_read.self_node.write().unwrap().last_heartbeat += 1;
            let peers: Vec<(String, String)> = manager_read
                .peers
                .values()
                .map(|peer| {
                    let peer = peer.read().unwrap();
                    (
                        peer.id.clone(),
                        peer.ip.clone() + ":" + &peer.port.to_string(),
                    )
                })
                .collect();
            peers
                .choose_multiple(&mut rand::thread_rng(), 3)
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut threads = Vec::new();
        for peer in selected_peers {
            let manager_clone = Arc::clone(&manager);
            let node_dir_clone = node_dir.to_owned();
            threads.push(thread::spawn(move || {
                gossip_to_peer(
                    manager_clone,
                    peer.0.as_str(),
                    peer.1.as_str(),
                    &node_dir_clone,
                );
            }));
        }

        for thread in threads {
            if let Err(err) = thread.join() {
                eprintln!("Error by processing gossip: {:?}", err);
            }
        }

        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}

fn gossip_to_peer(
    manager: Arc<RwLock<GossipManager>>,
    peer_id: &str,
    peer_address: &str,
    node_dir: &Path,
) {
    let address = peer_address.parse::<SocketAddr>().unwrap();
    let syn = {
        let manager_read = manager.read().unwrap();
        let self_node = manager_read.self_node.read().unwrap();
        Syn {
            sender: self_node.id.clone(),
            ip: self_node.ip.clone(),
            port: self_node.port,
            heartbeat: self_node.last_heartbeat,
            known_peers: manager_read
                .peers
                .values()
                .map(|peer| {
                    let peer_data = peer.read().unwrap();
                    Peer {
                        id: peer_data.id.clone(),
                        ip: peer_data.ip.clone(),
                        port: peer_data.port,
                        last_heartbeat: peer_data.last_heartbeat,
                        alive: peer_data.alive,
                    }
                })
                .collect(),
        }
    };
    let body = Body::Syn(syn);
    let Ok(mut stream) = TcpStream::connect(address) else {
        println!("Error while trying to connect to {}", peer_id);
        let manager_read = manager.read().unwrap();
        let mut peer = manager_read.peers.get(peer_id).unwrap().write().unwrap();
        peer.alive = false;
        return;
    };
    if send_message(&mut stream, FrameType::Syn, &body).is_err() {
        let manager_read = manager.read().unwrap();
        let mut peer = manager_read.peers.get(peer_id).unwrap().write().unwrap();
        peer.alive = false;
        return;
    }

    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let frame = match read_inc_frame(&mut stream) {
        Ok(frame) => frame,
        Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
            println!("Timeout by waiting Ack from {peer_id}");
            let manager_read = manager.read().unwrap();
            let mut peer = manager_read.peers.get(peer_id).unwrap().write().unwrap();
            peer.alive = false;
            return;
        }
        Err(e) => {
            println!("Error while trying to read Ack from {}: {:?}", peer_id, e);
            let manager_read = manager.read().unwrap();
            let mut peer = manager_read.peers.get(peer_id).unwrap().write().unwrap();
            peer.alive = false;
            return;
        }
    };
    match frame {
        (FrameType::Ack, Body::Ack(ack)) => {
            let mut new_peers = Vec::new();
            {
                let manager_read = manager.read().unwrap();

                let mut peer = manager_read.peers.get(peer_id).unwrap().write().unwrap();
                peer.alive = true;
                peer.last_heartbeat = ack.heartbeat;

                ack.update_peers.iter().for_each(|peer| {
                    if peer.id == manager_read.self_node.read().unwrap().id {
                        return;
                    }
                    if let Some(peer_lock) = manager_read.peers.get(&peer.id) {
                        let mut peer_data = peer_lock.write().unwrap();
                        if peer_data.last_heartbeat < peer.last_heartbeat {
                            peer_data.last_heartbeat = peer.last_heartbeat;
                            peer_data.alive = peer.alive;
                        }
                    } else {
                        new_peers.push(peer.clone());
                    }
                });
            }

            if !new_peers.is_empty() {
                let mut manager_write = manager.write().unwrap();
                for peer in new_peers {
                    manager_write.add_peer(peer.id.clone(), peer.ip.clone(), peer.port);
                    if let Some(peer_lock) = manager_write.peers.get_mut(&peer.id) {
                        let mut peer_data = peer_lock.write().unwrap();
                        peer_data.last_heartbeat = peer.last_heartbeat;
                        peer_data.alive = peer.alive;
                    }
                }
            }
        }
        _ => {
            println!("Invalid frame type");
            let manager_read = manager.read().unwrap();
            let mut peer = manager_read.peers.get(peer_id).unwrap().write().unwrap();
            peer.alive = false;
        }
    }
    if has_hints(node_dir, peer_id) {
        handle_hinted_handoff(node_dir, peer_id, peer_address);
    }
}
