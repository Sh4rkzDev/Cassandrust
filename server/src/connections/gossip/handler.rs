use std::{
    net::TcpStream,
    path::Path,
    sync::{Arc, RwLock},
};

use inc::{
    gossip::{ack::Ack, peer::Peer, syn::Syn},
    Body, FrameType,
};

use crate::connections::{
    hinted::{handle_hinted_handoff, has_hints},
    node::send_message,
};

use super::manager::GossipManager;

/// Handle a gossip message from a peer
pub(crate) fn handle_gossip(
    syn: Syn,
    mut stream: TcpStream,
    manager: Arc<RwLock<GossipManager>>,
    node_dir: &Path,
) {
    let mut new_peers = Vec::new();
    let mut send_peers = Vec::new();
    let heartbeat;
    {
        let manager_read = manager.read().unwrap();
        let my_id;
        {
            let self_node = manager_read.self_node.read().unwrap();
            my_id = self_node.ip.clone();
            heartbeat = self_node.last_heartbeat;
        }

        // Update the peer that sent the SYN
        if let Some(peer_lock) = manager_read.peers.get(&syn.sender) {
            let mut peer = peer_lock.write().unwrap();
            peer.last_heartbeat = syn.heartbeat;
            peer.alive = true;
        } else {
            new_peers.push(Peer {
                ip: syn.ip.clone(),
                port: syn.port,
                last_heartbeat: syn.heartbeat,
                alive: true,
            });
        }

        // Look for peers that the sender doesn't know about
        for peer_lock in manager_read.peers.values() {
            let peer_data = peer_lock.read().unwrap();
            if syn.known_peers.iter().all(|p| p.ip != peer_data.ip) {
                send_peers.push(peer_data.clone());
            }
        }

        // Update the peers that the sender knows about
        for peer in &syn.known_peers {
            if peer.ip == my_id {
                continue;
            }
            if let Some(peer_lock) = manager_read.peers.get(&peer.ip) {
                let mut peer_data = peer_lock.write().unwrap();
                // Maybe until I get the write lock, the peer has been updated
                if peer_data.last_heartbeat < peer.last_heartbeat {
                    peer_data.last_heartbeat = peer.last_heartbeat;
                    peer_data.alive = peer.alive;
                }
            } else {
                new_peers.push(peer.clone());
            }
        }
    }
    if !new_peers.is_empty() {
        let mut manager_write = manager.write().unwrap();
        for peer in new_peers {
            manager_write.add_peer(peer.ip.clone(), peer.port);
            if let Some(peer_lock) = manager_write.peers.get_mut(&peer.ip) {
                let mut peer_data = peer_lock.write().unwrap();
                peer_data.last_heartbeat = peer.last_heartbeat;
                peer_data.alive = peer.alive;
            }
        }
    }

    let body = Body::Ack(Ack {
        heartbeat,
        update_peers: send_peers,
    });
    send_message(&mut stream, FrameType::Ack, &body).unwrap();

    if has_hints(node_dir, &syn.sender) {
        handle_hinted_handoff(node_dir, &syn.sender, &format!("{}:{}", syn.ip, syn.port));
    }
}
