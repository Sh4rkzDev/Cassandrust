use std::{
    net::TcpStream,
    sync::{Arc, RwLock},
};

use inc::{
    gossip::{ack::Ack, peer::Peer, syn::Syn},
    Body, FrameType,
};

use crate::connections::node::send_message;

use super::manager::GossipManager;

pub(crate) fn handle_gossip(syn: Syn, mut stream: TcpStream, manager: Arc<RwLock<GossipManager>>) {
    let mut new_peers = Vec::new();
    let mut send_peers = Vec::new();
    let heartbeat;
    {
        let manager_read = manager.read().unwrap();
        heartbeat = manager_read.self_node.read().unwrap().last_heartbeat;

        // Update the peer that sent the SYN
        if let Some(peer_lock) = manager_read.peers.get(&syn.sender) {
            let mut peer = peer_lock.write().unwrap();
            peer.last_heartbeat = syn.heartbeat;
            peer.alive = true;
        } else {
            new_peers.push(Peer {
                id: syn.sender.clone(),
                ip: syn.ip,
                port: syn.port,
                last_heartbeat: syn.heartbeat,
                alive: true,
            });
        }

        // Look for peers that the sender doesn't know about
        for peer_lock in manager_read.peers.values() {
            let peer_data = peer_lock.read().unwrap();
            if syn.known_peers.iter().all(|p| p.id != peer_data.id) {
                send_peers.push(peer_data.clone());
            }
        }

        // Update the peers that the sender knows about
        for peer in &syn.known_peers {
            if let Some(peer_lock) = manager_read.peers.get(&peer.id) {
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
            manager_write.add_peer(peer.id.clone(), peer.ip.clone(), peer.port);
            if let Some(peer_lock) = manager_write.peers.get_mut(&peer.id) {
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
}
