use std::{collections::HashMap, sync::RwLock};

use inc::gossip::peer::Peer;

use crate::partitioner::node::Node;

pub(crate) struct GossipManager {
    pub(crate) self_node: RwLock<Peer>,
    pub(crate) peers: HashMap<String, RwLock<Peer>>,
}

impl GossipManager {
    pub(crate) fn new(self_node: &Node, nodes: &[Node]) -> Self {
        let mut peers = HashMap::new();
        for node in nodes {
            if node.id == self_node.id {
                continue;
            }
            peers.insert(
                node.id.clone(),
                RwLock::new(Peer {
                    id: node.id.clone(),
                    ip: node.ip_address.clone(),
                    port: node.port + 1,
                    last_heartbeat: 0,
                    alive: false,
                }),
            );
        }

        GossipManager {
            self_node: RwLock::new(Peer {
                id: self_node.id.clone(),
                ip: self_node.ip_address.clone(),
                port: self_node.port,
                last_heartbeat: 0,
                alive: true,
            }),
            peers,
        }
    }

    pub(crate) fn add_peer(&mut self, id: String, ip: String, port: u16) {
        self.peers.insert(
            id.clone(),
            RwLock::new(Peer {
                id,
                ip,
                port,
                last_heartbeat: 0,
                alive: true,
            }),
        );
    }

    #[allow(dead_code)]
    pub(crate) fn remove_peer(&mut self, id: &str) {
        self.peers.remove(id);
    }
}
