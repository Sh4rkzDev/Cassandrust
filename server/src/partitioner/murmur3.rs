use shared::not_found_error;

use super::node::{load_nodes_config, Node};

pub struct Partitioner {
    pub(crate) ring: Vec<Node>,
    pub(crate) self_node: Node,
}

impl Partitioner {
    /// Reads the configuration file and returns a new Partitioner instance.  
    /// **Must** execute at node startup.
    #[must_use]
    pub fn read_config(port: u16) -> Self {
        let nodes = load_nodes_config().unwrap();
        let self_node = nodes.iter().find(|node| node.port == port).unwrap().clone();
        Self {
            ring: nodes,
            self_node,
        }
    }

    pub fn get_node(&self, key: &str) -> std::io::Result<&Node> {
        let hash = murmur3::murmur3_x64_128(&mut key.as_bytes(), 0)? as i64;
        for node in &self.ring {
            if hash >= node.token_range.start && hash <= node.token_range.end {
                return Ok(node);
            }
        }
        Err(not_found_error!("Node not found"))
    }

    pub fn is_for_me(&self, key: &str) -> bool {
        let hash = murmur3::murmur3_x64_128(&mut key.as_bytes(), 0).unwrap() as i64;
        hash >= self.self_node.token_range.start && hash <= self.self_node.token_range.end
    }
}
