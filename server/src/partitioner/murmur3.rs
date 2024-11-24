use murmur3::murmur3_x64_128;
use rand::{distributions::Alphanumeric, Rng};
use shared::not_found_error;

use super::node::{load_nodes_config, Node};

pub struct Partitioner {
    pub(crate) ring: Vec<Node>,
    pub(crate) self_node: Node,
}

impl Partitioner {
    /// Reads the configuration file and returns a new Partitioner instance.  
    /// **Must** be execute at node startup.
    #[must_use]
    pub fn read_config(port: u16) -> Self {
        let nodes = load_nodes_config().unwrap();
        let self_node = nodes.iter().find(|node| node.port == port).unwrap().clone();
        Self {
            ring: nodes,
            self_node,
        }
    }

    /// Returns the nodes that are responsible for the given key.
    pub fn get_nodes(&self, key: &str) -> std::io::Result<Vec<&Node>> {
        let hash = murmur3_x64_128(&mut key.as_bytes(), 0)? as i64;
        let mut nodes = Vec::new();
        let mut found = -1;
        for (idx, node) in self.ring.iter().enumerate() {
            if hash >= node.token_range.start && hash <= node.token_range.end {
                nodes.push(node);
                found = idx as i32;
                break;
            }
        }
        if found == -1 {
            return Err(not_found_error!("Node not found"));
        }
        for offset in 1..=2 {
            let next_idx = (found + offset) % self.ring.len() as i32;
            nodes.push(&self.ring[next_idx as usize]);
        }
        Ok(nodes)
    }

    pub fn is_me(&self, node: &Node) -> bool {
        node.id == self.self_node.id
    }
}

pub fn generate_sample_keys_and_hashes(sample_size: usize) {
    let mut rng = rand::thread_rng();

    let token_range = (-1844674407370955161_i64, 1844674407370955161_i64);

    println!("Token Range: {:?}", token_range);

    for _ in 0..sample_size {
        let key: String = (0..3).map(|_| rng.sample(Alphanumeric) as char).collect();

        let hash = murmur3_x64_128(&mut key.as_bytes(), 0).unwrap_or_default() as i64;

        let in_range = hash >= token_range.0 && hash <= token_range.1;

        println!(
            "Key: {:<15} | Hash: {:<20} | In Range: {}",
            key, hash, in_range
        );
    }
}
