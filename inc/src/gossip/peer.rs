use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Peer {
    pub ip: String,
    pub port: u16,
    pub last_heartbeat: u64,
    pub alive: bool,
}
