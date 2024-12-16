use std::{fs::File, io::BufReader};

use serde::{Deserialize, Serialize};
use shared::get_workspace;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TokenRange {
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Node {
    pub ip_address: String,
    pub port: u16,
    pub token_range: TokenRange,
}

#[derive(Debug, Deserialize, Serialize)]
struct Config {
    nodes: Vec<Node>,
}

pub(crate) fn load_nodes_config() -> std::io::Result<Vec<Node>> {
    let file = File::open(get_workspace().join("cassandra.json"))?;
    let reader = BufReader::new(file);
    let config: Config = serde_json::from_reader(reader)?;
    Ok(config.nodes)
}
