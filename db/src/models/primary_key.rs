use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimaryKey {
    partition_key: Vec<String>,
    clustering_key: Vec<String>,
}

impl PrimaryKey {
    pub fn new(partition_key: Vec<String>, clustering_key: Vec<String>) -> Self {
        PrimaryKey {
            partition_key,
            clustering_key,
        }
    }

    pub fn get_partition_key(&self) -> &[String] {
        &self.partition_key
    }

    pub fn get_clustering_key(&self) -> &[String] {
        &self.clustering_key
    }
}
