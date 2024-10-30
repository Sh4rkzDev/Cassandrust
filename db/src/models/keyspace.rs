use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use shared::{map_io_error, not_found_error};

#[derive(Debug, Serialize, Deserialize)]
pub struct Replication {
    pub class: String,
    pub replication_factor: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Options {
    pub durable_writes: bool,
    pub replication: Replication,
}

impl Options {
    pub fn new(durable_writes: bool, class: String, replication_factor: i32) -> Self {
        Options {
            durable_writes,
            replication: Replication {
                class,
                replication_factor,
            },
        }
    }

    fn write_to_file(&self, keyspace: &Path) -> std::io::Result<()> {
        let file = fs::File::create(keyspace.join("options.json"))?;
        serde_json::to_writer(file, self).map_err(map_io_error!("Failed to write options"))
    }

    pub(crate) fn read_from_file(keyspace: &Path) -> std::io::Result<Self> {
        let file = fs::File::open(keyspace.join("options.json"))?;
        let reader = std::io::BufReader::new(file);
        serde_json::from_reader(reader).map_err(map_io_error!("Failed to read options"))
    }
}

pub(crate) fn create_keyspace(keyspace: &PathBuf, options: Options) -> std::io::Result<()> {
    if keyspace.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("Keyspace already exists"),
        ));
    }
    fs::create_dir_all(&keyspace)?;
    options.write_to_file(keyspace)?;
    Ok(())
}

pub(crate) fn drop_keyspace(keyspace: &PathBuf) -> std::io::Result<()> {
    if !keyspace.exists() {
        return Err(not_found_error!("Keyspace does not exist"));
    }
    fs::remove_dir_all(keyspace)
}

pub fn use_keyspace(keyspace: &PathBuf) -> std::io::Result<&PathBuf> {
    if keyspace.exists() {
        Ok(keyspace)
    } else {
        Err(not_found_error!("Keyspace does not exist"))
    }
}
