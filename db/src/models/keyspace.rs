use std::{
    fs::{create_dir_all, remove_dir_all, File},
    io::{Read, Write},
    path::Path,
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

    pub(crate) fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        serde_json::to_writer(writer, self).map_err(map_io_error!("Failed to write options"))
    }

    pub(crate) fn read<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        serde_json::from_reader(reader).map_err(map_io_error!("Failed to read options"))
    }
}

pub(crate) fn create_keyspace(keyspace: &Path, options: &Options) -> std::io::Result<()> {
    if keyspace.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "Keyspace already exists",
        ));
    }
    create_dir_all(keyspace)?;
    let mut opt_file = File::create(keyspace.join("options.json"))?;
    options.write(&mut opt_file)
}

pub(crate) fn drop_keyspace(keyspace: &Path) -> std::io::Result<()> {
    if !keyspace.exists() {
        return Err(not_found_error!("Keyspace does not exist"));
    }
    remove_dir_all(keyspace)
}

pub fn use_keyspace(keyspace: &Path) -> std::io::Result<&Path> {
    if keyspace.exists() {
        Ok(keyspace)
    } else {
        Err(not_found_error!("Keyspace does not exist"))
    }
}

pub(crate) fn get_keyspace_options(keyspace: &Path) -> std::io::Result<Options> {
    let mut opt_file = File::open(keyspace.join("options.json"))?;
    Options::read(&mut opt_file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Cursor, path::PathBuf};

    #[test]
    fn test_options_serde() {
        let options = Options::new(true, "SimpleStrategy".to_string(), 1);
        let mut buffer = Cursor::new(Vec::new());
        options.write(&mut buffer).unwrap();
        buffer.set_position(0);
        let read_options = Options::read(&mut buffer).unwrap();
        assert_eq!(options.durable_writes, read_options.durable_writes);
        assert_eq!(options.replication.class, read_options.replication.class);
        assert_eq!(
            options.replication.replication_factor,
            read_options.replication.replication_factor
        );
    }

    #[test]
    fn test_create_keyspace() {
        let keyspace = PathBuf::from("test_keyspace");
        let options = Options::new(true, "SimpleStrategy".to_string(), 1);
        create_keyspace(&keyspace, &options).unwrap();
        assert!(keyspace.exists());
        let read_options = get_keyspace_options(&keyspace).unwrap();
        drop_keyspace(&keyspace).unwrap();
        assert_eq!(options.durable_writes, read_options.durable_writes);
        assert_eq!(options.replication.class, read_options.replication.class);
        assert_eq!(
            options.replication.replication_factor,
            read_options.replication.replication_factor
        );
        assert!(!keyspace.exists());
    }
}
