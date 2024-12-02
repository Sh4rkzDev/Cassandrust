use std::{
    collections::HashMap,
    io::{Read, Write},
};

use serde::{Deserialize, Serialize};
use shared::map_io_error;

#[derive(Debug, Serialize, Deserialize)]
pub struct RowRepair {
    pub primary_key: Vec<String>,
    pub values: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadRepair {
    pub rows: Vec<RowRepair>,
}

impl ReadRepair {
    pub(crate) fn read<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let rr: ReadRepair = bincode::deserialize_from(reader)
            .map_err(map_io_error!("Cannot deserialize Result struct"))?;
        Ok(rr)
    }

    pub(crate) fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        bincode::serialize_into(writer, self)
            .map_err(map_io_error!("Cannot serialize Result struct"))
    }
}
