use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use shared::map_io_error;

#[derive(Debug, Serialize, Deserialize)]
pub struct Result {
    pub rows: Option<Vec<Vec<String>>>,
}

impl Result {
    pub(crate) fn read<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let result: Result = bincode::deserialize_from(reader)
            .map_err(map_io_error!("Cannot deserialize Result struct"))?;
        Ok(result)
    }

    pub(crate) fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        bincode::serialize_into(writer, self)
            .map_err(map_io_error!("Cannot serialize Result struct"))
    }
}
