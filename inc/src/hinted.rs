use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use shared::map_io_error;

use crate::query::Query;

#[derive(Debug, Serialize, Deserialize)]
pub struct Hinted {
    pub queries: Vec<Query>,
}

impl Hinted {
    pub(crate) fn read<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let hinted: Hinted = bincode::deserialize_from(reader)
            .map_err(map_io_error!("Cannot deserialize Hinted struct"))?;
        Ok(hinted)
    }

    pub(crate) fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        bincode::serialize_into(writer, self)
            .map_err(map_io_error!("Cannot serialize Hinted struct"))
    }
}
