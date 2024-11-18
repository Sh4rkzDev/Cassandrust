use std::io::{Read, Write};

use query::Query as Cql_Query;
use serde::{Deserialize, Serialize};
use shared::map_io_error;

#[derive(Debug, Serialize, Deserialize)]
pub struct Query {
    pub query: Cql_Query,
    pub table: String,
}

impl Query {
    pub(crate) fn read<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let query: Query = bincode::deserialize_from(reader)
            .map_err(map_io_error!("Cannot deserialize Query struct"))?;
        Ok(query)
    }

    pub(crate) fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        bincode::serialize_into(writer, self)
            .map_err(map_io_error!("Cannot serialize Query struct"))
    }
}
