use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use shared::map_io_error;

use super::peer::Peer;

#[derive(Debug, Serialize, Deserialize)]
pub struct Syn {
    pub sender: String,
    pub ip: String,
    pub port: u16,
    pub heartbeat: u64,
    pub known_peers: Vec<Peer>,
}

impl Syn {
    pub(crate) fn read<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let syn: Syn = bincode::deserialize_from(reader)
            .map_err(map_io_error!("Cannot deserialize Syn struct"))?;
        Ok(syn)
    }

    pub(crate) fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        bincode::serialize_into(writer, self).map_err(map_io_error!("Cannot serialize Syn struct"))
    }
}
