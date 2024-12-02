use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use shared::map_io_error;

use super::peer::Peer;

#[derive(Debug, Serialize, Deserialize)]
pub struct Ack {
    pub heartbeat: u64,
    pub update_peers: Vec<Peer>,
}

impl Ack {
    pub(crate) fn read<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let ack: Ack = bincode::deserialize_from(reader)
            .map_err(map_io_error!("Cannot deserialize Ack struct"))?;
        Ok(ack)
    }

    pub(crate) fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        bincode::serialize_into(writer, self).map_err(map_io_error!("Cannot serialize Ack struct"))
    }
}
