pub mod gossip;
pub mod hinted;
pub mod query;
pub mod result;

use std::io::{Read, Write};

use gossip::{ack::Ack, syn::Syn};
use hinted::Hinted;
use query::Query;
use result::Result;
use serde::{Deserialize, Serialize};
use shared::io_error;

#[derive(Debug)]
pub enum FrameType {
    Query = 0x01,
    Result = 0x02,
    Syn = 0x03,
    Ack = 0x04,
    Hinted = 0x05,
}

impl FrameType {
    fn read<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut buffer = [0u8; 1];
        reader.read_exact(&mut buffer)?;
        match buffer[0] {
            0x01 => Ok(FrameType::Query),
            0x02 => Ok(FrameType::Result),
            0x03 => Ok(FrameType::Syn),
            0x04 => Ok(FrameType::Ack),
            0x05 => Ok(FrameType::Hinted),
            _ => Err(io_error!("Invalid frame type")),
        }
    }

    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            FrameType::Query => writer.write_all(&[0x01u8]),
            FrameType::Result => writer.write_all(&[0x02u8]),
            FrameType::Syn => writer.write_all(&[0x03u8]),
            FrameType::Ack => writer.write_all(&[0x04u8]),
            FrameType::Hinted => writer.write_all(&[0x05u8]),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Body {
    Query(Query),
    Result(Result),
    Syn(Syn),
    Ack(Ack),
    Hinted(Hinted),
}

pub fn read_inc_frame<R: Read>(reader: &mut R) -> std::io::Result<(FrameType, Body)> {
    let frame_type = FrameType::read(reader)?;
    match frame_type {
        FrameType::Query => {
            let query = Query::read(reader)?;
            Ok((FrameType::Query, Body::Query(query)))
        }
        FrameType::Result => {
            let result = Result::read(reader)?;
            Ok((FrameType::Result, Body::Result(result)))
        }
        FrameType::Syn => {
            let syn = Syn::read(reader)?;
            Ok((FrameType::Syn, Body::Syn(syn)))
        }
        FrameType::Ack => {
            let ack = Ack::read(reader)?;
            Ok((FrameType::Ack, Body::Ack(ack)))
        }
        FrameType::Hinted => {
            let hinted = Hinted::read(reader)?;
            Ok((FrameType::Hinted, Body::Hinted(hinted)))
        }
    }
}

pub fn write_inc_frame<W: Write>(
    writer: &mut W,
    frame_type: FrameType,
    body: &Body,
) -> std::io::Result<()> {
    frame_type.write(writer)?;
    match (frame_type, body) {
        (FrameType::Query, Body::Query(query)) => {
            query.write(writer)?;
        }
        (FrameType::Result, Body::Result(result)) => {
            result.write(writer)?;
        }
        (FrameType::Syn, Body::Syn(syn)) => {
            syn.write(writer)?;
        }
        (FrameType::Ack, Body::Ack(ack)) => {
            ack.write(writer)?;
        }
        (FrameType::Hinted, Body::Hinted(hinted)) => {
            hinted.write(writer)?;
        }
        _ => return Err(io_error!("Invalid frame type")),
    }
    writer.flush()
}
