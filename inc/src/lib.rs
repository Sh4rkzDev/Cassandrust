pub mod query;
pub mod result;

use std::io::{Read, Write};

use query::Query;
use result::Result;
use shared::io_error;

#[derive(Debug)]
pub enum FrameType {
    Query = 0x01,
    Result = 0x02,
}

impl FrameType {
    fn read<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut buffer = [0u8; 1];
        reader.read_exact(&mut buffer)?;
        match buffer[0] {
            0x01 => Ok(FrameType::Query),
            0x02 => Ok(FrameType::Result),
            _ => Err(io_error!("Invalid frame type")),
        }
    }

    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            FrameType::Query => writer.write_all(&[0x01u8]),
            FrameType::Result => writer.write_all(&[0x02u8]),
        }
    }
}

#[derive(Debug)]
pub enum Body {
    Query(Query),
    Result(Result),
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
            println!("RESULT: {:?}", result);
            result.write(writer)?;
        }
        _ => return Err(io_error!("Invalid frame type")),
    }
    writer.flush()
}
