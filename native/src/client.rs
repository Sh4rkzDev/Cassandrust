use std::{collections::HashMap, io::Read};

use crate::native_protocol::{
    header::{Header, Opcode},
    models::query::QueryMsg,
    native::{Body, Frame},
    requests::request::Request,
};

use shared::io_error;

pub use crate::native_protocol::models::consistency::ConsistencyLevel;

pub const STARTUP: Opcode = Opcode::Startup;
pub const QUERY: Opcode = Opcode::Query;
pub const READY: Opcode = Opcode::Ready;
pub const RESULT: Opcode = Opcode::ResultOP;

pub fn create_request(
    opcode: Opcode,
    stream: u16,
    query: Option<&str>,
    consistency_level: Option<ConsistencyLevel>,
) -> std::io::Result<Frame> {
    let header = Header::new(0x04, 0x00, stream, opcode.clone())?;
    let req = match opcode {
        QUERY => {
            let query = QueryMsg::new(query.unwrap().to_owned(), consistency_level.unwrap(), 0x00)?;
            Request::Query(query)
        }
        STARTUP => Request::Startup(HashMap::from([(
            "CQL_VERSION".to_string(),
            "3.0.0".to_string(),
        )])),
        _ => return Err(io_error!(format!("Invalid opcode: {opcode}"))),
    };
    let body = Body::Request(req);
    Ok(Frame::new(header, body))
}

pub fn read_response<R: Read>(reader: &mut R) -> std::io::Result<Frame> {
    Frame::read(reader)
}
