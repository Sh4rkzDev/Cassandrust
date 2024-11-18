use std::io::Read;

use crate::native_protocol::{
    header::{Header, Opcode},
    native::{Body, Frame},
    responses::{error::Error, response::Response},
};

pub use crate::native_protocol::responses::error::ErrorCode;
pub use crate::native_protocol::responses::result_op::{
    ColumnSpec, DataTypeFlags, ResultOP, RowMetadata, Rows, RowsMetadaFlagsMask,
};

pub const READY: Opcode = Opcode::Ready;
pub const ERROR: Opcode = Opcode::Error;
pub const RESULT: Opcode = Opcode::ResultOP;

pub fn read_request<R: Read>(stream: &mut R) -> std::io::Result<Frame> {
    Frame::read(stream)
}

pub fn create_error_response(code: ErrorCode, message: &str) -> Response {
    let mut error = Error::new(code, message.to_string());
    error.add_extra("CQL_VERSION".to_string(), "3.0.0".to_string());
    Response::Error(error)
}

pub fn create_result_response(rows: Option<Rows>) -> Response {
    match rows {
        Some(rows) => Response::ResultOp(ResultOP::Rows(rows)),
        None => Response::ResultOp(ResultOP::Void),
    }
}

pub fn create_ready_response() -> Response {
    Response::Ready
}

pub fn create_response_frame(
    opcode: Opcode,
    stream_id: u16,
    response: Response,
) -> std::io::Result<Frame> {
    let header = Header::new(0x84, 0x00, stream_id, opcode)?;
    let body = Body::Response(response);
    Ok(Frame::new(header, body))
}
