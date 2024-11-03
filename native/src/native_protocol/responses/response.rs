use std::io::{Read, Write};

use shared::io_error;

use crate::native_protocol::header::Opcode;

use super::{error::Error, result_op::ResultOP};

pub enum Response {
    Ready,
    Error(Error),
    ResultOp(ResultOP),
}

impl Response {
    pub fn read<R: Read>(reader: &mut R, opcode: &Opcode, length: u32) -> std::io::Result<Self> {
        match opcode {
            Opcode::Error => {
                let (error, read) = Error::read(reader)?;
                if length != read {
                    return Err(io_error!("Invalid error length"));
                }
                Ok(Response::Error(error))
            }
            Opcode::Ready => Ok(Response::Ready),
            Opcode::ResultOP => {
                let result_op = ResultOP::read(reader, length)?;
                Ok(Response::ResultOp(result_op))
            }
            _ => Err(io_error!(format!("Invalid opcode: {opcode}"))),
        }
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<u32> {
        match self {
            Response::Ready => Ok(0),
            Response::Error(error) => error.write(writer),
            Response::ResultOp(result_op) => result_op.write(writer),
        }
    }
}
