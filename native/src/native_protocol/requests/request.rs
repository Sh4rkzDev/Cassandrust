use std::{
    collections::HashMap,
    io::{Read, Write},
};

use db::Context;
use shared::{get_keyspace, io_error};

use crate::native_protocol::{
    header::Opcode, models::query::QueryMsg, responses::{response::Response, result_op::{ResultOP, Rows}},
};

use super::{
    query::{read_query, write_query},
    startup::{read_startup, write_startup},
};

pub enum Request {
    Query(QueryMsg),
    Startup(HashMap<String, String>),
}

impl Request {
    pub fn read<R: Read>(reader: &mut R, opcode: &Opcode, length: u32) -> std::io::Result<Self> {
        match opcode {
            Opcode::Query => {
                let query = read_query(reader, length)?;
                Ok(Request::Query(query))
            }
            Opcode::Startup => {
                let startup = read_startup(reader, length)?;
                Ok(Request::Startup(startup))
            }
            _ => Err(io_error!(format!("Invalid opcode: {opcode}"))),
        }
    }

    pub fn process(&self, ctx: &mut Context) -> std::io::Result<Response> {
        match self {
            Request::Startup(_) => Ok(Response::Ready),
            Request::Query(query) => {
                let table = get_keyspace().join(&query.table);
                let cols = query.query.process(&table, ctx)?;
                if cols.is_some() {
                    Rows::new(metadata, rows_count, rows_content)
                    return Ok(Response::ResultOp(ResultOP::Rows(cols.unwrap())));
                }
                Ok(Response::ResultOp(ResultOP::))
            }
        }
    }

    pub fn is_query(&self) -> bool {
        matches!(self, Request::Query(_))
    }

    pub fn get_keys(&self) -> Option<Vec<String>> {
        match self {
            Request::Query(query) => Some(query.query.get_keys()),
            _ => None,
        }
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<u32> {
        match self {
            Request::Query(query) => write_query(
                writer,
                query.query_str.to_owned(),
                query.consistency.clone(),
                query.flags,
            ),
            Request::Startup(startup) => write_startup(writer, startup),
        }
    }
}
