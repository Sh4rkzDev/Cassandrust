use std::{
    collections::HashMap,
    io::{Read, Write},
};

use db::{Context, SchemaType};
use query::Query;
use shared::{get_keyspace, get_keyspace_name, io_error};

use crate::native_protocol::{
    header::Opcode,
    models::query::QueryMsg,
    responses::{
        response::Response,
        result_op::{ColumnSpec, DataTypeFlags, ResultOP, RowMetadata, Rows},
    },
};

use super::{
    query::{read_query, write_query},
    startup::{read_startup, write_startup},
};

#[derive(Debug)]
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

    pub fn process(&mut self, ctx: &mut Context) -> std::io::Result<Response> {
        match self {
            Request::Startup(map) => {
                let version = map.get("CQL_VERSION");
                if version.is_none() {
                    return Err(io_error!("CQL_VERSION key is mandatory"));
                }
                if version.unwrap() != "3.0.0" {
                    return Err(io_error!("Only CQL_VERSION 3.0.0 is supported"));
                }
                Ok(Response::Ready)
            }
            Request::Query(query) => {
                let table = get_keyspace().join(&query.table);
                let rows = query.query.process(&table, ctx)?;
                if rows.is_some() {
                    let rows = rows.unwrap();
                    let cols = query.query.get_cols();
                    let mut column_specs = Vec::new();
                    for col in cols {
                        match ctx
                            .get_table_schema(&get_keyspace_name().unwrap(), &query.table)?
                            .get_schema_type(&col)
                            .unwrap()
                        {
                            SchemaType::Int => {
                                column_specs.push(ColumnSpec::new(col, DataTypeFlags::Int));
                            }
                            SchemaType::Text => {
                                column_specs.push(ColumnSpec::new(col, DataTypeFlags::Varchar));
                            }
                            SchemaType::Float => {
                                column_specs.push(ColumnSpec::new(col, DataTypeFlags::Float));
                            }
                            SchemaType::Boolean => {
                                column_specs.push(ColumnSpec::new(col, DataTypeFlags::Boolean));
                            }
                            SchemaType::Timestamp => {
                                column_specs.push(ColumnSpec::new(col, DataTypeFlags::Timestamp));
                            }
                        }
                    }
                    let metadata = RowMetadata::new(
                        0x0001,
                        rows[0].len() as i32,
                        Some((get_keyspace_name()?, query.table.clone())),
                        Some(column_specs),
                    )?;
                    let rows = Rows::new(metadata, rows.len() as i32, rows);
                    return Ok(Response::ResultOp(ResultOP::Rows(rows)));
                }
                Ok(Response::ResultOp(ResultOP::Void))
            }
        }
    }

    pub fn get_query(&self) -> Option<(Query, String)> {
        match self {
            Request::Query(query) => Some((query.query.clone(), query.table.clone())),
            _ => None,
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
