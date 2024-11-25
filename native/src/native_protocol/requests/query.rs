use std::io::{Cursor, Read, Write};

use shared::io_error;

use crate::native_protocol::{
    models::{consistency::ConsistencyLevel, query::QueryMsg},
    parsers::long_string::{read_long_string, write_long_string},
};

/// Reads a query body from the provided reader.
///
/// The format of the body is:
/// - query: [long_string] (see `parsers/long_string.rs`)
/// - consistency: u16 = consistency level (see `models/consistency.rs`)
/// - flags: u8 = flags
///
/// Depending on the flags, the body may contain additional fields:
/// - VALUES: <n><value_1>...<value_n>
/// - PAGE_SIZE: <page_size> (4 bytes)
/// - PAGING_STATE: <paging_state> (<n><byte_1>...<byte_n>)
/// - WITH_NAMES_FOR_VALUES: VALUES but with names before each value
pub(crate) fn read_query<R: Read>(reader: &mut R, length: u32) -> std::io::Result<QueryMsg> {
    let mut buffer = vec![0; length as usize];
    reader.read_exact(&mut buffer)?;
    let mut cursor = Cursor::new(buffer);

    let (query_string, read) = read_long_string(&mut cursor)?;
    let mut bytes_read = read;

    let mut consistency_buffer = [0u8; 2];
    cursor.read_exact(&mut consistency_buffer)?;
    let consistency = ConsistencyLevel::from_u16(u16::from_be_bytes(consistency_buffer))?;
    bytes_read += 2;

    let mut flags_buffer = [0u8; 1];
    cursor.read_exact(&mut flags_buffer)?;
    bytes_read += 1;

    if bytes_read > length {
        return Err(io_error!("Body length is greater than the frame length"));
    };

    Ok(QueryMsg::new(query_string, consistency, flags_buffer[0])?)
}

pub(crate) fn write_query<W: Write>(
    writer: &mut W,
    query_str: String,
    consistency_level: ConsistencyLevel,
    flags: u8,
) -> std::io::Result<u32> {
    let mut buffer = Vec::new();
    write_long_string(&mut buffer, &query_str)?;
    buffer.extend((consistency_level as u16).to_be_bytes());
    buffer.push(flags.to_be());

    writer.write_all(&buffer)?;

    Ok(buffer.len() as u32)
}
