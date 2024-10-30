use std::{
    collections::HashMap,
    io::{Cursor, Error, ErrorKind, Read, Write},
};

use shared::io_error;

use crate::native_protocol::parsers::string_map::{read_string_map, write_string_map};

/// Reads a startup body from the provided reader.
///
/// The format of the body is:
/// - body: [string_map] (see `parsers/string_map.rs`)
///
/// All possible keys are:
/// - CQL_VERSION (**mandatory**): the version of the CQL protocol. The only valid value is "3.0.0"
/// - COMPRESSION: the compression algorithm to use. The only valid value is "lz4" or "snappy"
/// - NO_COMPACT: whether or not connection has to be established in compatibility mode
/// - THROW_ON_OVERLOAD: whether or not the server should throw an error when overloaded
pub fn read_startup<R: Read>(
    reader: &mut R,
    length: u32,
) -> std::io::Result<HashMap<String, String>> {
    let mut buffer = vec![0; length as usize];
    reader.read_exact(&mut buffer)?;
    let mut cursor = Cursor::new(buffer);
    let (map, read) = read_string_map(&mut cursor)?;
    if read > length {
        return Err(io_error!("Body length is greater than the frame length"));
    };
    if let Some(version) = map.get("CQL_VERSION") {
        if version != "3.0.0" {
            return Err(io_error!("Only CQL_VERSION 3.0.0 is supported"));
        }
    } else {
        return Err(io_error!("CQL_VERSION key is mandatory"));
    }
    Ok(map)
}

/// Writes a startup body to the provided writer.
///
/// The format of the body is:
/// - length: u32 = length of the frame body
/// - body: [string_map] (see `parsers/string_map.rs`)
///
/// All possible keys are:
/// - CQL_VERSION (**mandatory**): the version of the CQL protocol. The only valid value is "3.0.0"
/// - COMPRESSION: the compression algorithm to use. The only valid value is "lz4" or "snappy"
/// - NO_COMPACT: whether or not connection has to be established in compatibility mode
/// - THROW_ON_OVERLOAD: whether or not the server should throw an error when overloaded
///
/// All other keys are ignored.
///
/// # Returns
/// The number of bytes written to the writer.
pub fn write_startup<W: Write>(
    writer: &mut W,
    map: &HashMap<String, String>,
) -> std::io::Result<u32> {
    let mut buffer = Vec::new();
    if let Some(val) = map.get("CQL_VERSION") {
        if val != "3.0.0" {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Only CQL_VERSION 3.0.0 is supported",
            ));
        }
    } else {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "CQL_VERSION key is mandatory",
        ));
    }
    let map = HashMap::from_iter(
        map.iter()
            .filter(|(k, _)| {
                *k == "CQL_VERSION"
                    || *k == "COMPRESSION"
                    || *k == "NO_COMPACT"
                    || *k == "THROW_ON_OVERLOAD"
            })
            .map(|(k, v)| (k.clone(), v.clone())),
    );
    let bytes_written = write_string_map(&mut buffer, &map)?;
    writer.write_all(&(buffer.len() as u32).to_be_bytes())?;
    writer.write_all(&buffer)?;
    Ok(bytes_written + 4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_startup() {
        let mut input = Cursor::new(&[
            0x00, 0x01, // n = 1
            0x00, 0x0B, b'C', b'Q', b'L', b'_', b'V', b'E', b'R', b'S', b'I', b'O',
            b'N', // key = "CQL_VERSION"
            0x00, 0x05, b'3', b'.', b'0', b'.', b'0', // value = "3.0.0"
        ]);
        let result = read_startup(&mut input, 22).unwrap();
        let mut map = HashMap::new();
        map.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
        assert_eq!(result, map);
    }

    #[test]
    fn test_read_startup_invalid_length() {
        let mut input = Cursor::new(&[
            0x00, 0x01, // n = 1
            0x00, 0x0B, b'C', b'Q', b'L', b'_', b'V', b'E', b'R', b'S', b'I', b'O',
            b'N', // key = "CQL_VERSION"
            0x00, 0x05, b'3', b'.', b'0', b'.', b'0', // value = "3.0.0"
        ]);
        // length = 20 < 22
        let result = read_startup(&mut input, 20);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_startup_invalid_body() {
        let mut input = Cursor::new(&[0x00, 0x00, 0x00, 0x01, 0x00]);
        let result = read_startup(&mut input, 5);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_startup_invalid_key() {
        let mut input = Cursor::new(&[
            0x00, 0x01, // n = 1
            0x00, 0x01, b'a', // key = "a"
            0x00, 0x01, b'b', // value = "b"
        ]);
        let result = read_startup(&mut input, 8);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_startup_invalid_version() {
        let mut input = Cursor::new(&[
            0x00, 0x01, // n = 1
            0x00, 0x0B, b'C', b'Q', b'L', b'_', b'V', b'E', b'R', b'S', b'I', b'O',
            b'N', // key = "CQL_VERSION"
            0x00, 0x05, b'3', b'.', b'0', b'.', b'1', // value = "3.0.1"
        ]);
        let result = read_startup(&mut input, 8);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_startup_all_keys() {
        let mut input = Cursor::new(&[
            0x00, 0x04, // n = 4
            0x00, 0x0B, b'C', b'Q', b'L', b'_', b'V', b'E', b'R', b'S', b'I', b'O',
            b'N', // key = "CQL_VERSION"
            0x00, 0x05, b'3', b'.', b'0', b'.', b'0', // value = "3.0.0"
            0x00, 0x0B, b'C', b'O', b'M', b'P', b'R', b'E', b'S', b'S', b'I', b'O',
            b'N', // key = "COMPRESSION"
            0x00, 0x03, b'l', b'z', b'4', // value = "lz4"
            0x00, 0x0A, b'N', b'O', b'_', b'C', b'O', b'M', b'P', b'A', b'C',
            b'T', // key = "NO_COMPACT"
            0x00, 0x04, b't', b'r', b'u', b'e', // value = "true"
            0x00, 0x11, b'T', b'H', b'R', b'O', b'W', b'_', b'O', b'N', b'_', b'O', b'V', b'E',
            b'R', b'L', b'O', b'A', b'D', // key = "THROW_ON_OVERLOAD"
            0x00, 0x05, b'f', b'a', b'l', b's', b'e', // value = "false"
        ]);
        let result = read_startup(&mut input, 84).unwrap();
        let mut map = HashMap::new();
        map.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
        map.insert("COMPRESSION".to_string(), "lz4".to_string());
        map.insert("NO_COMPACT".to_string(), "true".to_string());
        map.insert("THROW_ON_OVERLOAD".to_string(), "false".to_string());
        assert_eq!(result, map);
    }

    #[test]
    fn test_write_startup() {
        let mut map = HashMap::new();
        map.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
        let mut output = Vec::new();
        let written = write_startup(&mut output, &map).unwrap();
        assert_eq!(written, 26);
        assert_eq!(
            output,
            vec![
                0x00, 0x00, 0x00, 0x16, // length = 22
                0x00, 0x01, // n = 1
                0x00, 0x0B, b'C', b'Q', b'L', b'_', b'V', b'E', b'R', b'S', b'I', b'O',
                b'N', // key = "CQL_VERSION"
                0x00, 0x05, b'3', b'.', b'0', b'.', b'0', // value = "3.0.0"
            ]
        );
    }

    #[test]
    fn test_write_startup_invalid_version() {
        let mut map = HashMap::new();
        map.insert("CQL_VERSION".to_string(), "3.0.1".to_string());
        let mut output = Vec::new();
        let result = write_startup(&mut output, &map);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_and_write_startup() {
        let mut map = HashMap::new();
        map.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
        map.insert("COMPRESSION".to_string(), "lz4".to_string());
        map.insert("NO_COMPACT".to_string(), "true".to_string());
        map.insert("THROW_ON_OVERLOAD".to_string(), "false".to_string());
        let mut output = Vec::new();
        let written = write_startup(&mut output, &map).unwrap();
        assert_eq!(written, 88);
        let mut input = Cursor::new(&output[4..]);
        let result = read_startup(&mut input, 84).unwrap();
        assert_eq!(result, map);
    }
}
