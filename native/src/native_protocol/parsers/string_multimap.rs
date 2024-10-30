use std::{
    collections::HashMap,
    io::{Read, Write},
};

use super::{
    string::{read_string, write_string},
    string_list::{read_string_list, write_string_list},
};

/// Read a map of strings as keys and lists of strings as values from a reader. The format is:
/// - n: u16 = number of key-value pairs
/// - For each key-value pair:
///     - key: string
///     - value: list of strings
///
/// # Returns
/// The map and the number of bytes read from the reader.
pub fn read_string_multimap<R: Read>(
    reader: &mut R,
) -> std::io::Result<(HashMap<String, Vec<String>>, u32)> {
    let mut buffer = [0u8; 2];
    reader.read_exact(&mut buffer)?;
    let length = u16::from_be_bytes(buffer);
    let mut bytes_read = 2u32;
    let mut map = HashMap::new();
    for _ in 0..length {
        let (key, read) = read_string(reader)?;
        bytes_read += read;
        let (value, read) = read_string_list(reader)?;
        bytes_read += read;
        map.insert(key, value);
    }
    Ok((map, bytes_read))
}

/// Write a map of strings as keys and lists of strings as values to a writer. The format in which is written is:
/// - n: u16 = number of key-value pairs *(2 bytes)*
/// - For each key-value pair:
///     - key: string *(see `write_string`)*
///     - value: list of strings *(see `write_string_list`)*
///
/// # Returns
/// The number of bytes written to the writer.
pub fn write_string_multimap<W: Write>(
    writer: &mut W,
    map: &HashMap<String, Vec<String>>,
) -> std::io::Result<u32> {
    writer.write_all(&(map.len() as u16).to_be_bytes())?;
    let mut bytes_written = 2u32;
    for (key, value) in map {
        bytes_written += write_string(writer, key)?;
        bytes_written += write_string_list(writer, value)?;
    }
    Ok(bytes_written)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_string_multimap() {
        let mut input = std::io::Cursor::new(&[
            0x00, 0x02, // n = 2
            0x00, 0x03, b'f', b'o', b'o', // key = "foo"
            0x00, 0x02, // m = 2
            0x00, 0x03, b'f', b'o', b'o', // "foo"
            0x00, 0x03, b'b', b'a', b'r', // "bar"
            0x00, 0x03, b'b', b'a', b'z', // key = "baz"
            0x00, 0x03, // m = 3
            0x00, 0x03, b'f', b'o', b'o', // "foo"
            0x00, 0x03, b'b', b'a', b'r', // "bar"
            0x00, 0x03, b'q', b'u', b'x', // "qux"
        ]);
        let (map, read) = read_string_multimap(&mut input).unwrap();
        assert_eq!(read, 41);
        let mut expected = HashMap::new();
        expected.insert(
            "foo".to_string(),
            vec!["foo".to_string(), "bar".to_string()],
        );
        expected.insert(
            "baz".to_string(),
            vec!["foo".to_string(), "bar".to_string(), "qux".to_string()],
        );
        assert_eq!(map, expected);
    }

    #[test]
    fn test_write_string_multimap() {
        let mut map = HashMap::new();
        map.insert(
            "foo".to_string(),
            vec!["foo".to_string(), "bar".to_string()],
        );
        map.insert(
            "baz".to_string(),
            vec!["foo".to_string(), "bar".to_string(), "qux".to_string()],
        );
        let mut output = Vec::new();
        assert_eq!(write_string_multimap(&mut output, &map).unwrap(), 41);
        // The order of the key-value pairs is not guaranteed
        assert!(
            output
                == vec![
                    0x00, 0x02, // n = 2
                    0x00, 0x03, b'f', b'o', b'o', // key = "foo"
                    0x00, 0x02, // m = 2
                    0x00, 0x03, b'f', b'o', b'o', // "foo"
                    0x00, 0x03, b'b', b'a', b'r', // "bar"
                    0x00, 0x03, b'b', b'a', b'z', // key = "baz"
                    0x00, 0x03, // m = 3
                    0x00, 0x03, b'f', b'o', b'o', // "foo"
                    0x00, 0x03, b'b', b'a', b'r', // "bar"
                    0x00, 0x03, b'q', b'u', b'x', // "qux"
                ]
                || output
                    == vec![
                        0x00, 0x02, // n = 2
                        0x00, 0x03, b'b', b'a', b'z', // key = "baz"
                        0x00, 0x03, // m = 3
                        0x00, 0x03, b'f', b'o', b'o', // "foo"
                        0x00, 0x03, b'b', b'a', b'r', // "bar"
                        0x00, 0x03, b'q', b'u', b'x', // "qux"
                        0x00, 0x03, b'f', b'o', b'o', // key = "foo"
                        0x00, 0x02, // m = 2
                        0x00, 0x03, b'f', b'o', b'o', // "foo"
                        0x00, 0x03, b'b', b'a', b'r', // "bar"
                    ]
        );
    }

    #[test]
    fn test_read_string_multimap_empty() {
        let mut input = std::io::Cursor::new(&[0x00, 0x00]);
        let (map, read) = read_string_multimap(&mut input).unwrap();
        assert_eq!(map, HashMap::new());
        assert_eq!(read, 2);
    }

    #[test]
    fn test_write_string_multimap_empty() {
        let map = HashMap::new();
        let mut output = Vec::new();
        assert_eq!(write_string_multimap(&mut output, &map).unwrap(), 2);
        assert_eq!(output, vec![0x00, 0x00]);
    }

    #[test]
    fn test_read_string_multimap_invalid_utf8() {
        let mut input = std::io::Cursor::new(&[0x00, 0x02, 0x00, 0x02, 0xFF, 0xFF]);
        let err = read_string_multimap(&mut input).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_write_string_multimap_invalid_utf8() {
        let mut map = HashMap::new();
        map.insert(
            "foo".to_string(),
            vec!["foo".to_string(), "bar".to_string()],
        );
        map.insert(
            "baz".to_string(),
            vec!["foo".to_string(), "bar".to_string(), "qux".to_string()],
        );
        let mut output = Vec::new();
        write_string_multimap(&mut output, &map).unwrap();
        output[5] = 0xFF;
        let mut input = std::io::Cursor::new(&output);
        let err = read_string_multimap(&mut input).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_read_string_multimap_incomplete() {
        let mut input = std::io::Cursor::new(&[0x00, 0x02, 0x00, 0x02, b'f']);
        let err = read_string_multimap(&mut input).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_write_string_multimap_incomplete() {
        let mut map = HashMap::new();
        map.insert(
            "foo".to_string(),
            vec!["foo".to_string(), "bar".to_string()],
        );
        map.insert(
            "baz".to_string(),
            vec!["foo".to_string(), "bar".to_string(), "qux".to_string()],
        );
        let mut output = Vec::new();
        write_string_multimap(&mut output, &map).unwrap();
        output.pop();
        let mut input = std::io::Cursor::new(&output);
        let err = read_string_multimap(&mut input).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_read_and_write_string_multimap() {
        let mut map = HashMap::new();
        map.insert(
            "foo".to_string(),
            vec!["foo".to_string(), "bar".to_string()],
        );
        map.insert(
            "baz".to_string(),
            vec!["foo".to_string(), "bar".to_string(), "qux".to_string()],
        );
        let mut output = Vec::new();
        let written = write_string_multimap(&mut output, &map).unwrap();
        let mut input = std::io::Cursor::new(&output);
        let (read_map, read) = read_string_multimap(&mut input).unwrap();
        assert_eq!(written, read);
        assert_eq!(map, read_map);
    }
}
