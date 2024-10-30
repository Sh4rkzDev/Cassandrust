use std::{
    collections::HashMap,
    io::{Read, Write},
};

use super::string::{read_string, write_string};

/// Read a map of strings from a reader. The format is:
/// - n: u16 = number of key-value pairs
/// - For each key-value pair:
///     - key: string
///     - value: string
///
/// # Returns
/// A tuple containing the map and the number of bytes read from the reader.
pub fn read_string_map<R: Read>(reader: &mut R) -> std::io::Result<(HashMap<String, String>, u32)> {
    let mut buffer = [0u8; 2];
    reader.read_exact(&mut buffer)?;
    let length = u16::from_be_bytes(buffer);
    let mut bytes_read = 2u32;
    let mut map = HashMap::new();
    for _ in 0..length {
        let (key, read) = read_string(reader)?;
        bytes_read += read;
        let (value, read) = read_string(reader)?;
        bytes_read += read;
        map.insert(key, value);
    }
    Ok((map, bytes_read))
}

/// Write a map of strings to a writer. The format in which is written is:
/// - n: u16 = number of key-value pairs *(2 bytes)*
/// - For each key-value pair:
///     - key: string *(see `write_string`)*
///     - value: string *(see `write_string`)*
///
/// # Returns
/// The number of bytes written to the writer.
pub fn write_string_map<W: Write>(
    writer: &mut W,
    map: &HashMap<String, String>,
) -> std::io::Result<u32> {
    writer.write_all(&(map.len() as u16).to_be_bytes())?;
    let mut bytes_written = 2u32;
    for (key, value) in map {
        bytes_written += write_string(writer, key)?;
        bytes_written += write_string(writer, value)?;
    }
    Ok(bytes_written)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_string_map() {
        let mut input = std::io::Cursor::new(&[
            0x00, 0x02, // n = 2
            0x00, 0x03, b'f', b'o', b'o', // key = "foo"
            0x00, 0x03, b'b', b'a', b'r', // value = "bar"
            0x00, 0x03, b'b', b'a', b'z', // key = "baz"
            0x00, 0x03, b'q', b'u', b'x', // value = "qux"
        ]);
        let (map, read) = read_string_map(&mut input).unwrap();
        assert_eq!(read, 22);
        let mut expected = HashMap::new();
        expected.insert("foo".to_string(), "bar".to_string());
        expected.insert("baz".to_string(), "qux".to_string());
        assert_eq!(map, expected);
    }

    #[test]
    fn test_write_string_map() {
        let mut map = HashMap::new();
        map.insert("foo".to_string(), "bar".to_string());
        map.insert("baz".to_string(), "qux".to_string());
        let mut output = Vec::new();
        let written = write_string_map(&mut output, &map).unwrap();
        assert_eq!(written, 22);
        // The order of the key-value pairs is not guaranteed
        assert!(
            output
                == vec![
                    0x00, 0x02, // n = 2
                    0x00, 0x03, b'f', b'o', b'o', // key = "foo"
                    0x00, 0x03, b'b', b'a', b'r', // value = "bar"
                    0x00, 0x03, b'b', b'a', b'z', // key = "baz"
                    0x00, 0x03, b'q', b'u', b'x', // value = "qux"
                ]
                || output
                    == vec![
                        0x00, 0x02, // n = 2
                        0x00, 0x03, b'b', b'a', b'z', // key = "baz"
                        0x00, 0x03, b'q', b'u', b'x', // value = "qux"
                        0x00, 0x03, b'f', b'o', b'o', // key = "foo"
                        0x00, 0x03, b'b', b'a', b'r', // value = "bar"
                    ]
        );
    }

    #[test]
    fn test_read_string_map_empty() {
        let mut input = std::io::Cursor::new(&[0x00, 0x00]);
        let (map, read) = read_string_map(&mut input).unwrap();
        assert_eq!(map, HashMap::new());
        assert_eq!(read, 2);
    }

    #[test]
    fn test_write_string_map_empty() {
        let map = HashMap::new();
        let mut output = Vec::new();
        let written = write_string_map(&mut output, &map).unwrap();
        assert_eq!(written, 2);
        assert_eq!(output, vec![0x00, 0x00]);
    }

    #[test]
    fn test_read_string_map_invalid_utf8() {
        let mut input = std::io::Cursor::new(&[0x00, 0x02, 0x00, 0x02, 0xFF, 0xFF]);
        let err = read_string_map(&mut input).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_write_string_map_invalid_utf8() {
        let mut map = HashMap::new();
        map.insert("foo".to_string(), "bar".to_string());
        map.insert("baz".to_string(), "qux".to_string());
        let mut output = Vec::new();
        write_string_map(&mut output, &map).unwrap();
        output[5] = 0xFF;
        let mut input = std::io::Cursor::new(&output);
        let err = read_string_map(&mut input).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_read_string_map_max_length_key() {
        let mut input_data = vec![0x00, 0x01];
        input_data.extend(vec![0xFF, 0xFF]);
        input_data.extend(vec![b'k'; 0xFFFF]);
        input_data.extend(vec![0x00, 0x03, b'v', b'a', b'l']);

        let mut input = std::io::Cursor::new(input_data);
        let map = read_string_map(&mut input).unwrap().0;

        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_write_string_map_max_length_key() {
        let mut map = HashMap::new();
        map.insert("k".repeat(0xFFFF), "val".to_string());

        let mut output = Vec::new();
        write_string_map(&mut output, &map).unwrap();
        assert_eq!(
            output.len(),
            2 /* map size indicator */ + 2 /* key size indicator */ + 0xFFFF /* key size */ + 2 /* value size indicator */ + 3 /* value size */
        );
    }

    #[test]
    fn test_read_string_map_max_length_value() {
        let mut input_data = vec![0x00, 0x01];
        input_data.extend(vec![0x00, 0x03, b'k', b'e', b'y']);
        input_data.extend(vec![0xFF, 0xFF]);
        input_data.extend(vec![b'v'; 0xFFFF]);

        let mut input = std::io::Cursor::new(input_data);
        let map = read_string_map(&mut input).unwrap().0;

        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_write_string_map_max_length_value() {
        let mut map = HashMap::new();
        map.insert("key".to_string(), "v".repeat(0xFFFF));

        let mut output = Vec::new();
        write_string_map(&mut output, &map).unwrap();
        assert_eq!(
            output.len(),
            2 /* map size indicator */ + 2 /* key size indicator */ + 3 /* key size */ + 2 /* value size indicator */ + 0xFFFF /* value size */
        );
    }

    #[test]
    fn test_read_and_write_string_map() {
        let mut map = HashMap::new();
        map.insert("foo".to_string(), "bar".to_string());
        map.insert("baz".to_string(), "qux".to_string());

        let mut output = Vec::new();
        let written = write_string_map(&mut output, &map).unwrap();

        let mut input = std::io::Cursor::new(&output);
        let (map2, read) = read_string_map(&mut input).unwrap();

        assert_eq!(written, read);
        assert_eq!(map, map2);
    }
}
