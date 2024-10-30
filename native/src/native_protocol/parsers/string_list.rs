use std::io::{self, Read, Write};

use super::string::{read_string, write_string};

/// Read a list of strings from a reader. The format is:
/// - n: u16 = number of strings
/// - For each string:
///     - m: u16 = length of the string
///     - m bytes = UTF-8 string
///
/// # Returns
/// A tuple containing the list of strings and the number of bytes read from the reader.
pub fn read_string_list<R: Read>(reader: &mut R) -> io::Result<(Vec<String>, u32)> {
    let mut buffer = [0u8; 2];
    reader.read_exact(&mut buffer)?;
    let n = u16::from_be_bytes(buffer);

    let mut bytes_read = 2u32;
    let mut strings = Vec::with_capacity(n as usize);

    for _ in 0..n {
        let (string, read) = read_string(reader)?;
        strings.push(string);
        bytes_read += read;
    }

    Ok((strings, bytes_read))
}

/// Write a list of strings to a writer. The format in which is written is:
/// - n: u16 = number of strings *(2 bytes)*
/// - For each string:
///     - m: u16 = length of the string *(2 bytes)*
///     - *m bytes* = UTF-8 string
///
/// # Returns
/// The number of bytes written to the writer.
pub fn write_string_list<W: Write>(writer: &mut W, strings: &[String]) -> io::Result<u32> {
    writer.write_all(&(strings.len() as u16).to_be_bytes())?;
    let mut bytes_written = 2u32;
    for s in strings {
        bytes_written += write_string(writer, &s)?;
    }
    Ok(bytes_written)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_string_list() {
        let mut input = io::Cursor::new(&[
            0x00, 0x02, // n = 2
            0x00, 0x03, b'f', b'o', b'o', // m = 3, "foo"
            0x00, 0x03, b'b', b'a', b'r', // m = 3, "bar"
        ]);
        let (strings, read) = read_string_list(&mut input).unwrap();
        assert_eq!(read, 12);
        assert_eq!(strings, vec!["foo".to_string(), "bar".to_string()]);
    }

    #[test]
    fn test_write_string_list() {
        let strings = vec!["foo".to_string(), "bar".to_string()];
        let mut output = Vec::new();
        assert_eq!(write_string_list(&mut output, &strings).unwrap(), 12);
        assert_eq!(
            output,
            vec![
                0x00, 0x02, // n = 2
                0x00, 0x03, b'f', b'o', b'o', // m = 3, "foo"
                0x00, 0x03, b'b', b'a', b'r', // m = 3, "bar"
            ]
        );
    }

    #[test]
    fn test_read_string_list_empty() {
        let mut input = io::Cursor::new(&[0x00, 0x00]);
        let (strings, read) = read_string_list(&mut input).unwrap();
        assert_eq!(strings, Vec::<String>::new());
        assert_eq!(read, 2);
    }

    #[test]
    fn test_write_string_list_empty() {
        let strings = Vec::<String>::new();
        let mut output = Vec::new();
        assert_eq!(write_string_list(&mut output, &strings).unwrap(), 2);
        assert_eq!(output, vec![0x00, 0x00]);
    }

    #[test]
    fn test_read_string_list_invalid_utf8() {
        let mut input = io::Cursor::new(&[0x00, 0x02, 0x00, 0x02, 0xFF, 0xFF]);
        let err = read_string_list(&mut input).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_write_string_list_invalid_utf8() {
        let strings = vec!["foo".to_string(), "bar".to_string()];
        let mut output = Vec::new();
        write_string_list(&mut output, &strings).unwrap();
        output[5] = 0xFF;
        let mut input = io::Cursor::new(&output);
        let err = read_string_list(&mut input).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_read_string_list_incomplete() {
        let mut input = io::Cursor::new(&[0x00, 0x02, 0x00, 0x02, b'f']);
        let err = read_string_list(&mut input).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_write_string_list_incomplete() {
        let strings = vec!["foo".to_string(), "bar".to_string()];
        let mut output = Vec::new();
        write_string_list(&mut output, &strings).unwrap();
        output.pop();
        let mut input = io::Cursor::new(&output);
        let err = read_string_list(&mut input).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_read_and_write_string_list() {
        let strings = vec!["foo".to_string(), "bar".to_string()];
        let mut output = Vec::new();
        let written = write_string_list(&mut output, &strings).unwrap();
        let mut input = io::Cursor::new(&output);
        let (read_strings, read) = read_string_list(&mut input).unwrap();
        assert_eq!(written, read);
        assert_eq!(strings, read_strings);
    }
}
