use std::io::{Read, Write};

/// Read a long string from a reader. The format is:
/// - n: u32 = length of the string
/// - n bytes = UTF-8 string
///
/// # Returns
/// A tuple containing the long string and the number of bytes read from the reader.
pub fn read_long_string<R: Read>(reader: &mut R) -> std::io::Result<(String, u32)> {
    let mut buffer = [0u8; 4];
    reader.read_exact(&mut buffer)?;
    let length = u32::from_be_bytes(buffer);

    let mut string_bytes = vec![0u8; length as usize];
    reader.read_exact(&mut string_bytes)?;

    Ok((
        String::from_utf8(string_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
        4 + length as u32,
    ))
}

/// Write a long string to a writer. The format in which is written is:
/// - n: u32 = length of the string *(4 bytes)*
/// - *n bytes* = UTF-8 string
///
///  # Returns
/// The number of bytes written to the writer.
pub fn write_long_string<W: Write>(writer: &mut W, string: &str) -> std::io::Result<u32> {
    writer.write_all(&(string.len() as u32).to_be_bytes())?;
    writer.write_all(string.as_bytes())?;
    Ok(4 + string.len() as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_long_string() {
        let mut input = std::io::Cursor::new(&[0x00, 0x00, 0x00, 0x03, b'f', b'o', b'o']);
        let (string, read) = read_long_string(&mut input).unwrap();
        assert_eq!(string, "foo");
        assert_eq!(read, 7);
    }

    #[test]
    fn test_write_long_string() {
        let string = "foo";
        let mut output = Vec::new();
        let written = write_long_string(&mut output, string).unwrap();
        assert_eq!(output, vec![0x00, 0x00, 0x00, 0x03, b'f', b'o', b'o']);
        assert_eq!(written, 7);
    }

    #[test]
    fn test_read_long_string_invalid_utf8() {
        let mut input = std::io::Cursor::new(&[0x00, 0x00, 0x00, 0x02, 0xFF, 0xFF]);
        let result = read_long_string(&mut input);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_long_string_empty() {
        let mut input = std::io::Cursor::new(&[0x00, 0x00, 0x00, 0x00]);
        let (string, read) = read_long_string(&mut input).unwrap();
        assert_eq!(string, "");
        assert_eq!(read, 4);
    }

    #[test]
    fn test_write_long_string_empty() {
        let string = "";
        let mut output = Vec::new();
        assert_eq!(write_long_string(&mut output, string).unwrap(), 4);
        assert_eq!(output, vec![0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_write_long_string_large() {
        let string = "a".repeat(0xFFFF);
        let mut output = Vec::new();
        let written = write_long_string(&mut output, &string).unwrap();
        assert_eq!(written, 0xFFFF + 4);
        assert_eq!(output.len(), 0xFFFF + 4);
    }

    #[test]
    fn test_read_and_write_long_string() {
        let string = "a".repeat(0xFFFF);
        let mut output = Vec::new();
        let written = write_long_string(&mut output, &string).unwrap();

        let mut input = std::io::Cursor::new(&output);
        let (read_string, read) = read_long_string(&mut input).unwrap();
        assert_eq!(read_string, string);
        assert_eq!(read, written);
    }
}
