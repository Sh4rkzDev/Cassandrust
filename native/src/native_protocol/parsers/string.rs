use std::io::{Read, Write};

/// Read a string from a reader. The format is:
/// - n: u16 = length of the string
/// - n bytes = UTF-8 string
///
/// # Returns
/// A tuple containing the string and the number of bytes read from the reader.
pub fn read_string<R: Read>(reader: &mut R) -> std::io::Result<(String, u32)> {
    let mut buffer = [0u8; 2];
    reader.read_exact(&mut buffer)?;
    let length = u16::from_be_bytes(buffer);

    let mut string_bytes = vec![0u8; length as usize];
    reader.read_exact(&mut string_bytes)?;

    Ok((
        String::from_utf8(string_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
        2 + length as u32,
    ))
}

/// Write a string to a writer. The format in which is written is:
/// - n: u16 = length of the string *(2 bytes)*
/// - *n bytes* = UTF-8 string
///
///  # Returns
/// The number of bytes written to the writer.
pub fn write_string<W: Write>(writer: &mut W, string: &str) -> std::io::Result<u32> {
    writer.write_all(&(string.len() as u16).to_be_bytes())?;
    writer.write_all(string.as_bytes())?;
    Ok(2 + string.len() as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_string() {
        let mut input = std::io::Cursor::new(&[0x00, 0x03, b'f', b'o', b'o']);
        let (string, read) = read_string(&mut input).unwrap();
        assert_eq!(string, "foo");
        assert_eq!(read, 5);
    }

    #[test]
    fn test_write_string() {
        let string = "foo";
        let mut output = Vec::new();
        let written = write_string(&mut output, string).unwrap();
        assert_eq!(output, vec![0x00, 0x03, b'f', b'o', b'o']);
        assert_eq!(written, 5);
    }

    #[test]
    fn test_read_string_invalid_utf8() {
        let mut input = std::io::Cursor::new(&[0x00, 0x02, 0xFF, 0xFF]);
        let result = read_string(&mut input);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_string_empty() {
        let mut input = std::io::Cursor::new(&[0x00, 0x00]);
        let (string, read) = read_string(&mut input).unwrap();
        assert_eq!(string, "");
        assert_eq!(read, 2);
    }

    #[test]
    fn test_write_string_empty() {
        let string = "";
        let mut output = Vec::new();
        assert_eq!(write_string(&mut output, string).unwrap(), 2);
        assert_eq!(output, vec![0x00, 0x00]);
    }

    #[test]
    fn test_read_string_max_length() {
        let mut input_data = vec![0xFF, 0xFF];
        input_data.extend(vec![b'a'; 0xFFFF]);

        let mut input = std::io::Cursor::new(input_data);
        let (string, read) = read_string(&mut input).unwrap();
        assert_eq!(read, 0xFFFF as u32 + 2);
        assert_eq!(string.len(), 0xFFFF);
    }

    #[test]
    fn test_write_string_max_length() {
        let string = "a".repeat(0xFFFF);
        let mut output = Vec::new();
        let written = write_string(&mut output, &string).unwrap();
        assert_eq!(output.len(), 0xFFFF + 2);
        assert_eq!(written, 0xFFFF as u32 + 2);
    }
}
