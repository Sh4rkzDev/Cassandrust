use std::io::{Read, Write};

/// Reads a value from the reader. The format of the value is:
/// - n: i32 = length of the value
/// - Depending on the value of n:
///     - n < -2: invalid value length
///     - n == -2: the value is not set and will not change if there is a previous value
///     - n == -1: the value is 'NULL'
///     - n >= 0: the value is an array of `n` bytes
///
/// # Returns
/// A tuple with the n bytes of the value and the number of bytes read.
pub fn read_value<R: Read>(reader: &mut R) -> std::io::Result<(Option<Vec<u8>>, u32)> {
    let mut length_buffer = [0u8; 4];
    reader.read_exact(&mut length_buffer)?;
    let n = i32::from_be_bytes(length_buffer);
    if n < -2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid value length: the length must be greater or equal than -2",
        ));
    } else if n == -2 {
        return Ok((None, 4));
    } else if n == -1 {
        return Ok((Some(Vec::new()), 4));
    } else {
        let mut buffer = vec![0; n as usize];
        reader.read_exact(&mut buffer)?;
        Ok((Some(buffer), 4 + n as u32))
    }
}

/// Writes a value to the writer. The format in which the value is written is:
/// - n: i32 = length of the value. If the value is not set, n is -2. If the value is "NULL", n is -1.
/// - Depending on the value of n:
///     - n < -2: invalid value length
///     - n == -2: the value is not set and will not change if there is a previous value
///     - n == -1: the value is "NULL"
///     - n >= 0: the value is an array of `n` bytes
///
/// # Returns
/// The number of bytes written.
pub fn write_value<W: Write>(writer: &mut W, value: Option<Vec<u8>>) -> std::io::Result<u32> {
    let bytes_written;
    match value {
        None => {
            writer.write_all(&(-2i32).to_be_bytes())?;
            bytes_written = 4;
        }
        Some(value) => {
            if value.is_empty() {
                writer.write_all(&(-1i32).to_be_bytes())?;
                bytes_written = 4;
            } else {
                writer.write_all(&(value.len() as i32).to_be_bytes())?;
                writer.write_all(&value)?;
                bytes_written = 4 + value.len() as u32;
            }
        }
    }
    Ok(bytes_written)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_read_value() {
        let mut buffer = Cursor::new(vec![0, 0, 0, 4, 1, 2, 3, 4]);
        let (mut value, mut bytes_read) = read_value(&mut buffer).unwrap();
        assert_eq!(bytes_read, 8);
        assert_eq!(value, Some(vec![1, 2, 3, 4]));

        buffer = Cursor::new(vec![0, 0, 0, 4, 255, 255, 255, 255]);
        (value, bytes_read) = read_value(&mut buffer).unwrap();
        assert_eq!(bytes_read, 8);
        assert_eq!(value, Some(vec![255, 255, 255, 255]));

        buffer = Cursor::new(vec![0, 0, 0, 9, 0, 0, 0, 5, 0, 0, 0, 0, 0]);
        (value, bytes_read) = read_value(&mut buffer).unwrap();
        assert_eq!(bytes_read, 13);
        assert_eq!(value, Some(vec![0, 0, 0, 5, 0, 0, 0, 0, 0]));
    }

    #[test]
    fn test_write_value() {
        let mut buffer = Vec::new();
        assert_eq!(write_value(&mut buffer, Some(Vec::new())).unwrap(), 4);
        assert_eq!(buffer, vec![0xFF, 0xFF, 0xFF, 0xFF]);

        buffer = Vec::new();
        assert_eq!(write_value(&mut buffer, Some(vec![1, 2, 3, 4])).unwrap(), 8);
        assert_eq!(buffer, vec![0, 0, 0, 4, 1, 2, 3, 4]);

        buffer = Vec::new();
        assert_eq!(
            write_value(&mut buffer, Some(vec![255, 255, 255, 255])).unwrap(),
            8
        );
        assert_eq!(buffer, vec![0, 0, 0, 4, 255, 255, 255, 255]);

        buffer = Vec::new();
        assert_eq!(
            write_value(&mut buffer, Some(vec![0, 0, 0, 0, 0])).unwrap(),
            9
        );
        assert_eq!(buffer, vec![0, 0, 0, 5, 0, 0, 0, 0, 0]);

        buffer = Vec::new();
        assert_eq!(write_value(&mut buffer, None).unwrap(), 4);
        assert_eq!(buffer, vec![0xFF, 0xFF, 0xFF, 0xFE]);
    }

    #[test]
    fn test_read_value_invalid_length() {
        let mut buffer = Cursor::new(vec![0, 0, 0, 4, 1, 2, 3]);
        assert!(read_value(&mut buffer).is_err());
    }
}
