use std::io::{Read, Write};

#[derive(Debug, PartialEq, Clone)]
pub struct Bytes {
    pub bytes_data: Vec<u8>,
}

impl Bytes {
    pub fn new(bytes_data: Vec<u8>) -> Self {
        Self { bytes_data }
    }

    pub fn read<R: Read>(reader: &mut R) -> std::io::Result<(Self, u32)> {
        let mut buf_length = [0u8; 4];
        reader.read_exact(&mut buf_length)?;
        let length = i32::from_be_bytes(buf_length);
        let mut bytes_data = vec![0; length as usize];
        reader.read_exact(&mut bytes_data)?;
        Ok((Bytes { bytes_data }, 4 + length as u32))
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<u32> {
        writer.write_all(&(self.bytes_data.len() as i32).to_be_bytes())?;
        writer.write_all(&self.bytes_data)?;

        Ok(4 + self.bytes_data.len() as u32)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_read_bytes() {
        let mut buffer = Cursor::new(vec![0, 0, 0, 4, 1, 2, 3, 4]);
        let (bytes_type, bytes_read) = Bytes::read(&mut buffer).unwrap();
        assert_eq!(bytes_read, 8);
        assert_eq!(bytes_type.bytes_data.len(), 4);
        assert_eq!(bytes_type.bytes_data, vec![1, 2, 3, 4]);
    }
    #[test]
    fn test_write_value() {
        let mut buffer: Vec<u8> = Vec::new();
        let bytes = Bytes::new(vec![1, 2, 3, 4]);
        assert_eq!(bytes.write(&mut buffer).unwrap(), 8);
        assert_eq!(buffer, vec![0, 0, 0, 4, 1, 2, 3, 4]);
    }
}
