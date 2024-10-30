use std::io::{Read, Write};

#[derive(Debug)]
pub struct BytesType{
    pub length: i32,
    pub bytes_data: Vec<u8>
}

pub fn read_bytes<R: Read>(reader: &mut R) -> std::io::Result<(BytesType, u32)> {
    let mut buf_length = [0u8; 4];
    reader.read_exact(&mut buf_length)?;
    let length = i32::from_be_bytes(buf_length);
    let mut bytes_data = vec![0; length as usize];
    reader.read_exact(&mut bytes_data)?;
    Ok((BytesType{length,bytes_data}, 4 + length as u32))
    
}


pub fn write_bytes<W: Write>(writer: &mut W,length:i32, bytes_data: Vec<u8>) -> std::io::Result<u32> {
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(&bytes_data)?;

    Ok(4  + length as u32)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_read_bytes() {
        let mut buffer = Cursor::new(vec![0, 0, 0, 4, 1, 2, 3, 4]);
        let ( bytes_type,  bytes_read) = read_bytes(&mut buffer).unwrap();
        assert_eq!(bytes_read, 8);
        assert_eq!(bytes_type.length, 4);
        assert_eq!(bytes_type.bytes_data,vec![1,2,3,4]);

    }
    #[test]
    fn test_write_value() {
    
        let mut buffer:Vec<u8> = Vec::new();
        assert_eq!(write_bytes(&mut buffer,4 as i32, vec![1, 2, 3, 4]).unwrap(), 8);
        assert_eq!(buffer, vec![0, 0, 0, 4, 1, 2, 3, 4]);

    
    }
}