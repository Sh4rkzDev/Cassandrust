use std::io::{Read, Write};

pub fn read_option<R: Read>(reader: &mut R) -> std::io::Result<(u16, u32)> {
    let mut id_buffer = [0u8; 2];
    reader.read_exact(&mut id_buffer)?;
    let id = u16::from_be_bytes(id_buffer);
    Ok((id, 2))
}

pub fn write_option<W: Write>(writer: &mut W, id: u16) -> std::io::Result<u32> {
    writer.write_all(&(id.to_be_bytes()))?;

    Ok(2)
}

mod tests {
    use crate::native_protocol::parsers::option_type::read_option;
    use crate::native_protocol::parsers::option_type::write_option;

    #[test]
    fn test_read_option_only_id() {
        let mut input = std::io::Cursor::new(&[0x00, 0x09]);
        let (id, read) = read_option(&mut input).unwrap();
        assert_eq!(id, 0x09);
        assert_eq!(read, 2);
    }

    #[test]
    fn test_write_option_only_id() {
        let id: u16 = 0x000D;
        let mut output = Vec::new();
        let written = write_option(&mut output, id).unwrap();
        assert_eq!(output, vec![0x00, 0x0D]);
        assert_eq!(written, 2);
    }
}
