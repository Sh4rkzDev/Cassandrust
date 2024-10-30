/* // ! Code snippet for length
    let mut length_buffer = [0u8; 4];
    reader.read_exact(&mut length_buffer)?;
    let length = u32::from_be_bytes(length_buffer);
    if length > 256 * 1024 * 1024 {
        // length > 256 MB
        return Err(Error::new(ErrorKind::InvalidData, "Frame body too large"));
    }
*/
