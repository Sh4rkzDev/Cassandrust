/* // ! Code snippet for length
    let mut length_buffer = [0u8; 4];
    reader.read_exact(&mut length_buffer)?;
    let length = u32::from_be_bytes(length_buffer);
    if length > 256 * 1024 * 1024 {
        // length > 256 MB
        return Err(Error::new(ErrorKind::InvalidData, "Frame body too large"));
    }
*/

use std::io::Read;

use shared::io_error;

use super::{
    header::{Header, Opcode},
    requests::request::Request,
    responses::response::Response,
};

pub enum Body {
    Request(Request),
    Response(Response),
}

pub struct Frame {
    pub header: Header,
    pub body: Body,
}

pub fn new_frame(header: Header, body: Body) -> Frame {
    Frame { header, body }
}

pub fn read_frame<R: Read>(reader: &mut R) -> std::io::Result<Frame> {
    let header = Header::read_header(reader)?;
    let mut length_buffer = [0u8; 4];
    reader.read_exact(&mut length_buffer)?;
    let length = u32::from_be_bytes(length_buffer);
    if length > 256 * 1024 * 1024 {
        // length > 256 MB
        return Err(io_error!("Frame body too large"));
    }
    let body: Body = match header.opcode {
        Opcode::Startup | Opcode::Query => {
            Body::Request(Request::read(reader, &header.opcode, length)?)
        }
        Opcode::Error | Opcode::Ready | Opcode::ResultOP => {
            Body::Response(Response::read(reader, &header.opcode, length)?)
        }
        _ => return Err(io_error!(format!("Invalid opcode: {}", header.opcode))),
    };
    Ok(Frame { header, body })
}

pub fn write_frame<W: std::io::Write>(writer: &mut W, frame: &Frame) -> std::io::Result<()> {
    frame.header.write_header(writer)?;
    let mut buffer = Vec::new();
    let length = match &frame.body {
        Body::Request(request) => request.write(&mut buffer)?,
        Body::Response(response) => response.write(&mut buffer)?,
    };
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(&buffer)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::native_protocol::models::query::QueryMsg;

    use super::*;
    use std::{collections::HashMap, io::Cursor};

    #[test]
    fn test_read_and_write_frame() {
        let frame = new_frame(
            Header::new(0x04, 0x00, 1234, Opcode::Startup).unwrap(),
            Body::Request(Request::Query(
                QueryMsg::new(
                    "SELECT * FROM table WHERE id = 1".to_string(),
                    crate::native_protocol::models::consistency::ConsistencyLevel::Three,
                    0,
                )
                .unwrap(),
            )),
        );

        let mut buffer = Vec::new();
        write_frame(&mut buffer, &frame).unwrap();

        let mut cursor = Cursor::new(buffer);
        let result = read_frame(&mut cursor).unwrap();
        assert_eq!(result.header.version, 0x04);
        assert_eq!(result.header.flag, 0x00);
        assert_eq!(result.header.stream, 1234);
        assert_eq!(result.header.opcode, Opcode::Startup);
        if let Body::Request(Request::Startup(map)) = result.body {
            assert_eq!(map.get("CQL_VERSION").unwrap(), "3.0.0");
        } else {
            panic!("Invalid body");
        }
    }

    #[test]
    fn test_read_frame_invalid_body() {
        let frame = new_frame(
            Header::new(0x04, 0x00, 1234, Opcode::Query).unwrap(),
            Body::Request(Request::Startup(HashMap::from([(
                "CQL_VERSION".to_string(),
                "3.0.0".to_string(),
            )]))),
        );

        let mut cursor = Cursor::new(buffer);
        let result = read_frame(&mut cursor);
        assert!(result.is_err());
    }
}
