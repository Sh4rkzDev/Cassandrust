use std::{
    fmt::Display,
    io::{Read, Write},
};

/*
      0         8        16        24        32         40
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
      |                                       |
      .            ...  body ...              .
      .                                       .
      .                                       .
      +----------------------------------------
*/

/// A single byte that describes the possible Opcodes that distinguishes the actual message
#[derive(Debug, PartialEq)]
pub enum Opcode {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    Options = 0x05,
    Supported = 0x06,
    Query = 0x07,
    ResultOP = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    AuthChallenge = 0x0E,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
}

impl Display for Opcode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let opcode = match self {
            Opcode::Error => "Error",
            Opcode::Startup => "Startup",
            Opcode::Ready => "Ready",
            Opcode::Authenticate => "Authenticate",
            Opcode::Options => "Options",
            Opcode::Supported => "Supported",
            Opcode::Query => "Query",
            Opcode::ResultOP => "Result",
            Opcode::Prepare => "Prepare",
            Opcode::Execute => "Execute",
            Opcode::AuthChallenge => "AuthChallenge",
            Opcode::AuthResponse => "AuthResponse",
            Opcode::AuthSuccess => "AuthSuccess",
        };
        write!(f, "{}", opcode)
    }
}

impl Opcode {
    pub fn new(opcode: u8) -> std::io::Result<Opcode> {
        match opcode {
            0x00 => Ok(Opcode::Error),
            0x01 => Ok(Opcode::Startup),
            0x02 => Ok(Opcode::Ready),
            0x03 => Ok(Opcode::Authenticate),
            0x05 => Ok(Opcode::Options),
            0x06 => Ok(Opcode::Supported),
            0x07 => Ok(Opcode::Query),
            0x08 => Ok(Opcode::ResultOP),
            0x09 => Ok(Opcode::Prepare),
            0x0A => Ok(Opcode::Execute),
            0x0E => Ok(Opcode::AuthChallenge),
            0x0F => Ok(Opcode::AuthResponse),
            0x10 => Ok(Opcode::AuthSuccess),
            _ => std::io::Result::Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid opcode: {opcode}"),
            )),
        }
    }

    fn to_be_bytes(&self) -> [u8; 1] {
        match self {
            Opcode::Error => [0x00],
            Opcode::Startup => [0x01],
            Opcode::Ready => [0x02],
            Opcode::Authenticate => [0x03],
            Opcode::Options => [0x05],
            Opcode::Supported => [0x06],
            Opcode::Query => [0x07],
            Opcode::ResultOP => [0x08],
            Opcode::Prepare => [0x09],
            Opcode::Execute => [0x0A],
            Opcode::AuthChallenge => [0x0E],
            Opcode::AuthResponse => [0x0F],
            Opcode::AuthSuccess => [0x10],
        }
    }
}

#[derive(Debug)]
pub struct Header {
    pub version: u8,
    pub flag: u8,
    pub stream: u16,
    pub opcode: Opcode,
}

impl Header {
    pub fn new(version: u8, flag: u8, stream: u16, opcode: Opcode) -> Result<Self, String> {
        if version != 0x04 && version != 0x84 {
            return Err(format!(
                "Invalid version: expected 0x04 or 0x84, got {version}"
            ));
        }
        let reqs = vec![
            Opcode::Startup,
            Opcode::AuthResponse,
            Opcode::Options,
            Opcode::Query,
            Opcode::Prepare,
            Opcode::Execute,
        ];
        let resp = vec![
            Opcode::Error,
            Opcode::Ready,
            Opcode::Authenticate,
            Opcode::Supported,
            Opcode::ResultOP,
            Opcode::AuthChallenge,
            Opcode::AuthSuccess,
        ];
        if version == 0x04 && !reqs.contains(&opcode) {
            return Err(format!("Invalid opcode: {opcode} is not a request opcode"));
        }
        if version == 0x84 && !resp.contains(&opcode) {
            return Err(format!("Invalid opcode: {opcode} is not a response opcode"));
        }
        Ok(Header {
            version,
            flag,
            stream,
            opcode,
        })
    }

    pub fn read_header<R: Read>(reader: &mut R) -> std::io::Result<Header> {
        let mut buffer = [0u8; 5];
        reader.read_exact(&mut buffer)?;
        let version = u8::from_be(buffer[0]);
        if version != 0x04 && version != 0x84 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid version: expected 0x04 or 0x84, got {version}"),
            ));
        }

        let flag = u8::from_be(buffer[1]);

        let stream = u16::from_be_bytes([buffer[2], buffer[3]]);

        let opcode = Opcode::new(buffer[4])?;

        let reqs = vec![
            Opcode::Startup,
            Opcode::AuthResponse,
            Opcode::Options,
            Opcode::Query,
            Opcode::Prepare,
            Opcode::Execute,
        ];
        let resp = vec![
            Opcode::Error,
            Opcode::Ready,
            Opcode::Authenticate,
            Opcode::Supported,
            Opcode::ResultOP,
            Opcode::AuthChallenge,
            Opcode::AuthSuccess,
        ];
        if version == 0x04 && !reqs.contains(&opcode) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid opcode: {opcode} is not a request opcode"),
            ));
        }
        if version == 0x84 && !resp.contains(&opcode) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid opcode: {opcode} is not a response opcode"),
            ));
        }

        Ok(Header {
            version,
            flag,
            stream,
            opcode,
        })
    }

    pub fn write_header<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let version_be = self.version.to_be_bytes();
        writer.write_all(&version_be)?;

        let flags_be = self.flag.to_be_bytes();
        writer.write_all(&flags_be)?;

        let stream_be = self.stream.to_be_bytes();
        writer.write_all(&stream_be)?;

        let opcode_be = self.opcode.to_be_bytes();
        writer.write_all(&opcode_be)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_read_header() {
        let mut buffer = Cursor::new(&[0x04, 0x00, 0x00, 0x01, 0x01]);
        let header = Header::read_header(&mut buffer).unwrap();

        assert_eq!(header.version, 0x04);
        assert_eq!(header.flag, 0x00);
        assert_eq!(header.stream, 0x0001);
        assert_eq!(header.opcode, Opcode::Startup);
    }

    #[test]
    fn test_write_header() {
        let header = Header::new(0x04, 0x00, 0x0001, Opcode::Startup).unwrap();
        let mut buffer = Vec::new();
        header.write_header(&mut buffer).unwrap();

        let expected = vec![0x04, 0x00, 0x00, 0x01, 0x01];
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_read_header_invalid_version() {
        let mut buffer = Cursor::new(&[0x05, 0x00, 0x00, 0x01, 0x01]);
        let result = Header::read_header(&mut buffer);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_header_invalid_opcode() {
        let mut buffer = Cursor::new(&[0x04, 0x00, 0x00, 0x01, 0x11, 0x00, 0x00, 0x00, 0x0C]);
        let result = Header::read_header(&mut buffer);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_header_invalid_version() {
        assert!(Header::new(0x05, 0x00, 0x0001, Opcode::Startup).is_err());
    }

    #[test]
    fn test_write_header_invalid_opcode() {
        assert!(Header::new(0x04, 0x00, 0x0001, Opcode::Error).is_err());
    }
}
