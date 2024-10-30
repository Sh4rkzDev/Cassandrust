use std::{
    collections::HashMap,
    io::{Read, Write},
};

use shared::{io_error, map_io_error};

use crate::native_protocol::{
    models::consistency::ConsistencyLevel,
    parsers::string::{read_string, write_string},
};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ErrorCode {
    ServerError = 0x0000,
    ProtocolError = 0x000A,
    AuthenticationError = 0x0100,
    UnavailableError = 0x1000,
    Overloaded = 0x1001,
    Bootstrapping = 0x1002,
    ReadFailure = 0x1300,
    WriteFailure = 0x1500,
    SyntaxError = 0x2000,
    Unauthorized = 0x2100,
    Invalid = 0x2200,
    ConfigError = 0x2300,
    AlreadyExists = 0x2400,
    Unprepared = 0x2500,
}

impl ErrorCode {
    pub fn from_u16(value: u16) -> std::io::Result<ErrorCode> {
        match value {
            0x0000 => Ok(ErrorCode::ServerError),
            0x000A => Ok(ErrorCode::ProtocolError),
            0x0100 => Ok(ErrorCode::AuthenticationError),
            0x1000 => Ok(ErrorCode::UnavailableError),
            0x1001 => Ok(ErrorCode::Overloaded),
            0x1002 => Ok(ErrorCode::Bootstrapping),
            0x1300 => Ok(ErrorCode::ReadFailure),
            0x1500 => Ok(ErrorCode::WriteFailure),
            0x2000 => Ok(ErrorCode::SyntaxError),
            0x2100 => Ok(ErrorCode::Unauthorized),
            0x2200 => Ok(ErrorCode::Invalid),
            0x2300 => Ok(ErrorCode::ConfigError),
            0x2400 => Ok(ErrorCode::AlreadyExists),
            0x2500 => Ok(ErrorCode::Unprepared),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid error code: {value}"),
            )),
        }
    }

    pub fn to_u16(&self) -> u16 {
        match self {
            ErrorCode::ServerError => 0x0000,
            ErrorCode::ProtocolError => 0x000A,
            ErrorCode::AuthenticationError => 0x0100,
            ErrorCode::UnavailableError => 0x1000,
            ErrorCode::Overloaded => 0x1001,
            ErrorCode::Bootstrapping => 0x1002,
            ErrorCode::ReadFailure => 0x1300,
            ErrorCode::WriteFailure => 0x1500,
            ErrorCode::SyntaxError => 0x2000,
            ErrorCode::Unauthorized => 0x2100,
            ErrorCode::Invalid => 0x2200,
            ErrorCode::ConfigError => 0x2300,
            ErrorCode::AlreadyExists => 0x2400,
            ErrorCode::Unprepared => 0x2500,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Error {
    pub code: ErrorCode,
    pub message: String,
    extras: HashMap<String, String>,
}

impl Error {
    pub fn new(code: ErrorCode, message: String) -> Self {
        Error {
            code,
            message,
            extras: HashMap::new(),
        }
    }

    pub fn add_extra(&mut self, key: String, value: String) {
        self.extras.insert(key, value);
    }

    pub fn read_error<R: Read>(reader: &mut R) -> std::io::Result<(Error, u32)> {
        let mut code_buffer = [0u8; 4];
        reader.read_exact(&mut code_buffer)?;
        let code = i32::from_be_bytes(code_buffer);
        let mut bytes_read = 4;

        let (message, read) = read_string(reader)?;
        bytes_read += read;

        let mut extras = HashMap::new();

        if code == ErrorCode::UnavailableError as i32 {
            let mut consistency_buffer = [0u8; 2];
            reader.read_exact(&mut consistency_buffer)?;
            let consistency = ConsistencyLevel::from_u16(u16::from_be_bytes(consistency_buffer))?;
            bytes_read += 2;
            extras.insert("consistency".to_string(), consistency.to_string());

            let mut required_buffer = [0u8; 4];
            reader.read_exact(&mut required_buffer)?;
            bytes_read += 4;
            let required = i32::from_be_bytes(required_buffer);
            extras.insert("required".to_string(), required.to_string());

            let mut alive_buffer = [0u8; 4];
            reader.read_exact(&mut alive_buffer)?;
            bytes_read += 4;
            let alive = i32::from_be_bytes(alive_buffer);
            extras.insert("alive".to_string(), alive.to_string());
        } else if code == ErrorCode::ReadFailure as i32 || code == ErrorCode::WriteFailure as i32 {
            let mut consistency_buffer = [0u8; 2];
            reader.read_exact(&mut consistency_buffer)?;
            let consistency = ConsistencyLevel::from_u16(u16::from_be_bytes(consistency_buffer))?;
            bytes_read += 2;
            extras.insert("consistency".to_string(), consistency.to_string());

            let mut int_buffer = [0u8; 4];
            reader.read_exact(&mut int_buffer)?;
            bytes_read += 4;
            let received = i32::from_be_bytes(int_buffer);
            extras.insert("received".to_string(), received.to_string());

            reader.read_exact(&mut int_buffer)?;
            bytes_read += 4;
            let block_for = i32::from_be_bytes(int_buffer);
            extras.insert("block_for".to_string(), block_for.to_string());

            reader.read_exact(&mut int_buffer)?;
            bytes_read += 4;
            let failures = i32::from_be_bytes(int_buffer);
            extras.insert("failures".to_string(), failures.to_string());

            if code == ErrorCode::ReadFailure as i32 {
                let mut data_present_buffer = [0u8; 1];
                reader.read_exact(&mut data_present_buffer)?;
                bytes_read += 1;
                let data_present = u8::from_be_bytes(data_present_buffer);
                extras.insert("data_present".to_string(), data_present.to_string());
            } else {
                let (string, read) = read_string(reader)?;
                bytes_read += read;
                extras.insert("write_type".to_string(), string);
            }
        } else if code == ErrorCode::AlreadyExists as i32 {
            let (keyspace, read) = read_string(reader)?;
            bytes_read += read;
            extras.insert("keyspace".to_string(), keyspace);

            let (table, read) = read_string(reader)?;
            bytes_read += read;
            extras.insert("table".to_string(), table);
        }

        let error_code = ErrorCode::from_u16(code as u16)?;
        Ok((
            Error {
                code: error_code,
                message,
                extras,
            },
            bytes_read,
        ))
    }

    pub fn write_error<W: Write>(&self, writer: &mut W) -> std::io::Result<u32> {
        writer.write_all(&(self.code as i32).to_be_bytes())?;
        let mut bytes_written = 4;

        bytes_written += write_string(writer, &self.message)?;

        match self.code {
            ErrorCode::UnavailableError => {
                let consistency = self
                    .extras
                    .get("consistency")
                    .ok_or(io_error!("'consistency' not found"))?;
                writer
                    .write_all(&(ConsistencyLevel::from_str(consistency)? as u16).to_be_bytes())?;
                bytes_written += 2;

                let required = self
                    .extras
                    .get("required")
                    .ok_or(io_error!("'required' not found"))?;
                writer.write_all(
                    &required
                        .parse::<i32>()
                        .map_err(map_io_error!(
                            "Invalid data: cannot parse 'required' field to integer"
                        ))?
                        .to_be_bytes(),
                )?;
                bytes_written += 4;

                let alive = self
                    .extras
                    .get("alive")
                    .ok_or(io_error!("'alive' not found"))?;
                writer.write_all(
                    &alive
                        .parse::<i32>()
                        .map_err(map_io_error!(
                            "Invalid data: cannot parse 'alive' field to integer"
                        ))?
                        .to_be_bytes(),
                )?;
                bytes_written += 4;
            }
            ErrorCode::ReadFailure | ErrorCode::WriteFailure => {
                let consistency = self
                    .extras
                    .get("consistency")
                    .ok_or(io_error!("'consistency' not found"))?;
                writer
                    .write_all(&(ConsistencyLevel::from_str(consistency)? as u16).to_be_bytes())?;
                bytes_written += 2;

                let received = self
                    .extras
                    .get("received")
                    .ok_or(io_error!("'received' not found"))?;
                writer.write_all(
                    &received
                        .parse::<i32>()
                        .map_err(map_io_error!(
                            "Indalid data: cannot parse 'received' field to integer"
                        ))?
                        .to_be_bytes(),
                )?;
                bytes_written += 4;

                let block_for = self
                    .extras
                    .get("block_for")
                    .ok_or(io_error!("'block_for' not found"))?;
                writer.write_all(
                    &block_for
                        .parse::<i32>()
                        .map_err(map_io_error!(
                            "Invalid data: cannot parse 'block_for' to integer"
                        ))?
                        .to_be_bytes(),
                )?;
                bytes_written += 4;

                let failures = self
                    .extras
                    .get("failures")
                    .ok_or(io_error!("'failures' not found"))?;
                writer.write_all(
                    &failures
                        .parse::<i32>()
                        .map_err(map_io_error!(
                            "Invalid data: cannot parse 'failures' to integer"
                        ))?
                        .to_be_bytes(),
                )?;
                bytes_written += 4;

                if self.code == ErrorCode::ReadFailure {
                    let data_present = self
                        .extras
                        .get("data_present")
                        .ok_or(io_error!("'data_present' not found"))?;
                    writer.write_all(
                        &data_present
                            .parse::<u8>()
                            .map_err(map_io_error!(
                                "Invalid data: cannot parse 'data_present' to byte"
                            ))?
                            .to_be_bytes(),
                    )?;
                    bytes_written += 1;
                } else {
                    let write_type = self
                        .extras
                        .get("write_type")
                        .ok_or(io_error!("'write_type' not found"))?;
                    bytes_written += write_string(writer, write_type)?;
                }
            }
            ErrorCode::AlreadyExists => {
                let keyspace = self
                    .extras
                    .get("keyspace")
                    .ok_or(io_error!("'keyspace' not found"))?;
                bytes_written += write_string(writer, keyspace)?;

                let table = self
                    .extras
                    .get("table")
                    .ok_or(io_error!("'table' not found"))?;
                bytes_written += write_string(writer, table)?;
            }
            _ => {}
        }

        Ok(bytes_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_and_write_server_error() {
        let error = Error::new(
            ErrorCode::ServerError,
            "Something unexpected happened".to_string(),
        );

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_read_and_write_protocol_error() {
        let error = Error::new(
            ErrorCode::ProtocolError,
            "A protocol error occurred.".to_string(),
        );

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_read_and_write_authentication_error() {
        let error = Error::new(
            ErrorCode::AuthenticationError,
            "Authentication failed".to_string(),
        );

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_read_and_write_unavailable_error() {
        let mut error = Error::new(ErrorCode::UnavailableError, "Unavailable".to_string());
        error.add_extra("consistency".to_string(), "ONE".to_string());
        error.add_extra("required".to_string(), "1".to_string());
        error.add_extra("alive".to_string(), "0".to_string());

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_read_and_write_overloaded_error() {
        let error = Error::new(ErrorCode::Overloaded, "Overloaded".to_string());

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_read_and_write_bootstrapping_error() {
        let error = Error::new(ErrorCode::Bootstrapping, "Bootstrapping".to_string());

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_read_and_write_read_failure_error() {
        let mut error = Error::new(ErrorCode::ReadFailure, "Read failure".to_string());
        error.add_extra("consistency".to_string(), "ONE".to_string());
        error.add_extra("received".to_string(), "1".to_string());
        error.add_extra("block_for".to_string(), "2".to_string());
        error.add_extra("failures".to_string(), "1".to_string());
        error.add_extra("data_present".to_string(), "1".to_string());

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_read_and_write_write_failure_error() {
        let mut error = Error::new(ErrorCode::WriteFailure, "Write failure".to_string());
        error.add_extra("consistency".to_string(), "ONE".to_string());
        error.add_extra("received".to_string(), "1".to_string());
        error.add_extra("block_for".to_string(), "2".to_string());
        error.add_extra("failures".to_string(), "1".to_string());
        error.add_extra("write_type".to_string(), "SIMPLE".to_string());

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_read_and_write_syntax_error() {
        let error = Error::new(ErrorCode::SyntaxError, "Syntax error".to_string());

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_read_and_write_unauthorized_error() {
        let error = Error::new(ErrorCode::Unauthorized, "Unauthorized".to_string());

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_read_and_write_invalid_error() {
        let error = Error::new(ErrorCode::Invalid, "Invalid".to_string());

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_read_and_write_config_error() {
        let error = Error::new(ErrorCode::ConfigError, "Config error".to_string());

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_read_and_write_already_exists_error() {
        let mut error = Error::new(ErrorCode::AlreadyExists, "Already exists".to_string());
        error.add_extra("keyspace".to_string(), "ks".to_string());
        error.add_extra("table".to_string(), "tbl".to_string());

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_read_and_write_unprepared_error() {
        let error = Error::new(ErrorCode::Unprepared, "Unprepared".to_string());

        let mut buffer = Vec::new();
        error.write_error(&mut buffer).unwrap();
        let (read_error, _) = Error::read_error(&mut buffer.as_slice()).unwrap();

        assert_eq!(error, read_error);
    }

    #[test]
    fn test_invalid_unavailable_error() {
        let mut error = Error::new(ErrorCode::UnavailableError, "Unavailable".to_string());
        error.add_extra("consistency".to_string(), "ONE".to_string());
        error.add_extra("required".to_string(), "1".to_string());

        let mut buffer = Vec::new();
        assert!(error.write_error(&mut buffer).is_err());
    }

    #[test]
    fn test_invalid_read_failure_error() {
        let mut error = Error::new(ErrorCode::ReadFailure, "Read failure".to_string());
        error.add_extra("consistency".to_string(), "ONE".to_string());
        error.add_extra("received".to_string(), "1".to_string());
        error.add_extra("block_for".to_string(), "2".to_string());
        error.add_extra("failures".to_string(), "1".to_string());

        let mut buffer = Vec::new();
        assert!(error.write_error(&mut buffer).is_err());
    }

    #[test]
    fn test_invalid_write_failure_error() {
        let mut error = Error::new(ErrorCode::WriteFailure, "Write failure".to_string());
        error.add_extra("consistency".to_string(), "ONE".to_string());
        error.add_extra("received".to_string(), "1".to_string());
        error.add_extra("block_for".to_string(), "2".to_string());
        error.add_extra("failures".to_string(), "1".to_string());

        let mut buffer = Vec::new();
        assert!(error.write_error(&mut buffer).is_err());
    }

    #[test]
    fn test_invalid_already_exists_error() {
        let mut error = Error::new(ErrorCode::AlreadyExists, "Already exists".to_string());
        error.add_extra("keyspace".to_string(), "ks".to_string());

        let mut buffer = Vec::new();
        assert!(error.write_error(&mut buffer).is_err());
    }
}
