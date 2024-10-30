use std::{
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

#[derive(Debug, PartialEq)]
pub struct Inet {
    pub address: IpAddr,
    pub port: i32,
}

impl Inet {
    /// Reads an Inet from a reader. The Inet is expected to be serialized as:
    /// - size: u8 = size of the address (4 for IPv4, 16 for IPv6)
    /// - address: [u8; size] = the address
    /// - port: i32 = the port
    ///
    /// # Returns
    /// A tuple containing the Inet and the number of bytes read from the reader.
    pub fn read_inet<R: Read>(reader: &mut R) -> std::io::Result<(Self, u32)> {
        let mut buffer = [0u8; 1];
        reader.read_exact(&mut buffer)?;
        let size = u8::from_be_bytes(buffer);
        let mut bytes_read = 1u32;
        let address: IpAddr;
        match size {
            0x04 => {
                let mut address_buffer = [0u8; 4];
                reader.read_exact(&mut address_buffer)?;
                bytes_read += 4;
                address = std::net::IpAddr::V4(Ipv4Addr::from(address_buffer));
            }
            0x10 => {
                let mut address_buffer = [0u8; 16];
                reader.read_exact(&mut address_buffer)?;
                bytes_read += 16;
                address = std::net::IpAddr::V6(Ipv6Addr::from(address_buffer));
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid inet size: {}", size),
                ))
            }
        }
        let mut port_buffer = [0u8; 4];
        reader.read_exact(&mut port_buffer)?;
        let port = i32::from_be_bytes(port_buffer);
        Ok((Inet { address, port }, bytes_read + 4))
    }

    /// Writes an Inet to a writer. The Inet is serialized as:
    /// - size: u8 = size of the address (4 for IPv4, 16 for IPv6)
    /// - address: [u8; size] = the address
    /// - port: i32 = the port
    ///
    /// # Returns
    /// The number of bytes written to the writer.
    pub fn write_inet<W: Write>(writer: &mut W, inet: &Inet) -> std::io::Result<u32> {
        let bytes_written;
        match inet.address {
            IpAddr::V4(v) => {
                writer.write_all(&0x04_u8.to_be_bytes())?;
                writer.write_all(&v.octets())?;
                bytes_written = 5;
            }
            IpAddr::V6(v) => {
                writer.write_all(&0x10_u8.to_be_bytes())?;
                writer.write_all(&v.octets())?;
                bytes_written = 17;
            }
        };
        writer.write_all(&inet.port.to_be_bytes())?;
        Ok(bytes_written + 4)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use super::Inet;

    #[test]
    fn test_read_inet() {
        let mut input = std::io::Cursor::new(&[
            0x04, // size = 4
            0x7F, 0x00, 0x00, 0x01, // address = 127.0.0.1
            0x00, 0x00, 0x00, 0x50, // port = 80
        ]);
        let (inet, read) = Inet::read_inet(&mut input).unwrap();
        assert_eq!(read, 9);
        assert_eq!(
            inet,
            Inet {
                address: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                port: 80
            }
        );
    }

    #[test]
    fn test_write_inet() {
        let inet = Inet {
            address: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 80,
        };
        let mut output = Vec::new();
        assert_eq!(Inet::write_inet(&mut output, &inet).unwrap(), 9);
        assert_eq!(
            output,
            vec![
                0x04, // size = 4
                0x7F, 0x00, 0x00, 0x01, // address = 127.0.0.1
                0x00, 0x00, 0x00, 0x50, // port = 80
            ]
        );
    }

    #[test]
    fn test_read_inet_ipv6() {
        let mut input = std::io::Cursor::new(&[
            0x10, // size = 16
            0x20, 0x01, 0x0D, 0xB8, // address = 2001:db8::
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x50, // port = 80
        ]);
        let (inet, read) = Inet::read_inet(&mut input).unwrap();
        assert_eq!(read, 21);
        assert_eq!(
            inet,
            Inet {
                address: IpAddr::V6(Ipv6Addr::new(0x2001, 0x0DB8, 0, 0, 0, 0, 0, 1)),
                port: 80
            }
        );
    }

    #[test]
    fn test_write_inet_ipv6() {
        let inet = Inet {
            address: IpAddr::V6(Ipv6Addr::new(0x2001, 0x0DB8, 0, 0, 0, 0, 0, 1)),
            port: 80,
        };
        let mut output = Vec::new();
        assert_eq!(Inet::write_inet(&mut output, &inet).unwrap(), 21);
        assert_eq!(
            output,
            vec![
                0x10, // size = 16
                0x20, 0x01, 0x0D, 0xB8, // address = 2001:db8::
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
                0x00, 0x50, // port = 80
            ]
        );
    }

    #[test]
    fn test_read_inet_invalid_size() {
        let mut input = std::io::Cursor::new(&[0xFF]);
        let err = Inet::read_inet(&mut input).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_read_inet_invalid_address() {
        let mut input = std::io::Cursor::new(&[0x04, 0xFF, 0xFF, 0xFF, 0xFF]);
        let err = Inet::read_inet(&mut input).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_read_inet_invalid_port() {
        let mut input = std::io::Cursor::new(&[0x04, 0x7F, 0x00, 0x00, 0x01, 0xFF]);
        let err = Inet::read_inet(&mut input).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_read_inet_incomplete() {
        let mut input = std::io::Cursor::new(&[0x04, 0x7F, 0x00, 0x00]);
        let err = Inet::read_inet(&mut input).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_write_inet_incomplete() {
        let inet = Inet {
            address: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 80,
        };
        let mut output = Vec::new();
        Inet::write_inet(&mut output, &inet).unwrap();
        output.pop();
        let mut input = std::io::Cursor::new(&output);
        let err = Inet::read_inet(&mut input).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_read_and_write_inet() {
        let inet = Inet {
            address: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 80,
        };
        let mut output = Vec::new();
        let written = Inet::write_inet(&mut output, &inet).unwrap();
        let mut input = std::io::Cursor::new(&output);
        let (read_inet, read) = Inet::read_inet(&mut input).unwrap();
        assert_eq!(written, read);
        assert_eq!(inet, read_inet);
    }
}
