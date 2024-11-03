use std::fmt::Display;

/// A short (u16) representing possibles consistency levels.
#[derive(Debug, PartialEq, Clone)]
pub enum ConsistencyLevel {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    Serial = 0x0008,
    LocalSerial = 0x0009,
    LocalOne = 0x000A,
}

impl Display for ConsistencyLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsistencyLevel::Any => write!(f, "ANY"),
            ConsistencyLevel::One => write!(f, "ONE"),
            ConsistencyLevel::Two => write!(f, "TWO"),
            ConsistencyLevel::Three => write!(f, "THREE"),
            ConsistencyLevel::Quorum => write!(f, "QUORUM"),
            ConsistencyLevel::All => write!(f, "ALL"),
            ConsistencyLevel::LocalQuorum => write!(f, "LOCAL_QUORUM"),
            ConsistencyLevel::EachQuorum => write!(f, "EACH_QUORUM"),
            ConsistencyLevel::Serial => write!(f, "SERIAL"),
            ConsistencyLevel::LocalSerial => write!(f, "LOCAL_SERIAL"),
            ConsistencyLevel::LocalOne => write!(f, "LOCAL_ONE"),
        }
    }
}

impl ConsistencyLevel {
    pub fn from_u16(value: u16) -> std::io::Result<Self> {
        match value {
            0x0000 => Ok(ConsistencyLevel::Any),
            0x0001 => Ok(ConsistencyLevel::One),
            0x0002 => Ok(ConsistencyLevel::Two),
            0x0003 => Ok(ConsistencyLevel::Three),
            0x0004 => Ok(ConsistencyLevel::Quorum),
            0x0005 => Ok(ConsistencyLevel::All),
            0x0006 => Ok(ConsistencyLevel::LocalQuorum),
            0x0007 => Ok(ConsistencyLevel::EachQuorum),
            0x0008 => Ok(ConsistencyLevel::Serial),
            0x0009 => Ok(ConsistencyLevel::LocalSerial),
            0x000A => Ok(ConsistencyLevel::LocalOne),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid Consistency Level",
            )),
        }
    }

    pub fn from_str(consistency: &str) -> std::io::Result<Self> {
        match consistency {
            "ANY" => Ok(ConsistencyLevel::Any),
            "ONE" => Ok(ConsistencyLevel::One),
            "TWO" => Ok(ConsistencyLevel::Two),
            "THREE" => Ok(ConsistencyLevel::Three),
            "QUORUM" => Ok(ConsistencyLevel::Quorum),
            "ALL" => Ok(ConsistencyLevel::All),
            "LOCAL_QUORUM" => Ok(ConsistencyLevel::LocalQuorum),
            "EACH_QUORUM" => Ok(ConsistencyLevel::EachQuorum),
            "SERIAL" => Ok(ConsistencyLevel::Serial),
            "LOCAL_SERIAL" => Ok(ConsistencyLevel::LocalSerial),
            "LOCAL_ONE" => Ok(ConsistencyLevel::LocalOne),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid Consistency Level",
            )),
        }
    }
}
