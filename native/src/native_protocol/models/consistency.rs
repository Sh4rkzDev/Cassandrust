use std::fmt::Display;

use shared::io_error;

/// A short (u16) representing possibles consistency levels.
#[derive(Debug, PartialEq, Clone)]
pub enum ConsistencyLevel {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
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
            _ => Err(io_error!("Invalid Consistency Level")),
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
            _ => Err(io_error!("Invalid Consistency Level")),
        }
    }

    pub fn to_u16(&self) -> u16 {
        match self {
            ConsistencyLevel::Any => 0x0000,
            ConsistencyLevel::One => 0x0001,
            ConsistencyLevel::Two => 0x0002,
            ConsistencyLevel::Three => 0x0003,
            ConsistencyLevel::Quorum => 0x0004,
            ConsistencyLevel::All => 0x0005,
        }
    }
}
