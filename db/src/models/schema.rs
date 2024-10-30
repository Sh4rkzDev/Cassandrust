use std::{
    collections::HashMap,
    fmt::Display,
    fs::File,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use shared::{io_error, map_io_error};

use super::primary_key::PrimaryKey;

/// Represents the data types of the columns of the table.
/// The data types are used to parse the data from the table.
///
/// Supported data types:
/// - Boolean
/// - Float
/// - Int
/// - Text
/// - Timestamp
#[derive(Debug, Clone)]
pub enum SchemaType {
    Boolean,
    Float,
    Int,
    Text,
    Timestamp,
}

impl Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaType::Boolean => write!(f, "boolean"),
            SchemaType::Float => write!(f, "float"),
            SchemaType::Int => write!(f, "int"),
            SchemaType::Text => write!(f, "text"),
            SchemaType::Timestamp => write!(f, "timestamp"),
        }
    }
}

impl SchemaType {
    /// Returns the `SchemaType` for the specified value type.
    /// The value type must be one of the supported data types.
    pub fn new(value_type: &str) -> std::io::Result<Self> {
        match value_type {
            "boolean" => Ok(SchemaType::Boolean),
            "float" => Ok(SchemaType::Float),
            "int" => Ok(SchemaType::Int),
            "text" => Ok(SchemaType::Text),
            "timestamp" => Ok(SchemaType::Timestamp),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid schema type",
            )),
        }
    }

    fn get_parse_function(&self) -> fn(&[u8]) -> std::io::Result<String> {
        match self {
            SchemaType::Boolean => {
                fn parse_boolean(bytes: &[u8]) -> std::io::Result<String> {
                    if bytes.len() != 1 {
                        return Err(io_error!("Invalid boolean value"));
                    }
                    match bytes {
                        [0] => Ok("false".to_string()),
                        _ => Ok("true".to_string()),
                    }
                }
                parse_boolean
            }
            SchemaType::Float => {
                fn parse_float(bytes: &[u8]) -> std::io::Result<String> {
                    if bytes.len() != 4 {
                        return Err(io_error!("Invalid float value"));
                    }
                    Ok(f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string())
                }
                parse_float
            }
            SchemaType::Int => {
                fn parse_int(bytes: &[u8]) -> std::io::Result<String> {
                    if bytes.len() != 4 {
                        return Err(io_error!("Invalid int value"));
                    }
                    Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string())
                }
                parse_int
            }
            SchemaType::Text => {
                fn parse_text(bytes: &[u8]) -> std::io::Result<String> {
                    String::from_utf8(bytes.to_vec()).map_err(map_io_error!("Invalid text value"))
                }
                parse_text
            }
            SchemaType::Timestamp => {
                fn parse_timestamp(bytes: &[u8]) -> std::io::Result<String> {
                    String::from_utf8(bytes.to_vec())
                        .map_err(map_io_error!("Invalid timestamp value"))
                }
                parse_timestamp
            }
        }
    }

    pub fn cmp(&self, val1: &str, val2: &str) -> std::io::Result<std::cmp::Ordering> {
        self.check_type(val1)?;
        self.check_type(val2)?;
        match self {
            SchemaType::Boolean => {
                let val1 = val1
                    .parse::<bool>()
                    .map_err(map_io_error!(format!("Invalid value: {val1}")))?;
                let val2 = val2
                    .parse::<bool>()
                    .map_err(map_io_error!(format!("Invalid value: {val2}")))?;
                Ok(val1.cmp(&val2))
            }
            SchemaType::Float => {
                let val1 = val1
                    .parse::<f32>()
                    .map_err(map_io_error!(format!("Invalid value: {val1}")))?;
                let val2 = val2
                    .parse::<f32>()
                    .map_err(map_io_error!(format!("Invalid value: {val2}")))?;
                Ok(val1.total_cmp(&val2))
            }
            SchemaType::Int => {
                let val1 = val1
                    .parse::<i32>()
                    .map_err(map_io_error!(format!("Invalid value: {val1}")))?;
                let val2 = val2
                    .parse::<i32>()
                    .map_err(map_io_error!(format!("Invalid value: {val2}")))?;
                Ok(val1.cmp(&val2))
            }
            SchemaType::Text => Ok(val1.cmp(val2)),
            SchemaType::Timestamp => Ok(val1.cmp(val2)),
        }
    }

    fn check_type(&self, value: &str) -> std::io::Result<()> {
        match self {
            SchemaType::Boolean => {
                if value != "true" && value != "false" {
                    return Err(io_error!("Invalid boolean value"));
                }
            }
            SchemaType::Float => {
                if value.parse::<f32>().is_err() {
                    return Err(io_error!("Invalid float value"));
                }
            }
            SchemaType::Int => {
                if value.parse::<i32>().is_err() {
                    return Err(io_error!("Invalid int value"));
                }
            }
            SchemaType::Text => {}
            SchemaType::Timestamp => {} // TODO: Add timestamp validation with chrono
        }
        Ok(())
    }
}

/// Represents the schema of a table.
/// The schema contains the columns and the primary key.  
/// Each column has a name and a data type and is used to parse the data from the table.  
/// The primary key contains the partition key and the clustering key.
#[derive(Debug, Clone)]
pub struct Schema {
    columns: HashMap<String, SchemaType>,
    primary_key: PrimaryKey,
}

impl Schema {
    /// Creates a new schema with the specified columns and primary key.
    pub fn new(columns: HashMap<String, SchemaType>, primary_key: PrimaryKey) -> Self {
        Schema {
            columns,
            primary_key,
        }
    }

    /// Returns a function that parses the data in form of bytes for the specified column.
    /// This function returns a string representation of the data from the bytes.
    pub fn get_parse_function(
        &self,
        column_name: &str,
    ) -> Option<fn(&[u8]) -> std::io::Result<String>> {
        self.columns
            .get(column_name)
            .map(|schema_type| schema_type.get_parse_function())
    }

    pub fn get_columns(&self) -> Vec<String> {
        self.columns.keys().cloned().collect()
    }

    pub fn get_primary_key(&self) -> &PrimaryKey {
        &self.primary_key
    }

    pub fn get_schema_type(&self, column: &str) -> Option<&SchemaType> {
        self.columns.get(column)
    }

    pub fn check_type(&self, column: &str, value: &str) -> std::io::Result<()> {
        self.columns
            .get(column)
            .ok_or(io_error!("Column not found"))
            .and_then(|schema_type| schema_type.check_type(value))
    }

    pub(crate) fn read_schema_file(path: &Path) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut columns = HashMap::new();
        let mut partition_key = Vec::new();
        let mut clustering_key = Vec::new();

        for line in reader.lines() {
            let line = line?;
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 {
                return Err(io_error!(
                    "Invalid schema file: each line must have at least two parts. Error Line: "
                        .to_owned()
                        + &line
                ));
            }
            if parts[0] == "PARTITION_KEY" {
                partition_key = parts[1..]
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect();
            } else if parts[0] == "CLUSTERING_KEY" {
                clustering_key = parts[1..]
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect();
            } else {
                columns.insert(parts[0].to_string(), SchemaType::new(parts[1])?);
            }
        }
        if partition_key.is_empty() {
            return Err(io_error!("Invalid schema file: no PARTITION_KEY found"));
        }
        return Ok(Schema {
            columns,
            primary_key: PrimaryKey::new(partition_key, clustering_key),
        });
    }

    pub(crate) fn write_schema_file(&self, table_dir: &PathBuf) -> std::io::Result<()> {
        let mut file = File::create(table_dir.join("table.schema"))?;
        for (column, schema_type) in &self.columns {
            file.write_fmt(format_args!("{} {}\n", column, schema_type))?;
        }
        file.write_fmt(format_args!(
            "PARTITION_KEY {}\n",
            self.primary_key.get_partition_key().join(" ")
        ))?;
        file.write_fmt(format_args!(
            "CLUSTERING_KEY {}\n",
            self.primary_key.get_clustering_key().join(" ")
        ))
    }
}
