use std::{
    collections::HashMap,
    fmt::Display,
    io::{BufRead, BufReader, BufWriter, Read, Write},
};

use chrono::DateTime;
use serde::{Deserialize, Serialize};
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
            _ => Err(io_error!("Invalid schema type")),
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
                let val1 = val1.parse::<bool>().unwrap();
                let val2 = val2.parse::<bool>().unwrap();
                Ok(val1.cmp(&val2))
            }
            SchemaType::Float => {
                let val1 = val1.parse::<f32>().unwrap();
                let val2 = val2.parse::<f32>().unwrap();
                Ok(val1.total_cmp(&val2))
            }
            SchemaType::Int => {
                let val1 = val1.parse::<i32>().unwrap();
                let val2 = val2.parse::<i32>().unwrap();
                Ok(val1.cmp(&val2))
            }
            SchemaType::Text | SchemaType::Timestamp => Ok(val1.cmp(val2)),
        }
    }

    fn check_type(&self, value: &str) -> std::io::Result<()> {
        match self {
            SchemaType::Boolean => {
                if value != "true" && value != "false" {
                    Err(io_error!("Invalid boolean value"))
                } else {
                    Ok(())
                }
            }
            SchemaType::Float => {
                if value.parse::<f32>().is_err() {
                    Err(io_error!("Invalid float value"))
                } else {
                    Ok(())
                }
            }
            SchemaType::Int => {
                if value.parse::<i32>().is_err() {
                    Err(io_error!("Invalid int value"))
                } else {
                    Ok(())
                }
            }
            SchemaType::Text => Ok(()),
            SchemaType::Timestamp => DateTime::parse_from_rfc3339(value)
                .map_err(map_io_error!("Invalid timestamp value"))
                .map(|_| ()),
        }
    }
}

/// Represents the schema of a table.  
/// The schema contains the columns and the primary key.  
/// Each column has a name and a data type and is used to parse the data from the table.  
/// The primary key contains the partition key and the clustering key.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    pub fn add_column(&mut self, column: String, schema_type: SchemaType) {
        self.columns.insert(column, schema_type);
    }

    /// Returns a function that parses the data in form of bytes for the specified column.
    /// This function returns a string representation of the data from the bytes.
    ///
    /// Useful for bound variables in the query.
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

    /// Reads the schema from the specified reader.
    ///
    /// ** The reader **must not** be a buffered reader. **
    /// This function is in charge of buffering the reader.
    ///
    /// The schema source must have the following format:
    /// - For each column, the line must have the column name and the data type separated by a space.
    /// - The `PARTITION_KEY` line must have the keyword `PARTITION_KEY` followed by the partition key columns separated by spaces.
    /// - The `CLUSTERING_KEY` line must have the keyword `CLUSTERING_KEY` followed by the clustering key columns separated by spaces.
    pub(crate) fn read<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut columns = HashMap::new();
        let mut partition_key = Vec::new();
        let mut clustering_key = Vec::new();

        let reader = BufReader::new(reader);
        for line in reader.lines() {
            let line = line?;
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 && parts[0] != "CLUSTERING_KEY" {
                return Err(io_error!(
                    "Invalid schema: each line must have at least two parts. Error Line: "
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
            return Err(io_error!("Invalid schema: no PARTITION_KEY found"));
        }
        Ok(Schema {
            columns,
            primary_key: PrimaryKey::new(partition_key, clustering_key),
        })
    }

    /// Writes the schema to the specified writer.
    ///
    /// ** The writer **must not** be a buffered writer. **
    /// This function is in charge of buffering the writer.
    ///
    /// The schema is written in the following format:
    /// - For each column, the line has the column name and the data type separated by a space.
    /// - The `PARTITION_KEY` line has the keyword `PARTITION_KEY` followed by the partition key columns separated by spaces.
    /// - The `CLUSTERING_KEY` line has the keyword `CLUSTERING_KEY` followed by the clustering key columns separated by spaces.
    pub(crate) fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let mut writer = BufWriter::new(writer);
        for (column, schema_type) in &self.columns {
            writer.write_fmt(format_args!("{column} {schema_type}\n"))?;
        }
        writer.write_fmt(format_args!(
            "PARTITION_KEY {}\n",
            self.primary_key.get_partition_key().join(" ")
        ))?;
        writer.write_fmt(format_args!(
            "CLUSTERING_KEY {}\n",
            self.primary_key.get_clustering_key().join(" ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashSet, io::Cursor};

    #[test]
    fn test_schema_serde() {
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), SchemaType::Int);
        columns.insert("name".to_string(), SchemaType::Text);
        let schema = Schema::new(
            columns,
            PrimaryKey::new(vec!["id".to_string()], vec!["name".to_string()]),
        );
        let mut buffer = Cursor::new(Vec::new());
        schema.write(&mut buffer).unwrap();
        buffer.set_position(0);
        let read_schema = Schema::read(&mut buffer).unwrap();
        assert_eq!(
            schema.get_columns().iter().collect::<HashSet<_>>(),
            read_schema.get_columns().iter().collect::<HashSet<_>>()
        );
        assert_eq!(
            schema.get_primary_key().get_partition_key(),
            read_schema.get_primary_key().get_partition_key()
        );
        assert_eq!(
            schema.get_primary_key().get_clustering_key(),
            read_schema.get_primary_key().get_clustering_key()
        );
        assert_eq!(
            schema.get_schema_type("id").unwrap().to_string(),
            read_schema.get_schema_type("id").unwrap().to_string()
        );
        assert_eq!(
            schema.get_schema_type("name").unwrap().to_string(),
            read_schema.get_schema_type("name").unwrap().to_string()
        );
    }

    #[test]
    fn test_schema_parse_function() {
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), SchemaType::Int);
        columns.insert("name".to_string(), SchemaType::Text);
        let schema = Schema::new(
            columns,
            PrimaryKey::new(vec!["id".to_string()], vec!["name".to_string()]),
        );
        let parse_id = schema.get_parse_function("id").unwrap();
        let parse_name = schema.get_parse_function("name").unwrap();
        assert_eq!(parse_id(&[0, 0, 0, 1]).unwrap(), "1");
        assert_eq!(parse_name(b"test").unwrap(), "test");
    }

    #[test]
    fn test_schema_cmp() {
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), SchemaType::Int);
        columns.insert("name".to_string(), SchemaType::Text);
        let schema = Schema::new(
            columns,
            PrimaryKey::new(vec!["id".to_string()], vec!["name".to_string()]),
        );
        assert_eq!(
            schema.get_schema_type("id").unwrap().cmp("1", "2").unwrap(),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            schema
                .get_schema_type("name")
                .unwrap()
                .cmp("test", "test")
                .unwrap(),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn test_schema_check_type() {
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), SchemaType::Int);
        columns.insert("name".to_string(), SchemaType::Text);
        let schema = Schema::new(
            columns,
            PrimaryKey::new(vec!["id".to_string()], vec!["name".to_string()]),
        );
        assert!(schema.check_type("id", "1").is_ok());
        assert!(schema.check_type("name", "test").is_ok());
        assert!(schema.check_type("id", "test").is_err());
    }

    #[test]
    fn test_schema_check_type_timestamp() {
        let mut columns = HashMap::new();
        columns.insert("timestamp".to_string(), SchemaType::Timestamp);
        let schema = Schema::new(
            columns,
            PrimaryKey::new(vec!["timestamp".to_string()], vec![]),
        );
        assert!(schema
            .check_type("timestamp", "2021-01-01T00:00:00Z")
            .is_ok());
        assert!(schema.check_type("timestamp", "2021-01-01").is_err());
        assert!(schema.check_type("timestamp", "test").is_err());
        assert!(schema
            .check_type("timestamp", "2021-01-01T00:00:00+00:00")
            .is_ok());
    }

    #[test]
    fn test_schema_check_type_boolean() {
        let mut columns = HashMap::new();
        columns.insert("boolean".to_string(), SchemaType::Boolean);
        let schema = Schema::new(
            columns,
            PrimaryKey::new(vec!["boolean".to_string()], vec![]),
        );
        assert!(schema.check_type("boolean", "true").is_ok());
        assert!(schema.check_type("boolean", "false").is_ok());
        assert!(schema.check_type("boolean", "test").is_err());
    }

    #[test]
    fn test_schema_check_type_float() {
        let mut columns = HashMap::new();
        columns.insert("float".to_string(), SchemaType::Float);
        let schema = Schema::new(columns, PrimaryKey::new(vec!["float".to_string()], vec![]));
        assert!(schema.check_type("float", "1.0").is_ok());
        assert!(schema.check_type("float", "test").is_err());
    }

    #[test]
    fn test_schema_check_type_int() {
        let mut columns = HashMap::new();
        columns.insert("int".to_string(), SchemaType::Int);
        let schema = Schema::new(columns, PrimaryKey::new(vec!["int".to_string()], vec![]));
        assert!(schema.check_type("int", "1").is_ok());
        assert!(schema.check_type("int", "test").is_err());
    }

    #[test]
    fn test_schema_check_type_text() {
        let mut columns = HashMap::new();
        columns.insert("text".to_string(), SchemaType::Text);
        let schema = Schema::new(columns, PrimaryKey::new(vec!["text".to_string()], vec![]));
        assert!(schema.check_type("text", "test").is_ok());
    }

    #[test]
    fn test_schema_check_type_not_found() {
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), SchemaType::Int);
        let schema = Schema::new(columns, PrimaryKey::new(vec!["id".to_string()], vec![]));
        assert!(schema.check_type("name", "test").is_err());
    }
}
