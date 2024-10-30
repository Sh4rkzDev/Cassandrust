use std::{
    collections::HashMap,
    fs::{self, rename, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use shared::{get_keyspace, io_error, not_found_error};

use super::schema::Schema;

/// Represents the tables in the keyspace.
/// The tables contain the schema of the tables and are used to create, drop, and read tables.
pub(crate) struct Tables {
    tables: HashMap<String, Schema>,
}

impl Tables {
    pub(crate) fn new() -> Self {
        Tables {
            tables: HashMap::new(),
        }
    }

    /// Reads the schema of the tables in the keyspace.
    /// Should be called when the server starts to load the schema of the tables on startup.
    ///
    /// # Arguments
    ///
    /// * `keyspace` - A reference to the path of the keyspace.
    ///
    /// # Returns
    ///
    /// * Returns a `Tables` with the schema of the tables in the keyspace.
    pub(crate) fn get_tables_schema(keyspace: &PathBuf) -> std::io::Result<Self> {
        let mut tables: HashMap<String, Schema> = HashMap::new();
        for entry in fs::read_dir(keyspace)? {
            let table = entry?;
            let table_path = table.path().join("table.schema");
            if !table_path.is_dir() {
                continue;
            }

            tables.insert(
                table
                    .file_name()
                    .to_str()
                    .ok_or(io_error!("Invalid table name"))?
                    .to_string(),
                Schema::read_schema_file(&table_path)?,
            );
        }
        Ok(Tables { tables })
    }

    pub(crate) fn create_table(&mut self, table: &str, schema: Schema) -> std::io::Result<()> {
        if self.tables.contains_key(table) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "Table already exists",
            ));
        }

        let table_dir = get_keyspace().join(table);
        fs::create_dir_all(&table_dir)?;
        let table_schema = table_dir.join("table.schema");

        schema.write_schema_file(&table_schema)?;

        for key in schema.get_primary_key().get_partition_key() {
            let partition_key = table_dir.join(key).with_extension("csv");
            let mut file = fs::File::create(&partition_key)?;
            let columns = schema.get_columns();
            file.write_fmt(format_args!("{}\n", columns.join(",")))?;
        }

        self.tables.insert(table.to_string(), schema);
        Ok(())
    }

    pub(crate) fn drop_table(&mut self, table: &str) -> std::io::Result<()> {
        fs::remove_dir_all(get_keyspace().join(table))?;

        self.tables
            .remove(table)
            .map(|_| ())
            .ok_or(not_found_error!("Table does not exist"))
    }

    pub(crate) fn get_table_schema(&self, table: &str) -> std::io::Result<Schema> {
        self.tables
            .get(table)
            .ok_or(not_found_error!("Table does not exist"))
            .cloned()
    }

    pub(crate) fn read_table(
        &self,
        table: &str,
        key: &str,
        visitor: &mut dyn FnMut(&HashMap<String, String>) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        let table_file = get_keyspace().join(table).join(key).with_extension("csv");

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(table_file)?;

        let headers = reader.headers()?.clone();

        for result in reader.records() {
            let record = result?;
            let map = map_string_record_to_hashmap(&record, &headers);
            visitor(&map)?;
        }

        Ok(())
    }

    pub(crate) fn append_to_table(
        &self,
        table: &str,
        key: &str,
        data: &HashMap<String, String>,
    ) -> std::io::Result<()> {
        let table_file = get_keyspace().join(table).join(key).with_extension("csv");
        ensure_newline_at_end(&table_file)?;
        let cols = get_columns(&table_file)?;

        let mut row = vec!["".to_string(); cols.len()];
        for (idx, col) in cols.iter().enumerate() {
            let null = "NULL".to_string();
            let value = data.get(col).cloned().unwrap_or(null);
            if value != "NULL" {
                self.tables
                    .get(table)
                    .ok_or(not_found_error!(format!("Table '{table}' not found")))?
                    .check_type(&col, &value)?;
            }
            row[idx] = value;
        }
        let mut file = OpenOptions::new().append(true).open(table_file)?;
        writeln!(file, "{}", row.join(","))
    }

    pub(crate) fn update_table(
        &self,
        table: &str,
        key: &str,
        visitor: &dyn Fn(
            &HashMap<String, String>,
        ) -> std::io::Result<Option<HashMap<String, String>>>,
    ) -> std::io::Result<()> {
        let input_file = get_keyspace().join(table).join(key).with_extension("csv");
        let output_file = get_keyspace().join(table).join(key).with_extension("tmp");
        let cols = get_columns(&input_file)?;

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(&input_file)?;
        let headers = reader.headers()?.clone();
        let mut writer = csv::Writer::from_path(&output_file)?;

        writer.write_record(&cols)?;

        for result in reader.records() {
            let record = result?;
            let map = map_string_record_to_hashmap(&record, &headers);
            let Some(updated_row) = visitor(&map)? else {
                continue;
            };
            let mut row = vec![""; cols.len()];
            let null = "NULL".to_string();
            for (idx, col) in cols.iter().enumerate() {
                let value = updated_row
                    .get(col)
                    .unwrap_or(map.get(col).unwrap_or(&null));
                if value != "NULL" {
                    self.tables
                        .get(table)
                        .ok_or(not_found_error!(format!("Table '{table}' not found")))?
                        .check_type(&col, value)?;
                }
                row[idx] = value;
            }
            writer.write_record(&row)?;
        }
        rename(output_file, input_file)
    }
}

fn ensure_newline_at_end(path: &PathBuf) -> std::io::Result<()> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let end_pos = file.seek(SeekFrom::End(0))?;
    if end_pos == 0 {
        return Ok(());
    }
    file.seek(SeekFrom::End(-1))?;
    let mut last_byte = [0; 1];
    file.read_exact(&mut last_byte)?;
    if last_byte[0] != b'\n' {
        file.seek(SeekFrom::End(0))?;
        file.write_all(b"\n")?;
    }

    Ok(())
}

fn get_columns(path: &PathBuf) -> std::io::Result<Vec<String>> {
    let Ok(file) = File::open(path) else {
        return Err(not_found_error!("Table does not exist"));
    };
    let reader = BufReader::new(file).lines().next();
    match reader {
        Some(Ok(line)) => Ok(line
            .split(',')
            .map(std::string::ToString::to_string)
            .collect()),
        _ => Err(io_error!("Error while reading the table.")),
    }
}

fn map_string_record_to_hashmap(
    record: &csv::StringRecord,
    headers: &csv::StringRecord,
) -> HashMap<String, String> {
    record
        .iter()
        .zip(headers.iter())
        .map(|(value, header)| (header.to_string(), value.to_string()))
        .collect()
}
