use std::{
    collections::HashMap,
    fs::{create_dir_all, read_dir, remove_dir_all, rename, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::RwLock,
};

use shared::{io_error, not_found_error};

use crate::context::get_file_name;

use super::schema::Schema;

/// Represents the tables in a keyspace.
/// The tables contain their schema and are this is used to create, drop, and read them.
#[derive(Debug)]
pub(crate) struct Tables {
    tables: RwLock<HashMap<String, RwLock<Schema>>>,
}

impl Tables {
    pub(crate) fn new() -> Self {
        Tables {
            tables: RwLock::new(HashMap::new()),
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
    ///
    /// # Errors
    ///
    /// * Returns an error if the keyspace does not exist or if there is an error reading the schema of the tables.
    pub(crate) fn get_tables_schema(keyspace: &Path) -> std::io::Result<Self> {
        let mut tables: HashMap<String, RwLock<Schema>> = HashMap::new();
        for entry in read_dir(keyspace)? {
            let table = entry?;
            if !table.file_type()?.is_dir() {
                continue;
            }
            let mut schema_file = File::open(table.path().join("table.schema"))?;

            tables.insert(
                get_file_name(&table.path(), "Invalid table name".to_string())?,
                RwLock::new(Schema::read(&mut schema_file)?),
            );
        }
        Ok(Tables {
            tables: RwLock::new(tables),
        })
    }

    pub(crate) fn create_table(&mut self, table: &Path, schema: Schema) -> std::io::Result<()> {
        let table_str = get_file_name(table, "Invalid table name".to_string())?;
        let read_guard = self.tables.read().unwrap();
        if read_guard.contains_key(&table_str) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "Table already exists",
            ));
        }
        drop(read_guard);
        create_dir_all(table)?;

        let mut schema_file = File::create(table.join("table.schema"))?;
        schema.write(&mut schema_file)?;

        let mut table_file = File::create(table.join("table.csv"))?;
        table_file.write_fmt(format_args!("{}\n", schema.get_columns().join(",")))?;

        self.tables
            .write()
            .unwrap()
            .insert(table_str, RwLock::new(schema));
        Ok(())
    }

    pub(crate) fn drop_table(&mut self, table: &Path) -> std::io::Result<()> {
        let table_str = get_file_name(table, "Invalid table name".to_string())?;
        let binding = self.tables.read().unwrap();
        let write_guard = binding
            .get(&table_str)
            .ok_or(not_found_error!("Table does not exist"))?
            .write()
            .unwrap();
        remove_dir_all(table)?;
        drop(write_guard);
        drop(binding);

        self.tables
            .write()
            .unwrap()
            .remove(&table_str)
            .map(|_| ())
            .ok_or(not_found_error!("Table does not exist"))
    }

    pub(crate) fn get_table_schema(&self, table: &str) -> std::io::Result<Schema> {
        self.tables
            .read()
            .unwrap()
            .get(table)
            .map(|schema| schema.read().unwrap().clone())
            .ok_or(not_found_error!("Table does not exist"))
    }

    pub(crate) fn read_table(
        &self,
        table: &Path,
        visitor: &mut dyn FnMut(HashMap<String, String>) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        let table_str = get_file_name(table, "Invalid table name".to_string())?;
        let read_guard = self.tables.read().unwrap();
        let __read_guard = read_guard
            .get(&table_str)
            .ok_or(not_found_error!("Table not found"))?
            .read()
            .unwrap();
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(table.join("table.csv"))?;

        let headers = reader.headers()?.clone();

        for result in reader.records() {
            let record = result?;
            let map = map_string_record_to_hashmap(&record, &headers);
            visitor(map)?;
        }

        Ok(())
    }

    pub(crate) fn append_to_table(
        &self,
        table: &Path,
        data: &HashMap<String, String>,
    ) -> std::io::Result<()> {
        let table_file = &table.join("table.csv");
        ensure_newline_at_end(table_file)?;
        let table_str = get_file_name(table, "Invalid table name".to_string())?;
        let binding = self.tables.read().unwrap();
        let read_guard = binding
            .get(&table_str)
            .ok_or(not_found_error!("Table not found"))?;
        let table_read_guard = read_guard.read().unwrap();
        let cols = get_columns(table_file)?;

        let mut row = vec![String::new(); cols.len()];
        for (idx, col) in cols.iter().enumerate() {
            let value = data
                .get(col)
                .map_or("NULL".to_string(), ToString::to_string);
            if value != "NULL" {
                table_read_guard.check_type(col, &value)?;
            }
            row[idx] = value;
        }
        drop(table_read_guard);

        let _table_write_guard = read_guard.write().unwrap();
        let mut file = OpenOptions::new().append(true).open(table_file)?;
        writeln!(file, "{}", row.join(","))
    }

    pub(crate) fn update_table(
        &self,
        table: &Path,
        visitor: &mut dyn FnMut(
            HashMap<String, String>,
        ) -> std::io::Result<Option<HashMap<String, String>>>,
    ) -> std::io::Result<()> {
        let output_file = table.join("table.tmp");
        let table_str = get_file_name(table, "Invalid table name".to_string())?;
        let table_file = table.join("table.csv");
        let binding = self.tables.read().unwrap();
        let read_guard = binding
            .get(&table_str)
            .ok_or(not_found_error!("Table not found"))?;
        let table_read_guard = read_guard.read().unwrap();
        let cols = get_columns(&table_file)?;

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(&table_file)?;
        let headers = reader.headers()?.clone();
        let mut writer = csv::Writer::from_path(&output_file)?;

        writer.write_record(&cols)?;

        for result in reader.records() {
            let record = result?;
            let map = map_string_record_to_hashmap(&record, &headers);
            let Some(updated_row) = visitor(map.clone())? else {
                continue;
            };
            let mut row = vec![String::new(); cols.len()];
            for (idx, col) in cols.iter().enumerate() {
                let value = updated_row
                    .get(col)
                    .map(ToString::to_string)
                    .unwrap_or(map.get(col).map_or("NULL".to_string(), ToString::to_string));
                if value != "NULL" {
                    table_read_guard.check_type(col, &value)?;
                }
                row[idx] = value;
            }
            writer.write_record(&row)?;
        }
        drop(table_read_guard);
        let _table_write_guard = read_guard.write().unwrap();
        rename(output_file, table_file)
    }
}

fn ensure_newline_at_end(path: &Path) -> std::io::Result<()> {
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

fn get_columns(path: &Path) -> std::io::Result<Vec<String>> {
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
