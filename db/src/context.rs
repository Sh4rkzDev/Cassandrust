use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use shared::{io_error, not_found_error};

use crate::{
    models::{
        keyspace::{create_keyspace, drop_keyspace, get_keyspace_options},
        schema::Schema,
        tables::Tables,
    },
    Options,
};

/// Represents the node's context.
/// The context contains all the keyspaces and tables in the node and is used to create, drop, and read keyspaces and tables.
/// The context is also used to store the schema of the tables.
#[derive(Debug)]
pub struct Context {
    ctx: HashMap<String, Tables>,
    pub node_dir: PathBuf,
}

/// Initializes the context with the keyspaces and tables in the node on startup.
///
/// # Arguments
///
/// * `node` - A reference to the path of the node's directory.
///
/// # Returns
///
/// * Returns a `Context` with the keyspaces and tables in the node.
///
/// # Errors
///
/// * Returns an `Error` if there is an issue while reading the keyspaces and tables.
pub fn initialize_context(node: &Path) -> std::io::Result<Context> {
    let mut ctx = HashMap::new();
    for entry in std::fs::read_dir(node)? {
        let keyspace_path = entry?.path();
        if keyspace_path.is_dir() {
            let keyspace =
                get_file_name(&keyspace_path, "Invalid path for node's dir".to_string())?;
            let tables = Tables::get_tables_schema(&keyspace_path)?;
            ctx.insert(keyspace, tables);
        }
    }
    Ok(Context {
        ctx,
        node_dir: node.to_path_buf(),
    })
}

impl Context {
    pub fn is_a_keyspace(&self, keyspace: &str) -> bool {
        self.ctx.contains_key(keyspace)
    }

    /// Creates a new keyspace in the node with the specified options.
    ///
    /// # Arguments
    ///
    /// * `keyspace` - The name of the keyspace.
    /// * `options` - The options of the keyspace.
    pub fn create_keyspace(&mut self, keyspace: &Path, options: &Options) -> std::io::Result<()> {
        create_keyspace(keyspace, options)?;
        let keyspace_name = get_file_name(keyspace, "Invalid keyspace path".to_string())?;
        self.ctx.insert(keyspace_name, Tables::new());
        Ok(())
    }

    /// Creates a new table within the keyspace that is currently set in the table context.
    ///
    /// # Arguments
    ///
    /// * `table` - The path of the new table directory.
    /// * `schema` - The schema of the table.
    pub fn create_table(&mut self, table: &Path, schema: &Schema) -> std::io::Result<()> {
        let keyspace = get_file_name(
            table.parent().ok_or(io_error!("Invalid table path"))?,
            "Invalid keyspace path".to_string(),
        )?;
        self.ctx
            .get_mut(&keyspace)
            .ok_or(not_found_error!("Keyspace does not exist"))?
            .create_table(table, schema.clone())
    }

    /// Drops the keyspace from the node.
    /// This will remove the keyspace and all the tables in the keyspace.
    ///
    /// # Arguments
    ///
    /// * `keyspace` - The path of the keyspace to be removed.
    pub fn drop_keyspace(&mut self, keyspace: &Path) -> std::io::Result<()> {
        drop_keyspace(keyspace)?;
        if let Some(keyspace_name) = keyspace.file_name().and_then(|name| name.to_str()) {
            self.ctx.remove(keyspace_name);
        }
        Ok(())
    }

    /// Drops the table from the keyspace that is currently set in the connection context.
    /// This will remove the table and all the data in the table.
    ///
    /// # Arguments
    ///
    /// * `table` - The path of the table to be removed.
    pub fn drop_table(&mut self, table: &Path) -> std::io::Result<()> {
        let keyspace = get_file_name(
            table.parent().ok_or(io_error!("Invalid table path"))?,
            "Invalid keyspace path".to_string(),
        )?;
        self.ctx
            .get_mut(&keyspace)
            .ok_or(not_found_error!("Keyspace does not exist"))?
            .drop_table(table)
    }

    pub fn get_keyspace_options(&self, keyspace: &Path) -> std::io::Result<Options> {
        get_keyspace_options(keyspace)
    }

    /// Returns the schema of the table from the keyspace that is set.
    ///
    /// # Arguments
    ///
    /// * `table` - The name of the table.
    pub fn get_table_schema(&self, keyspace: &str, table: &str) -> std::io::Result<Schema> {
        self.ctx
            .get(keyspace)
            .ok_or(not_found_error!("Keyspace does not exist"))?
            .get_table_schema(table)
    }

    /// Reads the table from the keyspace that is currently set in the connection context.
    ///
    /// # Arguments
    ///
    /// * `table` - The name of the table.
    /// * `key` - Indicates the `PARTITION KEY` of the table and therefore the path to the file.
    /// * `visitor` - A function that takes a reference to a `HashMap` of the data in the table.
    pub fn read_table(
        &self,
        table: &Path,
        visitor: &mut dyn FnMut(HashMap<String, String>) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        let keyspace = get_file_name(
            table.parent().ok_or(io_error!("Invalid table path"))?,
            "Invalid keyspace path".to_string(),
        )?;
        self.ctx
            .get(&keyspace)
            .ok_or(not_found_error!("Keyspace does not exist"))?
            .read_table(table, visitor)
    }

    /// Appends the data to the table from the keyspace that is currently set in the connection context.
    ///
    /// # Arguments
    ///
    /// * `table` - The path of the table dir.
    /// * `data` - A `HashMap` of the data to be appended to the table.
    pub fn append_to_table(
        &mut self,
        table: &Path,
        data: HashMap<String, String>,
    ) -> std::io::Result<()> {
        let keyspace = get_file_name(
            table.parent().ok_or(io_error!("Invalid table path"))?,
            "Invalid keyspace path".to_string(),
        )?;
        self.ctx
            .get_mut(&keyspace)
            .ok_or(not_found_error!("Keyspace does not exist"))?
            .append_to_table(table, &data)
    }

    /// Updates the table from the keyspace that is currently set in the connection context.
    ///
    /// # Arguments
    ///
    /// * `table` - The path of the table.
    /// * `visitor` - A function that takes a reference to a `HashMap` of the data in the table and returns an `Option<HashMap<String, String>>`.
    ///
    /// The function should return `Some` with the updated data if the data should be updated, otherwise `None`.
    /// In case of None, the data will not be present in the table (deleted).  
    /// In case of some column that is not present in the hashmap, the column will not be updated.
    pub fn update_table(
        &mut self,
        table: &Path,
        visitor: &mut dyn FnMut(
            HashMap<String, String>,
        ) -> std::io::Result<Option<HashMap<String, String>>>,
    ) -> std::io::Result<()> {
        let keyspace = get_file_name(
            table.parent().ok_or(io_error!("Invalid table path"))?,
            "Invalid keyspace path".to_string(),
        )?;
        self.ctx
            .get_mut(&keyspace)
            .ok_or(not_found_error!("Keyspace does not exist"))?
            .update_table(table, visitor)
    }
}

pub(crate) fn get_file_name(path: &Path, msg: String) -> std::io::Result<String> {
    path.file_name().ok_or(io_error!(msg)).and_then(|name| {
        name.to_str()
            .ok_or(io_error!("Invalid name"))
            .map(|s| s.to_string())
    })
}
