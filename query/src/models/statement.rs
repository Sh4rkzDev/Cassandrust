use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use shared::io_error;

/// Represents the columns selected in a SQL query.
pub(crate) type Cols = Vec<String>;
/// Represents the optional ORDER BY clause in a SQL query.
type OrderBy = Option<(String, OrderMode)>;

/// Specifies the order mode for sorting results.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum OrderMode {
    Asc,
    Desc,
}

/// Represents a SQL statement that can be executed against a CSV table.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum Statement {
    /// Select (`cols_to_be_printed`, `order_by`)
    Select(Cols, OrderBy),
    /// Insert (new row)
    Insert(HashMap<String, String>),
    /// Update (new row)
    Update(HashMap<String, String>),
    Delete,
    //  ///Create table (columns<name, type>). Partition key is under the key "PARTITION_KEY", same for clustering key.
    // CreateTable(HashMap<String, String>),
}

impl Statement {
    /// Parses a string into a `Statement`.
    ///
    /// # Arguments
    ///
    /// * `s` - A string representing the SQL statement.
    ///
    /// # Returns
    ///
    /// * `Self` if the string is successfully parsed into a valid `Statement`.
    /// * `Error` if the string does not match any valid statement.
    pub(crate) fn new(s: &str) -> std::io::Result<Self> {
        match s {
            "SELECT" => Ok(Statement::Select(Vec::new(), None)),
            "INSERT" => Ok(Statement::Insert(HashMap::new())),
            "UPDATE" => Ok(Statement::Update(HashMap::new())),
            "DELETE" => Ok(Statement::Delete),
            _ => Err(io_error!("Invalid statement")),
        }
    }

    pub(crate) fn add_cols_to_be_printed(&mut self, cols: Cols) -> std::io::Result<()> {
        match self {
            Statement::Select(table_cols, _) => {
                *table_cols = cols;
                Ok(())
            }
            _ => Err(io_error!("Invalid use of method")),
        }
    }

    /// Adds an ORDER BY clause to the `Statement`.
    ///
    /// # Arguments
    ///
    /// * `order_by_col` - The column by which to order the results.
    /// * `mode` - The order mode (`Asc` or `Desc`).
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the ORDER BY clause is successfully added.
    /// * `Error` if the statement type is not `Select`.
    pub(crate) fn add_order_by(
        &mut self,
        order_by_col: String,
        mode: OrderMode,
    ) -> std::io::Result<()> {
        match self {
            Statement::Select(_, order_by) => {
                *order_by = Some((order_by_col, mode));
                Ok(())
            }
            _ => Err(io_error!("Invalid syntax")),
        }
    }

    pub(crate) fn add_row(&mut self, col: String, val: String) -> std::io::Result<()> {
        match self {
            Statement::Insert(row) => {
                row.insert(col, val);
                Ok(())
            }
            Statement::Update(row) => {
                row.insert(col, val);
                Ok(())
            }
            _ => Err(io_error!("Invalid use of method")),
        }
    }
}
