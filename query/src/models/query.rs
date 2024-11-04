use std::{cmp::Ordering, collections::HashMap, path::Path};

use db::Context;

use super::{
    statement::{Cols, OrderMode, Statement},
    where_clause::WhereClause,
};

/// An array of SQL keywords used in query parsing.
pub const KEYWORDS: [&str; 15] = [
    "SELECT", "INSERT", "UPDATE", "DELETE", "FROM", "WHERE", "AND", "OR", "SET", "INTO", "ORDER",
    "BY", "ASC", "DESC", "NOT",
];

/// Represents a parsed SQL query, containing a statement and an optional WHERE clause.
#[derive(Debug)]
pub struct Query {
    statement: Statement,
    where_clause: Option<WhereClause>,
}

impl Query {
    /// Creates a new `Query` with a specified statement and optional WHERE clause.
    ///
    /// # Arguments
    ///
    /// * `statement` - The SQL statement to be executed.
    /// * `where_clause` - An optional WHERE clause for filtering the results.
    ///
    /// # Returns
    ///
    /// * A new instance of `Query`.
    pub(crate) fn new(statement: Statement, where_clause: Option<WhereClause>) -> Self {
        Query {
            statement,
            where_clause,
        }
    }

    /// Processes the query against a specified table.
    ///
    /// # Arguments
    ///
    /// * `table` - The path of the table dir to execute the query against.
    /// * `ctx` - The context of the node.
    ///
    /// # Returns
    ///
    /// * `Vec<Cols>` if the query is successfully processed and the statement is `SELECT`.
    /// * `None` if the query is successfully processed and the statement is not `SELECT`.
    ///
    /// # Errors
    ///
    /// * `Error` if an error occurs during processing.
    pub fn process(
        &mut self,
        table: &Path,
        ctx: &mut Context,
    ) -> std::io::Result<Option<Vec<Cols>>> {
        let schema = ctx.get_table_schema(table)?;
        match &self.statement {
            Statement::Select(to_print, order) => {
                let mut rows = Vec::new();
                ctx.read_table(table, &mut |row| {
                    if self.where_clause.as_ref().unwrap().eval(&row, &schema)? {
                        rows.push(row);
                    }
                    Ok(())
                })?;
                order_rows(&mut rows, order, to_print)
            }
            Statement::Insert(new_row) => ctx.append_to_table(table, new_row.clone()).map(|_| None),
            Statement::Update(new_rows) => ctx
                .update_table(table, &mut |mut row| {
                    if self.where_clause.as_ref().unwrap().eval(&row, &schema)? {
                        for (col, val) in new_rows.iter() {
                            row.insert(col.to_string(), val.to_string());
                        }
                        Ok(Some(row))
                    } else {
                        Ok(Some(row))
                    }
                })
                .map(|_| None),
            Statement::Delete => ctx
                .update_table(table, &mut |row| {
                    if self.where_clause.as_ref().unwrap().eval(&row, &schema)? {
                        Ok(None)
                    } else {
                        Ok(Some(row.clone()))
                    }
                })
                .map(|_| None),
        }
    }

    /// Returns a vector of columns that act as keys for the query.
    /// This is useful to determine the nodes that need to be queried.
    ///
    /// # Returns
    ///
    /// * `SELECT`: The columns that appear in the `WHERE` clause.
    /// * `INSERT`: The columns that appear in the `INTO` clause.
    /// * `UPDATE`: The columns that appear in the `WHERE` clause.
    /// * `DELETE`: The columns that appear in the `WHERE` clause.
    ///
    /// In all other cases, it returns `None`.
    pub fn get_keys(&self) -> Vec<String> {
        match &self.statement {
            Statement::Select(_, _) => self.where_clause.as_ref().unwrap().get_keys(),
            Statement::Insert(row) => row.keys().cloned().collect(),
            Statement::Update(_) => self.where_clause.as_ref().unwrap().get_keys(),
            Statement::Delete => self.where_clause.as_ref().unwrap().get_keys(),
        }
    }

    pub fn get_cols(&self) -> Vec<String> {
        match &self.statement {
            Statement::Select(cols, _) => cols.clone(),
            _ => Vec::new(),
        }
    }
}

fn order_rows(
    rows: &mut Vec<HashMap<String, String>>,
    order: &Option<(String, OrderMode)>,
    to_print: &[String],
) -> std::io::Result<Option<Vec<Cols>>> {
    if let Some((order_by, order_mode)) = order {
        match order_mode {
            OrderMode::Asc => rows.sort_by(|a, b| {
                let a = a.get(order_by);
                let b = b.get(order_by);
                a.partial_cmp(&b).unwrap_or(Ordering::Equal)
            }),
            OrderMode::Desc => rows.sort_by(|a, b| {
                let a = a.get(order_by);
                let b = b.get(order_by);
                b.partial_cmp(&a).unwrap_or(Ordering::Equal)
            }),
        }
    }
    Ok(Some(
        rows.iter()
            .map(|row| {
                to_print
                    .iter()
                    .map(|col| row.get(col).cloned().unwrap_or_else(|| "NULL".to_string()))
                    .collect::<Vec<String>>()
            })
            .collect::<Vec<Vec<String>>>(),
    ))
}
