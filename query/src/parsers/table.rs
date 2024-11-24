use std::collections::HashMap;

use db::{PrimaryKey, Schema, SchemaType};
use shared::io_error;

use crate::{models::statement::Statement, Query};

/// Process a table creation query.
///
/// # Arguments
///
/// * `tokens` - A slice of strings containing the tokens of the query.
///
/// # Returns
///
/// * A `Query` object
/// * A `String` containing the table name
///
/// # Errors
///
/// * Returns an error if the number of arguments is invalid or if there is a syntax error.
pub(crate) fn process_table_creation(tokens: &[String]) -> std::io::Result<(Query, String)> {
    let primary = tokens.iter().position(|s| s == "PRIMARY");
    if tokens.len() < 11
    // 11 is the minimum number of tokens for a valid CREATE TABLE query in CQL
        || tokens[0] != "TABLE"
        || tokens[2] != "("
        || primary.is_none()
        || (primary.unwrap() + 1) % 2 != 0
        || tokens[primary.unwrap() + 1] != "KEY"
        || tokens[primary.unwrap() + 2] != "("
    {
        return Err(io_error!(
            "CREATE TABLE query should look like: CREATE TABLE <table> (<col> <type>, ..., PRIMARY KEY (<col>, ...))"
        ));
    }

    let mut columns = HashMap::new();

    let mut i = 3;
    while tokens[i] != "PRIMARY" {
        if tokens[i + 1] == "PRIMARY" {
            return Err(io_error!("Invalid column definition type"));
        }
        columns.insert(
            tokens[i].to_owned(),
            SchemaType::new(&tokens[i + 1].trim_end_matches(','))?,
        );
        i += 2;
    }

    let partition_key = vec![tokens[i + 3].trim_end_matches(',').to_owned()];
    let clustering_key = tokens[i + 4..tokens.len() - 2]
        .iter()
        .map(|s| s.trim_end_matches(',').to_owned())
        .collect::<Vec<String>>();

    let primary_key = PrimaryKey::new(partition_key, clustering_key);
    let statement = Statement::CreateTable(Schema::new(columns, primary_key));

    Ok((Query::new(statement, None), tokens[1].to_owned()))
}

pub(crate) fn process_table_deletion(tokens: &[String]) -> std::io::Result<(Query, String)> {
    if tokens.len() != 2 || tokens[0] != "TABLE" {
        return Err(io_error!(
            "DROP TABLE query should look like: DROP TABLE <table>"
        ));
    }

    let statement = Statement::DropTable;
    Ok((Query::new(statement, None), tokens[1].to_owned()))
}
