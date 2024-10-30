use shared::io_error;

use crate::{
    models::{query::Query, statement::Statement},
    utils::tokens::get_columns_from_vec,
};

/// Processes an `INSERT` query and prepares the `Query` and table name.
///
/// This function parses the `INSERT` statement, including columns and values,
/// and constructs a `Query` object.
///
/// # Arguments
///
/// * `parts` - A `Vec<String>` containing the components of the `INSERT` query.
///
/// # Returns
///
/// * `std::io::Result<(Query, String)>`: A tuple containing the parsed `Query`
///   and the name of the table
///
/// # Errors
///
/// * Returns an Error if there are syntax errors in the `INSERT` query.
pub(crate) fn process_insert(tokens: &[String]) -> std::io::Result<(Query, String)> {
    let Some(values) = tokens.iter().position(|s| s == "VALUES") else {
        return Err(io_error!("No VALUES keyword"));
    };
    if tokens.len() < 9
        || tokens[0] != "INTO"
        || tokens[2] != "("
        || tokens[values + 1] != "("
        || tokens[values - 1] != ")"
        || tokens.last() != Some(&")".to_string())
    {
        return Err(io_error!(
            "INSERT query should follow this pattern: INSERT INTO <table> (col) VALUES (value)"
        ));
    }
    let mut statement = Statement::new("INSERT")?;
    let cols = get_columns_from_vec(&tokens[3..values - 1])?;
    let new_values = get_columns_from_vec(&tokens[values + 2..tokens.len() - 1])?;
    for i in 0..cols.len() {
        statement.add_row(cols[i].to_owned(), new_values[i].to_owned())?;
    }
    Ok((Query::new(statement, None), tokens[1].to_owned()))
}
