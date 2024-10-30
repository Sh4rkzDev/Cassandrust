use shared::io_error;

use crate::models::{query::Query, statement::Statement, where_clause::WhereClause};

/// Processes a `DELETE` query and prepares the `Query` and table name.
///
/// This function parses the `DELETE` statement, including evaluating WHERE clauses,
/// and constructs a `Query` object.
///
/// # Arguments
///
/// * `parts` - A `Vec<String>` containing the components of the `DELETE` query.
///
/// # Returns
///
/// * `std::io::Result<(Query, String)>`: A tuple containing the parsed `Query`
///   and the name of the table
///
/// # Errors
///
/// * Returns an Error if there are syntax errors in the `DELETE` query.
pub(crate) fn process_delete(parts: &[String]) -> std::io::Result<(Query, String)> {
    if parts.len() < 6 || parts[0] != "FROM" || parts[2] != "WHERE" || parts[4] != "=" {
        return Err(io_error!(
            "DELETE query should look like this: DELETE FROM <table> WHERE <cond>"
        ));
    }
    let statement = Statement::new("DELETE")?;
    let (where_clause, _) = WhereClause::new(&parts[3..])?;
    Ok((
        Query::new(statement, Some(where_clause)),
        parts[1].to_owned(),
    ))
}
