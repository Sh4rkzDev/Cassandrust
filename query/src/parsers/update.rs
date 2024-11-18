use shared::io_error;

use crate::models::{query::Query, statement::Statement, where_clause::WhereClause};

/// Processes an `UPDATE` query and prepares the `Query` and table path.
///
/// This function parses the `UPDATE` statement, including setting columns
/// and evaluating WHERE clauses, and constructs a `Query` object.
///
/// # Arguments
///
/// * `parts` - A `Vec<String>` containing the components of the `UPDATE` query.
///
/// # Returns
///
/// * `std::io::Result<(Query, String)>`: A tuple containing the parsed `Query`
///   and the table name.
///
/// # Errors
///
/// * Returns an Error if there are syntax errors in the `UPDATE` query.
///
pub(crate) fn process_update(tokens: &[String]) -> std::io::Result<(Query, String)> {
    if tokens.len() < 9 || tokens[1] != "SET" || !tokens.contains(&"WHERE".to_string()) {
        // 9 is the minimum number of tokens for a valid UPDATE query in CQL
        return Err(io_error!(
            "UPDATE query should look like: UPDATE <table> SET <col> = <val> WHERE <condition>"
        ));
    }
    let mut statement = Statement::new("UPDATE")?;
    let mut where_clause = None;
    let mut col = String::new();
    let mut new_val = String::new();
    let mut equals = false;
    for (idx, token) in tokens[2..].iter().enumerate() {
        let token = token.replace('\'', "");
        match token.as_str() {
            "=" => {
                if col.is_empty() {
                    return Err(io_error!(
                        "You must choose a column to set its new value first"
                    ));
                }
                equals = true;
            }
            "WHERE" => {
                if !equals || new_val.is_empty() {
                    return Err(io_error!(
                        "Unexpected WHERE clause before declaring new value."
                    ));
                }
                let (where_clause_opt, keyword) = WhereClause::new(&tokens[idx + 3..])?;
                if keyword.is_some() {
                    return Err(io_error!("WHERE should be the last clause"));
                }
                where_clause = Some(where_clause_opt);
                break;
            }
            _ => {
                if let Some(stripped) = token.strip_suffix(",") {
                    if !equals {
                        return Err(io_error!(format!(
                            "Unexpected comma in column identifier: {token}"
                        )));
                    }
                    new_val += &(" ".to_string() + stripped);
                    statement.add_row(col.trim().to_owned(), new_val.trim().to_string())?;
                    col = String::new();
                    new_val = String::new();
                    equals = false;
                } else if equals {
                    new_val += &(" ".to_string() + &token);
                } else {
                    col += &(" ".to_string() + &token);
                }
            }
        }
    }
    if where_clause.is_none() {
        return Err(io_error!("WHERE clause is missing"));
    }
    statement.add_row(col.trim().to_owned(), new_val.trim().to_string())?;
    Ok((Query::new(statement, where_clause), tokens[0].to_owned()))
}
