use shared::io_error;

use crate::{
    models::{
        query::Query,
        statement::{OrderMode, Statement},
        where_clause::WhereClause,
    },
    utils::tokens::get_columns_from_vec,
};

/// Processes a `SELECT` query and prepares the `Query` and table path.
///
/// This function parses the `SELECT` statement, including columns,
/// WHERE and ORDER BY clauses, and constructs a `Query` object.
///
/// # Arguments
///
/// * `parts` - A `Vec<String>` containing the components of the `SELECT` query.
///
/// # Returns
///
/// * `std::io::Result<(Query, String)>`: A tuple containing the parsed `Query`
///   and the path to the table
///
/// # Errors
///
/// * Returns an Error if there are syntax errors in the `SELECT` query.
pub(crate) fn process_select(parts: &[String]) -> std::io::Result<(Query, String)> {
    let Some(from) = parts.iter().position(|s| s == "FROM") else {
        return Err(io_error!("No FROM keyword"));
    };
    if from + 1 >= parts.len() {
        return Err(io_error!("No table provided"));
    }
    if parts[0] == "FROM"
        || parts[0] == "("
        || parts[0] == ")"
        || parts[from + 1] == "("
        || parts[from + 1] == ")"
    {
        return Err(io_error!("Unexpected bracket"));
    }
    let mut statement = Statement::new("SELECT")?;

    // First I process the columns
    statement.add_cols_to_be_printed(get_columns_from_vec(&parts[..from])?)?;

    // Then I process the rest of the query
    let mut where_clause = None;
    let mut keyword = parts.get(from + 2).map(|_| from + 2);
    while let Some(idx) = keyword {
        match parts[idx].to_uppercase().as_str() {
            "WHERE" => {
                let (where_clause_opt, plus) = WhereClause::new(&parts[idx + 1..])?;
                keyword = plus.map(|plus| idx + 1 + plus);
                where_clause = Some(where_clause_opt);
            }
            "ORDER" => {
                if parts.len() < idx + 2 || parts[idx + 1].to_uppercase() != "BY" {
                    return Err(io_error!(
                        "ORDER should be followed by \"BY\" and a column."
                    ));
                }
                let order = &parts[idx + 2];
                let mode = match parts.get(idx + 3) {
                    Some(mode) => match mode.to_uppercase().as_str() {
                        "ASC" => OrderMode::Asc,
                        "DESC" => OrderMode::Desc,
                        _ => return Err(io_error!("Invalid order mode")),
                    },
                    None => OrderMode::Asc,
                };
                statement.add_order_by(order.to_owned(), mode)?;
                keyword = None;
            }
            _ => return Err(io_error!(format!("Unexpected keyword: \"{}\"", parts[idx]))),
        }
    }
    Ok((
        Query::new(statement, where_clause),
        parts[from + 1].to_owned(),
    ))
}
