use shared::io_error;

use crate::{models::query::Query, utils::tokens::separate_parenthesis};

use super::{
    delete::process_delete,
    insert::process_insert,
    select::process_select,
    table::{process_table_creation, process_table_deletion},
    update::process_update,
};

/// Processes a raw SQL query string and determines the query type.
///
/// This function splits the SQL query string into individual parts and identifies
/// the query type (`SELECT`, `INSERT`, `UPDATE`, `DELETE`). It then delegates the
/// parsing to the appropriate processing function.
///
/// # Arguments
///
/// * `query` - A `String` containing the raw SQL query.
///
/// # Returns
///
/// * `std::io::Result<(Query, String)>`: A tuple containing the parsed `Query`
///   and the path to the table, or an Error if the query is invalid.
///
/// # Errors
///
/// * Returns an Error if the query type is not recognized or
///   if there are errors in processing the query.
pub fn process_query(query: &str) -> std::io::Result<(Query, String)> {
    let query_vec = separate_parenthesis(
        &query
            .replace(';', "")
            .split_whitespace()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect(),
    )?;
    if query_vec.len() <= 2 {
        return Err(io_error!("Invalid syntax"));
    }
    let rest_of_query = query_vec[1..].to_vec();
    match query_vec[0].as_str() {
        "SELECT" => process_select(&rest_of_query),
        "INSERT" => process_insert(&rest_of_query),
        "UPDATE" => process_update(&rest_of_query),
        "DELETE" => process_delete(&rest_of_query),
        "CREATE" => process_table_creation(&rest_of_query),
        "DROP" => process_table_deletion(&rest_of_query),
        query => Err(io_error!(format!(
            "Invalid query: cannot recognize query '{query}'",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_query_valid_select() {
        let query_str = "SELECT id FROM clients WHERE name = 'Pepe'";
        let result = process_query(query_str);
        assert!(result.is_ok());

        let (_, path) = result.unwrap();
        assert_eq!(path, "clients");
    }

    #[test]
    fn test_process_query_valid_select_all() {
        let query_str = "SELECT * FROM clients WHERE name = 'Pepe'";
        let result = process_query(query_str);
        println!("{:?}", result);
        assert!(result.is_ok());

        let (_, path) = result.unwrap();
        assert_eq!(path, "clients");
    }

    #[test]
    fn test_process_query_valid_select_with_order() {
        let query_str = "SELECT id FROM clients WHERE name = 'Pepito' ORDER BY name DESC";
        let result = process_query(query_str);
        assert!(result.is_ok());

        let (_, path) = result.unwrap();
        assert_eq!(path, "clients");
    }

    #[test]
    fn test_process_query_valid_insert() {
        let query_str = "INSERT INTO clients (id, name) VALUES (1, 'Pepe')";
        let result = process_query(query_str);
        assert!(result.is_ok());

        let (_, path) = result.unwrap();
        assert_eq!(path, "clients");
    }

    #[test]
    fn test_process_query_valid_insert_with_spaces() {
        let query_str = "INSERT INTO clients (id, full name) VALUES ( 1, 'Sapo Pepe' )";
        let result = process_query(query_str);
        assert!(result.is_ok());

        let (_, path) = result.unwrap();
        assert_eq!(path, "clients");
    }

    #[test]
    fn test_process_query_valid_update() {
        let query_str = "UPDATE clients SET name = 'Pepe' WHERE id = 1";
        let result = process_query(query_str);
        assert!(result.is_ok());

        let (_, path) = result.unwrap();
        assert_eq!(path, "clients");
    }

    #[test]
    fn test_process_query_valid_update_with_spaces() {
        let query_str = "UPDATE clients SET full name = 'Sapo Pepe' WHERE id = 1";
        let result = process_query(query_str);
        assert!(result.is_ok());

        let (_, path) = result.unwrap();
        assert_eq!(path, "clients");
    }

    #[test]
    fn test_process_query_valid_delete() {
        let query_str = "DELETE FROM clients WHERE id = 1";
        let result = process_query(query_str);
        assert!(result.is_ok());

        let (_, path) = result.unwrap();
        assert_eq!(path, "clients");
    }

    #[test]
    fn test_process_query_invalid() {
        let result = process_query("INVALID QUERY");
        assert!(result.is_err());
    }

    #[test]
    fn test_process_query_invalid_select_missing_from() {
        let query_str = "SELECT id WHERE name = 'Pepe'";
        let result = process_query(query_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_query_invalid_select_missing_where() {
        let query_str = "SELECT id FROM clients ORDER name = 'Pepe'";
        let result = process_query(query_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_query_invalid_select_with_parentesis() {
        let query_str = "SELECT (id) FROM clients";
        let result = process_query(query_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_query_invalid_insert_missing_into() {
        let query_str = "INSERT clients (id, name) VALUES (1, 'Pepe')";
        let result = process_query(query_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_query_invalid_insert_missing_values() {
        let query_str = "INSERT INTO clients (id, name) (1, 'Pepe')";
        let result = process_query(query_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_query_invalid_insert_missing_parenthesis() {
        let query_str = "INSERT INTO clients id, name VALUES 1, 'Pepe'";
        let result = process_query(query_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_query_invalid_update_missing_set() {
        let query_str = "UPDATE clients name = 'Pepe' WHERE id = 1";
        let result = process_query(query_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_query_invalid_update_syntax() {
        let query_str = "UPDATE clients SET name 'Pepe' WHERE id = 1";
        let result = process_query(query_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_query_invalid_delete_missing_from() {
        let query_str = "DELETE clients WHERE id = 1";
        let result = process_query(query_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_query_invalid_delete_missing_where() {
        let query_str = "DELETE FROM clients id = 1";
        let result = process_query(query_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_create_table() {
        let query_str = "CREATE TABLE clients (id int, name text, PRIMARY KEY (id))";
        let result = process_query(query_str);
        assert!(result.is_ok());

        let (_, path) = result.unwrap();
        assert_eq!(path, "clients");
    }

    #[test]
    fn test_process_create_table_invalid() {
        let query_str = "CREATE TABLE clients (id int, name text, PRIMARY KEY id)";
        let result = process_query(query_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_create_table_invalid_syntax() {
        let query_str = "CREATE TABLE clients id int, name text, PRIMARY KEY (id)";
        let result = process_query(query_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_create_table_complete() {
        let query_str = "CREATE TABLE clients (id int, name text, age int, date timestamp, PRIMARY KEY (id, name))";
        let result = process_query(query_str);
        assert!(result.is_ok());

        let (_, path) = result.unwrap();
        assert_eq!(path, "clients");
    }
}
