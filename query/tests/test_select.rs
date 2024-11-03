use db::initialize_context;
use query::process_query;

use std::path::Path;

const ROOT: &str = "tests/node_test/ks_test";

fn get_headers() -> Vec<String> {
    vec![
        "id".to_string(),
        "name".to_string(),
        "email".to_string(),
        "age".to_string(),
        "all".to_string(),
    ]
}
fn get_first_row() -> Vec<String> {
    vec![
        "1".to_string(),
        "John Doe".to_string(),
        "john@example.com".to_string(),
        "30".to_string(),
        "true".to_string(),
    ]
}
fn get_second_row() -> Vec<String> {
    vec![
        "2".to_string(),
        "Jane Smith".to_string(),
        "jane@example.com".to_string(),
        "20".to_string(),
        "true".to_string(),
    ]
}
fn get_rows() -> Vec<Vec<String>> {
    vec![get_headers(), get_first_row(), get_second_row()]
}

#[test]
fn test_select_query_with_where_clause() {
    let (mut query, table) =
        process_query("SELECT * FROM table_test_select WHERE age > 25").unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query
        .process(&Path::new(ROOT).join(table), &mut ctx)
        .unwrap();

    assert_eq!(output, Some(vec![get_headers(), get_first_row()]));
}

#[test]
fn test_select_query_with_where_clause_and_columns() {
    let (mut query, table) =
        process_query("SELECT name, email FROM table_test_select WHERE age > 25").unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query
        .process(&Path::new(ROOT).join(table), &mut ctx)
        .unwrap();

    assert_eq!(
        output,
        Some(vec![
            vec!["name".to_string(), "email".to_string()],
            vec!["John Doe".to_string(), "john@example.com".to_string()],
        ])
    );
}

#[test]
fn test_select_query_with_columns_in_different_order() {
    let (mut query, table) =
        process_query("SELECT email, name FROM table_test_select WHERE all = true").unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query
        .process(&Path::new(ROOT).join(table), &mut ctx)
        .unwrap();

    assert_eq!(
        output,
        Some(vec![
            vec!["email".to_string(), "name".to_string()],
            vec!["john@example.com".to_string(), "John Doe".to_string()],
            vec!["jane@example.com".to_string(), "Jane Smith".to_string()]
        ])
    );
}

#[test]
fn test_select_query_with_where_clause_using_greater_or_equal_operator() {
    let (mut query, table) =
        process_query("SELECT * FROM table_test_select WHERE age >= 20").unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query
        .process(&Path::new(ROOT).join(table), &mut ctx)
        .unwrap();

    assert_eq!(output, Some(get_rows()));
}

#[test]
fn test_select_query_with_where_clause_using_less_or_equal_operator() {
    let (mut query, table) =
        process_query("SELECT * FROM table_test_select WHERE age <= 30").unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query
        .process(&Path::new(ROOT).join(table), &mut ctx)
        .unwrap();

    assert_eq!(output, Some(get_rows()));
}

#[test]
fn test_select_query_with_invalid_table() {
    let (mut query, table) = process_query("SELECT * FROM invalid_table WHERE all = true").unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query.process(&Path::new(ROOT).join(table), &mut ctx);

    assert!(output.is_err());
}

#[test]
fn test_select_query_with_invalid_column() {
    let (mut query, table) =
        process_query("SELECT invalid_col FROM table_test_select WHERE age = 20").unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query.process(&Path::new(ROOT).join(table), &mut ctx);

    assert!(output.is_err());
}

#[test]
fn test_select_query_with_invalid_where_clause() {
    let (mut query, table) =
        process_query("SELECT * FROM table_test_select WHERE invalid_column > 25").unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query.process(&Path::new(ROOT).join(table), &mut ctx);

    assert!(output.is_err());
}

#[test]
fn test_select_query_with_invalid_where_clause_operator() {
    assert!(process_query("SELECT * FROM table_test_select WHERE age invalid_op 25").is_err());
}

#[test]
fn test_select_query_with_complex_where_clause() {
    let (mut query, table) = process_query(
        "SELECT * FROM table_test_select WHERE id = 1 AND name = 'John Doe' AND age >= 30",
    )
    .unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query
        .process(&Path::new(ROOT).join(table), &mut ctx)
        .unwrap();

    assert_eq!(output, Some(vec![get_headers(), get_first_row()]));
}

#[test]
fn test_select_query_without_where_clause() {
    assert!(process_query("SELECT * FROM table_test_select").is_err());
}
