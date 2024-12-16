use db::initialize_context;
use query::process_query;

use std::{collections::HashSet, path::Path};

const ROOT: &str = "tests/node_test/ks_test";

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
    vec![get_first_row(), get_second_row()]
}
fn is_equal(original_row: Vec<Vec<String>>, expected: Vec<Vec<String>>) {
    assert!(original_row.len() == expected.len());
    let set: Vec<HashSet<_>> = expected
        .iter()
        .map(|row| row.iter().collect::<HashSet<_>>())
        .collect();
    original_row.iter().enumerate().for_each(|(idx, row)| {
        row.iter().for_each(|col| {
            assert!(set[idx].contains(col));
        });
    });
}

#[test]
fn test_select_query_with_where_clause() {
    let (mut query, table) =
        process_query("SELECT id, name, email, age, all FROM table_test_select WHERE age > 25")
            .unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query
        .process(&Path::new(ROOT).join(table), &mut ctx)
        .unwrap()
        .unwrap();

    is_equal(output, vec![get_first_row()]);
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
        Some(vec![vec![
            "John Doe".to_string(),
            "john@example.com".to_string()
        ],])
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
            vec!["john@example.com".to_string(), "John Doe".to_string()],
            vec!["jane@example.com".to_string(), "Jane Smith".to_string()]
        ])
    );
}

#[test]
fn test_select_query_with_where_clause_using_greater_or_equal_operator() {
    let (mut query, table) =
        process_query("SELECT id, name, email, age, all FROM table_test_select WHERE age >= 20")
            .unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query
        .process(&Path::new(ROOT).join(table), &mut ctx)
        .unwrap()
        .unwrap();

    is_equal(output, get_rows());
}

#[test]
fn test_select_query_with_where_clause_using_less_or_equal_operator() {
    let (mut query, table) =
        process_query("SELECT id, name, email, age, all FROM table_test_select WHERE age <= 30")
            .unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query
        .process(&Path::new(ROOT).join(table), &mut ctx)
        .unwrap()
        .unwrap();

    is_equal(output, get_rows());
}

#[test]
fn test_select_query_with_invalid_table() {
    let (mut query, table) =
        process_query("SELECT id, name, email, age, all FROM invalid_table WHERE all = true")
            .unwrap();
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
    let (mut query, table) = process_query(
        "SELECT id, name, email, age, all FROM table_test_select WHERE invalid_column > 25",
    )
    .unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query.process(&Path::new(ROOT).join(table), &mut ctx);

    assert!(output.is_err());
}

#[test]
fn test_select_query_with_invalid_where_clause_operator() {
    assert!(process_query(
        "SELECT id, name, email, age, all FROM table_test_select WHERE age invalid_op 25"
    )
    .is_err());
}

#[test]
fn test_select_query_with_complex_where_clause() {
    let (mut query, table) = process_query(
        "SELECT id, name, email, age, all FROM table_test_select WHERE id = 1 AND name = 'John Doe' AND age >= 30",
    )
    .unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let output = query
        .process(&Path::new(ROOT).join(table), &mut ctx)
        .unwrap()
        .unwrap();

    is_equal(output, vec![get_first_row()]);
}

#[test]
fn test_select_query_without_where_clause() {
    assert!(process_query("SELECT id, name, email, age, all FROM table_test_select").is_err());
}
