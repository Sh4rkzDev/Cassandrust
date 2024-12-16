use std::path::Path;

use db::initialize_context;
use query::process_query;
use rand::Rng;

const ROOT: &str = "tests/node_test/ks_test";

// Do all tests in one function to avoid parallelism issues
#[test]
fn test_update_query() {
    // ! Test 1
    let mut john_age = rand::thread_rng().gen_range(1..100);
    let (mut query, mut table_str) = process_query(&format!(
        "UPDATE table_test_update SET age = {john_age} WHERE id = 1"
    ))
    .unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();

    let mut table = Path::new(ROOT).join(table_str);
    let output = query.process(&table, &mut ctx).unwrap();
    assert!(output.is_none());

    let mut updated = std::fs::read_to_string(table.join("table.csv")).unwrap();

    assert_eq!(
        updated,
        format!("id,name,email,age\n1,John Doe,john@example.com,{john_age}\n2,Jane Smith,jane@example.com,20\n")
    );

    // ! Test 2

    let jane_age = rand::thread_rng().gen_range(1..100);
    (query, table_str) =
        process_query(&format!("UPDATE table_test_update SET age = {jane_age} WHERE id = 2 AND name = 'Jane Smith' AND age = 20"))
            .unwrap();
    table = Path::new(ROOT).join(table_str);
    let output = query.process(&table, &mut ctx).unwrap();
    assert!(output.is_none());

    updated = std::fs::read_to_string(table.join("table.csv")).unwrap();

    assert_eq!(
        updated,
        format!("id,name,email,age\n1,John Doe,john@example.com,{john_age}\n2,Jane Smith,jane@example.com,{jane_age}\n")
    );

    // ! Test 3

    assert!(process_query("UPDATE table_test_update SET age = 45 WHERE id 1").is_err());

    // ! Test 4

    assert!(process_query("UPDATE table_test_update SET age 25 WHERE id = 1").is_err());

    // ! Test 5

    (query, table_str) = process_query("UPDATE invalid_table SET age = 25 WHERE id = 1").unwrap();
    table = Path::new(ROOT).join(table_str);

    assert!(query.process(&table, &mut ctx).is_err());

    // ! Test 6

    john_age = rand::thread_rng().gen_range(1..100);
    (query, table_str) = process_query(&format!("UPDATE table_test_update SET age = {john_age}, name = 'Doe John' WHERE id = 1 AND name = 'John Doe'")).unwrap();

    table = Path::new(ROOT).join(table_str);
    let output = query.process(&table, &mut ctx).unwrap();
    assert!(output.is_none());

    updated = std::fs::read_to_string(table.join("table.csv")).unwrap();

    assert_eq!(
        updated,
        format!("id,name,email,age\n1,Doe John,john@example.com,{john_age}\n2,Jane Smith,jane@example.com,{jane_age}\n")
    );

    // ! Set back to original
    (query, table_str) =
        process_query("UPDATE table_test_update SET age = 30, name = 'John Doe' WHERE id = 1")
            .unwrap();
    table = Path::new(ROOT).join(table_str);
    query.process(&table, &mut ctx).unwrap();
    (query, table_str) =
        process_query("UPDATE table_test_update SET age = 20, name = 'Jane Smith' WHERE id = 2")
            .unwrap();
    table = Path::new(ROOT).join(table_str);
    query.process(&table, &mut ctx).unwrap();
}
