use std::path::Path;

use db::initialize_context;
use query::process_query;

const ROOT: &str = "tests/node_test/ks_test";

// Do all tests in one function to avoid parallelism issues
#[test]
fn test_insert_and_delete_query() {
    // ! Test 1 - Insert
    let (mut query, table_str) = process_query(
        "INSERT INTO table_test_insert (id, name, email, age) VALUES (3, 'John Snow', 'snow@got.com', 40)"
    ).unwrap();
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();
    let table = Path::new(ROOT).join(table_str);
    let output = query.process(&table, &mut ctx).unwrap();
    assert!(output.is_none());

    let mut updated = std::fs::read_to_string(table.join("table.csv")).unwrap();
    assert_eq!(
        updated,
    "id,name,email,age\n1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,20\n3,John Snow,snow@got.com,40\n"
    );

    // ! Test 2 - Insert

    (query, _) = process_query(
        "INSERT INTO table_test_insert (id, name, age) VALUES (4, 'Fantastic Four', 40)",
    )
    .unwrap();

    let output = query.process(&table, &mut ctx).unwrap();
    assert!(output.is_none());

    updated = std::fs::read_to_string(table.join("table.csv")).unwrap();
    assert_eq!(
        updated,
    "id,name,email,age\n1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,20\n3,John Snow,snow@got.com,40\n4,Fantastic Four,NULL,40\n"
    );

    // ! Test 3 - Insert
    (query, _) =
        process_query("INSERT INTO table_test_insert (id, name) VALUES (5, 'Hi Five')").unwrap();

    let output = query.process(&table, &mut ctx).unwrap();
    assert!(output.is_none());

    updated = std::fs::read_to_string(table.join("table.csv")).unwrap();
    assert_eq!(
        updated,
    "id,name,email,age\n1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,20\n3,John Snow,snow@got.com,40\n4,Fantastic Four,NULL,40\n5,Hi Five,NULL,NULL\n"
    );

    // ! Test 4 - Delete
    (query, _) = process_query("DELETE FROM table_test_insert WHERE id = 4").unwrap();
    let output = query.process(&table, &mut ctx).unwrap();
    assert!(output.is_none());

    updated = std::fs::read_to_string(table.join("table.csv")).unwrap();
    assert_eq!(
        updated,
    "id,name,email,age\n1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,20\n3,John Snow,snow@got.com,40\n5,Hi Five,NULL,NULL\n"
    );

    // ! Test 5 - Delete
    (query, _) = process_query("DELETE FROM table_test_insert WHERE name = 'Hi Five'").unwrap();
    let output = query.process(&table, &mut ctx).unwrap();
    assert!(output.is_none());

    updated = std::fs::read_to_string(table.join("table.csv")).unwrap();
    assert_eq!(updated, 
    "id,name,email,age\n1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,20\n3,John Snow,snow@got.com,40\n"
    );

    // ! Test 6 - Delete
    (query, _) = process_query("DELETE FROM table_test_insert WHERE email = 'snow@got.com' AND age = 40").unwrap();
    let output = query.process(&table, &mut ctx).unwrap();
    assert!(output.is_none());

    updated = std::fs::read_to_string(table.join("table.csv")).unwrap();
    assert_eq!(updated, 
    "id,name,email,age\n1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,20\n"
    );

}
