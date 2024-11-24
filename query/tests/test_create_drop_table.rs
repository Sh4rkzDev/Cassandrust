use std::{collections::HashSet, path::Path};

use db::initialize_context;
use query::process_query;

const ROOT: &str = "tests/node_test/ks_test";

#[test]
fn test_create_drop_table() {
    let mut ctx = initialize_context(Path::new("tests/node_test")).unwrap();
    let (mut query, table_str) = process_query(
        "CREATE TABLE table_test_create (id int, name text, email text, age int, PRIMARY KEY (id))",
    )
    .unwrap();
    let table = Path::new(ROOT).join(table_str);
    let output = query.process(&table, &mut ctx).unwrap();
    assert!(output.is_none());

    let updated = std::fs::read_to_string(table.join("table.csv"))
        .unwrap()
        .split(',')
        .map(|s| s.trim().to_string())
        .collect::<HashSet<String>>();
    assert_eq!(
        updated,
        HashSet::from([
            "id".to_string(),
            "name".to_string(),
            "email".to_string(),
            "age".to_string()
        ])
    );

    (query, _) = process_query("DROP TABLE table_test_create").unwrap();
    let output = query.process(&table, &mut ctx).unwrap();
    assert!(output.is_none());
    assert!(!table.exists());
}
