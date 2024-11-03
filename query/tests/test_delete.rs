use std::process::Command;

const CONTENT: &str =
    "id,name,email,age\n1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,20\n";

#[test]
fn test_delete_query_simple() {
    let table = "test_delete_query_simple_1";
    std::fs::write(table.to_owned() + ".csv", CONTENT).expect("Could not write to file");
    Command::new("target/debug/sql")
        .args(["./", format!("DELETE FROM {}", table).as_str()])
        .output()
        .expect("Failed to execute command");

    let updated = std::fs::read_to_string(table.to_owned() + ".csv");

    assert_eq!(
        updated.unwrap_or("FAILED TO READ".to_string()),
        "id,name,email,age\n"
    );
    std::fs::remove_file(table.to_owned() + ".csv").expect("Could not delete test file");
}

#[test]
fn test_delete_query_with_where() {
    let table = "test_delete_query_with_where_2";
    std::fs::write(table.to_owned() + ".csv", CONTENT).expect("Could not write to file");
    Command::new("target/debug/sql")
        .args(["./", format!("DELETE FROM {} WHERE id = 1", table).as_str()])
        .output()
        .expect("Failed to execute command");

    let updated = std::fs::read_to_string(table.to_owned() + ".csv");

    assert_eq!(
        updated.unwrap_or("FAILED TO READ".to_string()),
        "id,name,email,age\n2,Jane Smith,jane@example.com,20\n"
    );
    std::fs::remove_file(table.to_owned() + ".csv").expect("Could not delete test file");
}

#[test]
fn test_delete_query_with_multiple_where() {
    let table = "test_delete_query_with_multiple_where_3";
    std::fs::write(table.to_owned() + ".csv", CONTENT).expect("Could not write to file");
    Command::new("target/debug/sql")
        .args([
            "./",
            format!("DELETE FROM {} WHERE id = 1 AND name = 'John Doe'", table).as_str(),
        ])
        .output()
        .expect("Failed to execute command");

    let updated = std::fs::read_to_string(table.to_owned() + ".csv");

    assert_eq!(
        updated.unwrap_or("FAILED TO READ".to_string()),
        "id,name,email,age\n2,Jane Smith,jane@example.com,20\n"
    );
    std::fs::remove_file(table.to_owned() + ".csv").expect("Could not delete test file");
}
