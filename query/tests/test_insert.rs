use std::process::Command;

const CONTENT: &str =
    "id,name,email,age\n1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,20\n";

#[test]
fn test_insert_query_simple() {
    let table = "test_insert_query_simple_1";
    std::fs::write(table.to_owned() + ".csv", CONTENT).expect("Could not write to file");
    Command::new("target/debug/sql")
        .args([
            "./",
            format!(
                "INSERT INTO {} (id, name, email, age) VALUES (3, 'John Snow', 'snow@got.com', 40)",
                table
            )
            .as_str(),
        ])
        .output()
        .expect("Failed to execute command");

    let updated = std::fs::read_to_string(table.to_owned() + ".csv");
    assert_eq!(
        updated.unwrap_or("FAILED TO READ".to_string()),
    "id,name,email,age\n1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,20\n3,John Snow,snow@got.com,40\n"
    );
    std::fs::remove_file(table.to_owned() + ".csv").expect("Could not delete test file");
}

#[test]
fn test_insert_with_null() {
    let table = "test_insert_with_null_2";
    std::fs::write(table.to_owned() + ".csv", CONTENT).expect("Could not write to file");
    Command::new("target/debug/sql")
        .args([
            "./",
            format!(
                "INSERT INTO {} (id, name, age) VALUES (3, 'John Snow', 40)",
                table
            )
            .as_str(),
        ])
        .output()
        .expect("Failed to execute command");

    let updated = std::fs::read_to_string(table.to_owned() + ".csv");
    assert_eq!(
        updated.unwrap_or("FAILED TO READ".to_string()),
    "id,name,email,age\n1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,20\n3,John Snow,NULL,40\n");
    std::fs::remove_file(table.to_owned() + ".csv").expect("Could not delete test file");
}

#[test]
fn test_insert_with_multiple_null() {
    let table = "test_insert_with_multiple_null_3";
    std::fs::write(table.to_owned() + ".csv", CONTENT).expect("Could not write to file");
    Command::new("target/debug/sql")
        .args([
            "./",
            format!("INSERT INTO {} (id, name) VALUES (3, 'John Snow')", table).as_str(),
        ])
        .output()
        .expect("Failed to execute command");

    let updated = std::fs::read_to_string(table.to_owned() + ".csv");
    assert_eq!(
        updated.unwrap_or("FAILED TO READ".to_string()),
    "id,name,email,age\n1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,20\n3,John Snow,NULL,NULL\n");
    std::fs::remove_file(table.to_owned() + ".csv").expect("Could not delete test file");
}
