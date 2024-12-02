use std::{collections::HashMap, path::PathBuf};

use db::{initialize_context, Options, PrimaryKey, Schema, SchemaType};

#[test]
fn test_initialize_context() {
    let node = PathBuf::from("tests/node_test");
    let ks = node.join("ks_test");
    let ctx = initialize_context(&node).unwrap();
    let opt = ctx.get_keyspace_options(&ks).unwrap();
    let opt_cmp = Options::new(true, "SimpleStrategy".to_string(), 3);
    assert!(ctx.is_a_keyspace("ks_test"));
    assert_eq!(opt.durable_writes, opt_cmp.durable_writes);
    assert_eq!(opt.replication.class, opt_cmp.replication.class);
    assert_eq!(
        opt.replication.replication_factor,
        opt_cmp.replication.replication_factor
    );
}

#[test]
fn test_create_delete_keyspace() {
    let node = PathBuf::from("tests/node_tests_ks");
    let ks = node.join("ks_test_create");
    let mut ctx = initialize_context(&node).unwrap();
    let opt = Options::new(true, "SimpleStrategy".to_string(), 2);
    ctx.create_keyspace(&ks, &opt).unwrap();
    let opt_cmp = ctx.get_keyspace_options(&ks).unwrap();
    assert!(ctx.is_a_keyspace("ks_test_create"));
    assert_eq!(opt.durable_writes, opt_cmp.durable_writes);
    assert_eq!(opt.replication.class, opt_cmp.replication.class);
    assert_eq!(
        opt.replication.replication_factor,
        opt_cmp.replication.replication_factor
    );

    ctx.drop_keyspace(&ks).unwrap();
    assert!(!ks.exists());
    assert!(!ctx.is_a_keyspace("ks_test_create"));
}

#[test]
fn test_create_delete_table() {
    let node = PathBuf::from("tests/node_test_delete");
    let table = node.join("ks_test/table_test_create");
    let mut ctx = initialize_context(&node).unwrap();
    let mut cols = HashMap::new();
    cols.insert("id".to_string(), SchemaType::Int);
    cols.insert("name".to_string(), SchemaType::Text);
    cols.insert("email".to_string(), SchemaType::Text);
    cols.insert("age".to_string(), SchemaType::Int);
    let prim_key = PrimaryKey::new(vec!["id".to_string()], vec!["age".to_string()]);
    let schema = Schema::new(cols, prim_key.clone());
    ctx.create_table(&table, &schema).unwrap();
    assert!(table.exists());

    let table_schema = ctx
        .get_table_schema("ks_test", "table_test_create")
        .unwrap();
    let table_primary_key = table_schema.get_primary_key();
    assert_eq!(schema.get_columns(), table_schema.get_columns());
    assert_eq!(
        prim_key.get_clustering_key(),
        table_primary_key.get_clustering_key()
    );
    assert_eq!(
        prim_key.get_partition_key(),
        table_primary_key.get_partition_key()
    );

    ctx.drop_table(&table).unwrap();
    assert!(!table.exists());
}

#[test]
fn test_select() {
    let node = PathBuf::from("tests/node_test");
    let table = node.join("ks_test/table_test_select");
    let ctx = initialize_context(&node).unwrap();
    let mut rows = Vec::new();
    ctx.read_table(&table, &mut |row| {
        rows.push(row);
        Ok(())
    })
    .unwrap();

    assert_eq!(rows.len(), 2);
    let mut row = &rows[0];
    assert_eq!(row.get("id").unwrap(), "1");
    assert_eq!(row.get("name").unwrap(), "John Doe");
    assert_eq!(row.get("email").unwrap(), "john@example.com");
    assert_eq!(row.get("age").unwrap(), "30");
    row = &rows[1];
    assert_eq!(row.get("id").unwrap(), "2");
    assert_eq!(row.get("name").unwrap(), "Jane Smith");
    assert_eq!(row.get("email").unwrap(), "jane@example.com");
    assert_eq!(row.get("age").unwrap(), "20");
}

#[test]
fn test_insert_and_delete() {
    let node = PathBuf::from("tests/node_test");
    let table = node.join("ks_test/table_test_insert");
    let mut ctx = initialize_context(&node).unwrap();

    let mut new_row = HashMap::new();
    new_row.insert("name".to_string(), "tablet".to_string());
    new_row.insert("price".to_string(), "150".to_string());
    new_row.insert("quantity".to_string(), "3".to_string());

    ctx.append_to_table(&table, new_row).unwrap();

    let mut rows = Vec::new();
    ctx.read_table(&table, &mut |row| {
        rows.push(row.clone());
        Ok(())
    })
    .unwrap();

    assert_eq!(rows.len(), 3);
    let mut row = &rows[0];
    assert_eq!(row.get("name").unwrap(), "phone");
    assert_eq!(row.get("price").unwrap(), "99.99");
    assert_eq!(row.get("quantity").unwrap(), "1");

    row = &rows[1];
    assert_eq!(row.get("name").unwrap(), "laptop");
    assert_eq!(row.get("price").unwrap(), "999.99");
    assert_eq!(row.get("quantity").unwrap(), "1");

    row = &rows[2];
    assert_eq!(row.get("name").unwrap(), "tablet");
    assert_eq!(row.get("price").unwrap(), "150");
    assert_eq!(row.get("quantity").unwrap(), "3");

    ctx.update_table(&table, &mut |row| {
        if row.get("name").unwrap() == "tablet" {
            Ok(None)
        } else {
            Ok(Some(row.clone()))
        }
    })
    .unwrap();

    let mut new_rows = Vec::new();
    ctx.read_table(&table, &mut |row| {
        new_rows.push(row.clone());
        Ok(())
    })
    .unwrap();

    assert_eq!(new_rows.len(), 2);
    let mut row = &new_rows[0];
    assert_eq!(row.get("name").unwrap(), "phone");
    assert_eq!(row.get("price").unwrap(), "99.99");
    assert_eq!(row.get("quantity").unwrap(), "1");

    row = &new_rows[1];
    assert_eq!(row.get("name").unwrap(), "laptop");
    assert_eq!(row.get("price").unwrap(), "999.99");
    assert_eq!(row.get("quantity").unwrap(), "1");
}

#[test]
fn test_update() {
    let node = PathBuf::from("tests/node_test");
    let table = node.join("ks_test/table_test_update");
    let mut ctx = initialize_context(&node).unwrap();

    ctx.update_table(&table, &mut |row| {
        if row.get("name").unwrap() == "phone" {
            let mut new_row = row.clone();
            new_row.insert("price".to_string(), "199.99".to_string());
            Ok(Some(new_row))
        } else {
            Ok(Some(row.clone()))
        }
    })
    .unwrap();

    let mut rows = Vec::new();
    ctx.read_table(&table, &mut |row| {
        rows.push(row.clone());
        Ok(())
    })
    .unwrap();

    assert_eq!(rows.len(), 2);
    let mut row = &rows[0];
    assert_eq!(row.get("name").unwrap(), "phone");
    assert_eq!(row.get("price").unwrap(), "199.99");
    assert_eq!(row.get("quantity").unwrap(), "1");

    row = &rows[1];
    assert_eq!(row.get("name").unwrap(), "laptop");
    assert_eq!(row.get("price").unwrap(), "999.99");
    assert_eq!(row.get("quantity").unwrap(), "1");

    ctx.update_table(&table, &mut |row| {
        if row.get("name").unwrap() == "phone" {
            let mut new_row = row.clone();
            new_row.insert("price".to_string(), "99.99".to_string());
            Ok(Some(new_row))
        } else {
            Ok(Some(row.clone()))
        }
    })
    .unwrap();
}

#[test]
fn test_create_keyspace_already_exist() {
    let node = PathBuf::from("tests/node_test");
    let ks = node.join("ks_test");
    let mut ctx = initialize_context(&node).unwrap();
    let opt = Options::new(true, "SimpleStrategy".to_string(), 2);
    assert!(ctx.create_keyspace(&ks, &opt).is_err());
}

#[test]
fn test_create_table_already_exist() {
    let node = PathBuf::from("tests/node_test");
    let table = node.join("ks_test/table_test_select");
    let mut ctx = initialize_context(&node).unwrap();
    let mut cols = HashMap::new();
    cols.insert("id".to_string(), SchemaType::Int);
    cols.insert("name".to_string(), SchemaType::Text);
    cols.insert("email".to_string(), SchemaType::Text);
    cols.insert("age".to_string(), SchemaType::Int);
    let prim_key = PrimaryKey::new(vec!["id".to_string()], vec!["age".to_string()]);
    let schema = Schema::new(cols, prim_key);
    assert!(ctx.create_table(&table, &schema).is_err());
}
