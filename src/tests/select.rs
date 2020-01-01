use super::*;

#[test]
fn test_select_star() {
    let db = setup();
    let projection = run_select(&db, "select * from users").unwrap();

    assert_eq!(2, projection.rows.len());

    let mut tuples = projection.to_tuples();

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(1))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Alice"))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(25))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.country_id"), Value::from(1))
    );

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(2))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Bob"))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(20))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.country_id"), Value::from(2))
    );

    assert!(tuples.is_empty());
}

#[test]
fn test_select_relative_star() {
    let db = setup();
    let projection = run_select(&db, "select users.* from users").unwrap();

    assert_eq!(2, projection.rows.len());

    let mut tuples = projection.to_tuples();

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(1))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Alice"))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(25))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.country_id"), Value::from(1))
    );

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(2))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Bob"))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(20))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.country_id"), Value::from(2))
    );

    assert!(tuples.is_empty());
}

#[test]
fn test_select_relative() {
    let db = setup();
    let projection =
        run_select(&db, "select id, name, age, country_id from users").unwrap();

    assert_eq!(2, projection.rows.len());

    let mut tuples = projection.to_tuples();

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(1))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Alice"))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(25))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.country_id"), Value::from(1))
    );

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(2))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Bob"))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(20))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.country_id"), Value::from(2))
    );

    assert!(tuples.is_empty());
}

#[test]
fn test_select_absolute() {
    let db = setup();
    let projection = run_select(
        &db,
        "select users.id, users.name, users.age, users.country_id from users",
    ).unwrap();

    assert_eq!(2, projection.rows.len());

    let mut tuples = projection.to_tuples();

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(1))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Alice"))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(25))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.country_id"), Value::from(1))
    );

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(2))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Bob"))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(20))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.country_id"), Value::from(2))
    );

    assert!(tuples.is_empty());
}

#[test]
fn test_select_relative_subset() {
    let db = setup();
    let projection = run_select(&db, "select id, name from users").unwrap();

    assert_eq!(2, projection.rows.len());

    let mut tuples = projection.to_tuples();

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(1))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Alice"))
    );

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(2))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Bob"))
    );

    assert!(tuples.is_empty());
}

#[test]
fn test_select_absolute_subset() {
    let db = setup();
    let projection = run_select(&db, "select users.id, users.name from users").unwrap();

    assert_eq!(2, projection.rows.len());

    let mut tuples = projection.to_tuples();

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(1))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Alice"))
    );

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(2))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Bob"))
    );

    assert!(tuples.is_empty());
}

#[test]
fn test_select_relative_different_order() {
    let db = setup();
    let projection = run_select(&db, "select age, name from users").unwrap();

    assert_eq!(2, projection.rows.len());

    let mut tuples = projection.to_tuples();

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(25))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Alice"))
    );

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(20))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Bob"))
    );

    assert!(tuples.is_empty());
}

#[test]
fn test_select_absolute_different_order() {
    let db = setup();
    let projection = run_select(&db, "select users.age, users.name from users").unwrap();

    assert_eq!(2, projection.rows.len());

    let mut tuples = projection.to_tuples();

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(25))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Alice"))
    );

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(20))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Bob"))
    );

    assert!(tuples.is_empty());
}

#[test]
fn select_same_column_twice() {
    let db = setup();
    let projection = run_select(&db, "select age, age from users").unwrap();

    assert_eq!(2, projection.rows.len());

    let mut tuples = projection.to_tuples();

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(25))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(25))
    );

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(20))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(20))
    );

    assert!(tuples.is_empty());
}

#[test]
fn select_same_column_twice_including_star() {
    let db = setup();
    let projection = run_select(&db, "select users.*, age, age from users").unwrap();

    assert_eq!(2, projection.rows.len());

    let mut tuples = projection.to_tuples();

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(1))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Alice"))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(25))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.country_id"), Value::from(1))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(25))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(25))
    );

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(2))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Bob"))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(20))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.country_id"), Value::from(2))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(20))
    );
    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.age"), Value::from(20))
    );

    assert!(tuples.is_empty());
}

#[test]
fn select_double_star_relative() {
    let db = setup();
    let projection = run_select(&db, "select *, * from users").unwrap();

    assert_eq!(2, projection.rows.len());

    let mut tuples = projection.to_tuples();

    for _ in 0..2 {
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.id"), Value::from(1))
        );
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.name"), Value::from("Alice"))
        );
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.age"), Value::from(25))
        );
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.country_id"), Value::from(1))
        );
    }

    for _ in 0..2 {
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.id"), Value::from(2))
        );
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.name"), Value::from("Bob"))
        );
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.age"), Value::from(20))
        );
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.country_id"), Value::from(2))
        );
    }

    assert!(tuples.is_empty());
}

#[test]
fn select_double_star_absolute() {
    let db = setup();
    let projection = run_select(&db, "select users.*, users.* from users").unwrap();

    assert_eq!(2, projection.rows.len());

    let mut tuples = projection.to_tuples();

    for _ in 0..2 {
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.id"), Value::from(1))
        );
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.name"), Value::from("Alice"))
        );
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.age"), Value::from(25))
        );
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.country_id"), Value::from(1))
        );
    }

    for _ in 0..2 {
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.id"), Value::from(2))
        );
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.name"), Value::from("Bob"))
        );
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.age"), Value::from(20))
        );
        assert_eq!(
            tuples.remove(0),
            (AbsoluteColumn::from("users.country_id"), Value::from(2))
        );
    }

    assert!(tuples.is_empty());
}

#[test]
fn selecting_undefined_column_relative() {
    let db = setup();
    let projection = run_select(&db, "select foo from users");
    assert!(projection.is_err());
}

#[test]
fn selecting_undefined_column_absolute() {
    let db = setup();
    let projection = run_select(&db, "select users.foo from users");
    assert!(projection.is_err());
}

#[test]
fn column_missing_from_source() {
    let db = setup();
    let projection = run_select(&db, "select countries.id from users");
    assert!(projection.is_err());
}

#[test]
fn column_missing_from_source_star() {
    let db = setup();
    let projection = run_select(&db, "select countries.* from users");
    assert!(projection.is_err());
}

#[test]
fn column_missing_from_source_star_undefined_source() {
    let db = setup();
    let projection = run_select(&db, "select foo.* from users");
    assert!(projection.is_err());
}

#[test]
fn select_where_relative_lit_eq_lit() {
    let db = setup();
    let projection = run_select(&db, "select id from users where 1 = 1").unwrap();
    let mut tuples = projection.to_tuples();

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(1))
    );

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.id"), Value::from(2))
    );

    assert!(tuples.is_empty());
}

#[test]
fn select_where_relative_lit_eq_lit_false() {
    let db = setup();
    let projection = run_select(&db, "select id from users where 1 = 2").unwrap();
    let tuples = projection.to_tuples();

    assert!(tuples.is_empty());
}

#[test]
fn select_where_relative_col_eq_lit() {
    let db = setup();
    let projection = run_select(&db, "select name from users where id = 1").unwrap();

    let mut tuples = projection.to_tuples();

    assert_eq!(
        tuples.remove(0),
        (AbsoluteColumn::from("users.name"), Value::from("Alice"))
    );

    assert!(tuples.is_empty());
}

// TODO: relative ambiguous gives error (requires joins)
