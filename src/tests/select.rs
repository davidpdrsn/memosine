use super::*;

#[test]
fn test_select_star() {
    let db = setup();
    let projection = run_select(&db, "select * from users");

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
    let projection = run_select(&db, "select users.* from users");

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
        run_select(&db, "select id, name, age, country_id from users");

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
    );

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
    let projection = run_select(&db, "select id, name from users");

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
    let projection = run_select(&db, "select users.id, users.name from users");

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
    let projection = run_select(&db, "select age, name from users");

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
    let projection = run_select(&db, "select users.age, users.name from users");

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
    let projection = run_select(&db, "select age, age from users");

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
    let projection = run_select(&db, "select users.*, age, age from users");

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
    let projection = run_select(&db, "select *, * from users");

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
    let projection = run_select(&db, "select users.*, users.* from users");

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

// TODO: `select users.* from countries` should fail
// TODO: relative ambiguous gives error (requires joins)