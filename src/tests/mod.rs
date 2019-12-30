use crate::database::{Database, Value, Projection, AbsoluteColumn};
use crate::sql::{parse_sql_queries, Ident, Statement};

mod select;

pub fn setup() -> Database {
    let mut db = Database::new();

    let setup_statements = r#"
        create table countries (
            id Int,
            name String,
        );

        insert into countries (name = "Denmark");
        insert into countries (name = "Sweden");

        create table users (
            id Int,
            name String,
            age Int,
            country_id Int,
        );

        insert into users (name = "Alice", age = 25, country_id = 1);
        insert into users (name = "Bob", age = 20, country_id = 2);
    "#;

    let statements = parse_sql_queries(setup_statements).unwrap();
    run_all_statements(&mut db, statements);

    db
}

fn run_all_statements(db: &mut Database, statements: Vec<Statement>) {
    for statement in statements {
        match statement {
            Statement::CreateTable(inner) => {
                db.run_create_table(inner).unwrap();
            }
            Statement::Insert(inner) => {
                db.run_insert(inner).unwrap();
            }
            Statement::Select(_) => {
                panic!("cannot run select statements during test setup")
            }
        }
    }
}

fn run_select<'a>(db: &'a Database, query: &str) -> Projection<'a> {
    let mut queries = parse_sql_queries(query).unwrap();
    assert_eq!(1, queries.len(), "more than one query");
    let query = match queries.remove(0) {
        Statement::Select(inner) => inner,
        _ => panic!("thats not a select query"),
    };
    db.run_select(&query).unwrap()
}
