use database::Database;
use schema::Schema;
use sql::{parse_sql_queries, Statement};
use std::{
    io::{self, Read},
    mem,
};

pub mod error;
pub mod database;
pub mod schema;
pub mod sql;
pub mod utils;

macro_rules! or_exit {
    ($e:expr) => {
        $e.unwrap_or_else(|e| {
            eprintln!("{}", e);
            std::process::exit(1)
        })
    };
}

fn main() {
    let mut query = String::new();
    or_exit!(io::stdin().read_to_string(&mut query));
    let statements = or_exit!(parse_sql_queries(&query));

    let mut db = Database::new();

    for statement in statements {
        match statement {
            Statement::CreateTable(inner) => {
                or_exit!(db.run_create_table(inner));
            }
            Statement::Insert(inner) => {
                or_exit!(db.run_insert(inner));
            }
            Statement::Select(inner) => {
                let selection = or_exit!(db.run_select(inner));
                println!("{}", selection.tsv());
            }
        }
    }
}
