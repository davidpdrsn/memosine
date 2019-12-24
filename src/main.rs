use sql::parse_sql_queries;
use std::io::{self, Read};

pub mod schema;
pub mod sql;

fn main() {
    let mut query = String::new();
    io::stdin().read_to_string(&mut query).unwrap();

    let queries = parse_sql_queries(&query).unwrap_or_else(|e| {
        eprintln!("{}", e);
        std::process::exit(1)
    });

    for query in queries {
        println!("{}", query);
    }
}
