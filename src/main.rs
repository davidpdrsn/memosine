use sql::{parse_sql_query, Statement};
use std::io;
use std::io::Read;

pub mod sql;

fn main() {
    let mut query = String::new();
    io::stdin().read_to_string(&mut query).unwrap();
    let ast = parse_sql_query(&query).unwrap();
    println!("{:#?}", ast);
}
