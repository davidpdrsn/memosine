use sql::{parse, Statement};
use std::io;
use std::io::Read;

pub mod sql;

fn main() {
    let mut query = String::new();
    io::stdin().read_to_string(&mut query).unwrap();
    let ast = parse(&Statement::parser(), &query).unwrap();
    println!("{:#?}", ast);
}
