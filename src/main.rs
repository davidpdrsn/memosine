use database::Database;
use std::path::PathBuf;
use sql::{parse_sql_queries, Statement};
use std::io::{self, Read};
use structopt::StructOpt;

#[cfg(test)]
mod tests;

#[macro_use]
pub mod utils;
pub mod database;
pub mod error;
pub mod schema;
pub mod sql;
pub mod cli;

fn main() {
    cli::main()
}

// fn main() {
// }
