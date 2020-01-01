use database::Database;
use sql::{parse_sql_queries, Statement};
use std::io::{self, Read};
use std::path::PathBuf;
use structopt::StructOpt;

#[cfg(test)]
mod tests;

#[macro_use]
pub mod utils;
pub mod cli;
pub mod database;
pub mod error;
pub mod schema;
pub mod sql;

fn main() {
    cli::main()
}

// fn main() {
// }
