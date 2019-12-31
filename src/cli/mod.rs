use crate::{
    database::Database,
    error::Error,
    sql::{parse_sql_queries, Statement},
    utils::parse::*,
};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use structopt::StructOpt;

macro_rules! or_exit {
    ($e:expr) => {
        $e.unwrap_or_else(|e| {
            eprintln!("{}", e);
            std::process::exit(1)
        })
    };
}

#[derive(StructOpt, Debug)]
#[structopt(name = "memosine")]
enum Cli {
    Repl {
        #[structopt(short = "f", long, name = "FILE", parse(from_os_str))]
        load_file: Option<PathBuf>,
    },
}

#[derive(Debug)]
enum CliError<'a> {
    DatabaseError(Error),
    ParseError(ParseError<'a>),
    ReadlineError(ReadlineError),
}

impl<'a> fmt::Display for CliError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use CliError::*;
        match self {
            DatabaseError(inner) => write!(f, "{}", inner),
            ParseError(inner) => write!(f, "{}", inner),
            ReadlineError(inner) => write!(f, "{}", inner),
        }
    }
}

impl<'a> std::error::Error for CliError<'a> {}

impl<'a> From<Error> for CliError<'a> {
    fn from(x: Error) -> CliError<'a> {
        CliError::DatabaseError(x)
    }
}

impl<'a> From<ParseError<'a>> for CliError<'a> {
    fn from(x: ParseError<'a>) -> CliError<'a> {
        CliError::ParseError(x)
    }
}

impl<'a> From<ReadlineError> for CliError<'a> {
    fn from(x: ReadlineError) -> CliError<'a> {
        CliError::ReadlineError(x)
    }
}

pub fn main() {
    let cli = Cli::from_args();

    match cli {
        Cli::Repl { load_file } => {
            let file_contents = load_file
                .map(|path| {
                    println!("Running file {}", path.display());
                    fs::read_to_string(path)
                })
                .transpose();
            let file_contents = or_exit!(file_contents);
            or_exit!(repl(&file_contents));
        }
    };
}

fn repl(file_contents: &Option<String>) -> Result<(), CliError> {
    let mut db = Database::new();

    if let Some(file_contents) = file_contents {
        run_file(&mut db, file_contents)?;
    }

    let mut rl = Editor::<()>::new();

    let history_path = "/tmp/memosine-repl-history.txt";
    let _ = rl.load_history(history_path);

    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(line) => {
                let cmd = match parse(&CliCommand::parser(), &line) {
                    Ok(cmd) => cmd,
                    Err(err) => {
                        eprintln!("Parse error: {}", err);
                        continue;
                    }
                };

                match cmd {
                    CliCommand::JustWhitespace => {}
                    CliCommand::Quit => break,
                    CliCommand::SqlStatement(stmt) => {
                        rl.add_history_entry(line.as_str());

                        let start = Instant::now();
                        run_statement(&mut db, stmt)?;
                        let duration = start.elapsed();
                        println!("Took {:?}", duration);
                    }
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => return Err(CliError::from(err)),
        }
    }

    let _ = rl.save_history(history_path);

    Ok(())
}

fn run_file<'a>(
    db: &mut Database,
    file_contents: &'a String,
) -> Result<(), CliError<'a>> {
    let statements = parse_sql_queries(&file_contents)?;

    for statement in statements {
        println!("Running `{}`", statement);
        run_statement(db, statement)?;
    }

    Ok(())
}

fn run_statement<'a>(
    db: &mut Database,
    statement: Statement,
) -> Result<(), CliError<'a>> {
    match statement {
        Statement::CreateTable(inner) => {
            db.run_create_table(inner)?;
        }
        Statement::Insert(inner) => {
            db.run_insert(inner)?;
        }
        Statement::Select(inner) => {
            let selection = db.run_select(&inner)?;
            println!("{}", selection.tsv());
        }
    }

    Ok(())
}

#[derive(Debug)]
enum CliCommand {
    SqlStatement(Statement),
    Quit,
    JustWhitespace,
}

impl CliCommand {
    pub fn parser() -> impl Parser<Output = Self> {
        let select = Statement::parser().map(CliCommand::SqlStatement);
        let just_whitespace = whitespace().map(|_| CliCommand::JustWhitespace);
        let quit = string("\\q").map(|_| CliCommand::Quit);

        select.or(quit).or(just_whitespace)
    }
}
