use crate::schema::Type;
use crate::sql::Ident;
use crate::utils::parse::ParseError;
use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    TableAlreadyDefined {
        name: Ident,
    },
    UndefinedTable {
        name: Ident,
    },
    TableMissingFromSource {
        name: Ident,
    },
    UndefinedColumn {
        table_name: Option<Ident>,
        column_name: Ident,
    },
    InsertTypeError {
        table_name: Ident,
        column_name: Ident,
        expected_type: Type,
        actual_type: Type,
    },
    StarInWhereClause {
        table_name: Option<Ident>,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Error::*;

        match self {
            TableAlreadyDefined { name } => {
                write!(f, "Table `{}` is already defined", name)
            }
            UndefinedTable { name } => {
                write!(f, "Table `{}` is not defined", name)
            }
            TableMissingFromSource { name } => write!(
                f,
                "Table `{}` is missing from `from` or `join` clauses",
                name
            ),
            UndefinedColumn {
                table_name,
                column_name,
            } => {
                if let Some(table_name) = table_name {
                    write!(
                        f,
                        "Column `{}.{}` is not defined",
                        table_name, column_name
                    )
                } else {
                    write!(f, "Column `{}` is not defined", column_name)
                }
            }
            InsertTypeError {
                table_name,
                column_name,
                expected_type,
                actual_type,
            } => {
                writeln!(f, "Type error in insert statement")?;
                writeln!(
                    f,
                    "`{}.{}` has type `{}`",
                    table_name, column_name, expected_type
                )?;
                write!(f, "but received value of type `{}`", actual_type)?;

                Ok(())
            }
            StarInWhereClause { table_name } => {
                if let Some(table_name) = table_name {
                    write!(f, "Cannot have `{}.*` in where clause", table_name)
                } else {
                    write!(f, "Cannot have `*` in where clause")
                }
            }
        }
    }
}

impl std::error::Error for Error {}
