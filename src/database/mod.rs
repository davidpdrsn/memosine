mod byte_conversion;

use crate::error::{Error, Result};
use crate::schema::{self, Schema, Type};
use crate::sql::{
    AbsoluteColumn, Column, CreateTable, Ident, Insert, RelativeColumn, Select, Source,
};
use crate::utils::{Arena, Id, IdMap};
use std::collections::HashMap;
use std::fmt;
use std::iter::FromIterator;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub struct Database {
    tables: IdMap<Table>,
    all_rows: Arena<Row>,
    prev_id: AtomicI64,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: IdMap::new(),
            all_rows: Arena::new(),
            prev_id: AtomicI64::new(0),
        }
    }

    fn next_id(&self) -> i64 {
        let next_id = self.prev_id.fetch_add(1, Ordering::SeqCst);
        next_id + 1
    }

    pub fn run_create_table(&mut self, stmt: CreateTable) -> Result<()> {
        if self.tables.contains_key(&stmt.name) {
            return Err(Error::TableAlreadyDefined {
                name: stmt.name.clone(),
            });
        }

        let name = stmt.name.clone();
        let schema = schema::Table::create(stmt);
        let table = Table {
            name,
            rows: Vec::new(),
            schema,
        };
        self.tables.insert(table);

        Ok(())
    }

    pub fn run_insert(&mut self, mut stmt: Insert) -> Result<()> {
        let row = {
            let table = self
                .tables
                .get(&stmt.table)
                .ok_or_else(|| Error::UndefinedTable {
                    name: stmt.table.clone(),
                })?;
            let table_schema = &table.schema;

            table_schema
                .columns()
                .into_iter()
                .map(|(column_name, column_type)| {
                    if column_name == "id" {
                        Ok(Value::Int(self.next_id()))
                    } else {
                        let value = stmt.values.remove(column_name).ok_or_else(|| {
                            Error::UndefinedColumn {
                                table_name: table.name.clone(),
                                column_name: column_name.clone(),
                            }
                        })?;

                        let value_type = value.ty();

                        if column_type == &value_type {
                            Ok(value)
                        } else {
                            Err(Error::InsertTypeError {
                                table_name: table.name.clone(),
                                column_name: column_name.clone(),
                                expected_type: column_type.clone(),
                                actual_type: value_type.clone(),
                            })
                        }
                    }
                })
                .collect::<Result<Row>>()?
        };

        let table = self
            .tables
            .get_mut(&stmt.table)
            .ok_or_else(|| Error::UndefinedTable {
                name: stmt.table.clone(),
            })?;

        let row_idx = self.all_rows.push(row);
        table.rows.push(row_idx);

        Ok(())
    }

    pub fn run_select(&self, stmt: Select) -> Result<SelectOutput> {
        assert!(stmt.where_clause.is_none(), "where clauses not supported");
        assert!(stmt.joins.is_empty(), "joins not supported");

        let mut output = SelectOutput::default();

        let row_candidates = match &stmt.source {
            Source::Table(table) => {
                let table = self.get_table(table)?;
                output.set_header(table);

                let mut row_candidates = Vec::<usize>::new();

                for selection in &stmt.columns {
                    match selection {
                        Column::Relative(column) => match column {
                            RelativeColumn::Ident(column) => unimplemented!("ident"),
                            RelativeColumn::Star(_) => row_candidates.extend(&table.rows),
                        },

                        Column::Absolute(AbsoluteColumn {
                            table: table_name,
                            column,
                        }) => {
                            assert!(
                                table_name == &table.name,
                                "`{}` is not part of from clause",
                                table_name
                            );

                            match column {
                                RelativeColumn::Ident(column) => unimplemented!("ident absolute"),
                                RelativeColumn::Star(_) => row_candidates.extend(&table.rows),
                            }
                        }
                    }
                }

                row_candidates
            }
            Source::Query { query, alias } => unimplemented!("sub queries not supported"),
        };

        let row = row_candidates.iter().map(|idx| &self.all_rows[*idx]);
        output.rows.extend(row);

        Ok(output)
    }

    fn get_table(&self, name: &Ident) -> Result<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| Error::UndefinedTable { name: name.clone() })
    }
}

#[derive(Debug)]
struct Table {
    name: Ident,
    rows: Vec<usize>,
    schema: schema::Table,
}

impl Id for Table {
    type Id = Ident;

    fn id(&self) -> Self::Id {
        self.name.id()
    }
}

#[derive(Debug)]
pub struct Row {
    values: Vec<Value>,
}

impl FromIterator<Value> for Row {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Value>,
    {
        Row {
            values: iter.into_iter().collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    String(String),
}

impl Value {
    fn ty(&self) -> Type {
        match self {
            Value::Int(_) => Type::Named(Ident::new("Int")),
            Value::String(_) => Type::Named(Ident::new("String")),
        }
    }
}

#[derive(Debug, Default)]
pub struct SelectOutput<'a> {
    pub columns: Vec<AbsoluteColumn>,
    pub rows: Vec<&'a Row>,
}

impl<'a> SelectOutput<'a> {
    fn set_header(&mut self, table: &Table) {
        self.columns.extend(
            table
                .schema
                .columns()
                .into_iter()
                .map(|(ident, _)| AbsoluteColumn {
                    table: table.name.clone(),
                    column: RelativeColumn::Ident(ident.clone()),
                }),
        );
    }
}

impl<'a> SelectOutput<'a> {
    pub fn tsv(&'a self) -> Tsv<'a> {
        Tsv(self)
    }
}

pub struct Tsv<'a>(&'a SelectOutput<'a>);

impl<'a> fmt::Display for Tsv<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "{}",
            self.0
                .columns
                .iter()
                .map(|c| format!("{}", c))
                .collect::<Vec<_>>()
                .join("\t")
        )?;

        for row in &self.0.rows {
            writeln!(
                f,
                "{}",
                row.values
                    .iter()
                    .map(|c| format!("{}", c))
                    .collect::<Vec<_>>()
                    .join("\t")
            )?;
        }

        Ok(())
    }
}
