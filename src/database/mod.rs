use crate::error::{self, Error, Result};
use crate::schema::{self, Schema, Type};
use crate::sql::{
    self, BinOp, Column, CreateTable, Ident, Insert, RelativeColumn, Select,
    Source,
};
use crate::utils::ExtendOrSet;
use crate::utils::{Arena, Id, IdMap};
use std::borrow::Borrow;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::iter::FromIterator;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub struct Database {
    tables: IdMap<Table>,
    all_rows: Arena<Row>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: IdMap::new(),
            all_rows: Arena::new(),
        }
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
            prev_id: AtomicI64::new(0),
        };
        self.tables.insert(table);

        Ok(())
    }

    pub fn run_insert(&mut self, mut stmt: Insert) -> Result<()> {
        let row = {
            let table = self.tables.get(&stmt.table).ok_or_else(|| {
                Error::UndefinedTable {
                    name: stmt.table.clone(),
                }
            })?;
            let table_schema = &table.schema;

            table_schema
                .columns()
                .into_iter()
                .map(|(column_name, column_type)| {
                    if column_name == "id" {
                        Ok(Value::Int(table.next_id()))
                    } else {
                        let value = stmt
                            .values
                            .remove(column_name)
                            .ok_or_else(|| Error::UndefinedColumn {
                                table_name: Some(table.name.clone()),
                                column_name: column_name.clone(),
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

        let table = self.tables.get_mut(&stmt.table).ok_or_else(|| {
            Error::UndefinedTable {
                name: stmt.table.clone(),
            }
        })?;

        let row_idx = self.all_rows.push(row);
        table.rows.push(row_idx);

        Ok(())
    }

    pub fn run_select(&self, stmt: &Select) -> Result<Projection> {
        assert!(stmt.joins.len() == 0, "joins are not supported");

        let full_projection = {
            let mut full_projection = Projection::default();

            let rows = match &stmt.source {
                Source::Table(table_name) => {
                    let table = self.get_table(table_name)?;
                    full_projection
                        .extend_header_with_columns_from_table(table);
                    table.rows.iter().map(|idx| &self.all_rows[*idx])
                }
                other => todo!("{:?}", other),
            };
            full_projection.extend_rows(rows);

            full_projection
        };

        let full_projection = if let Some(where_clause) = &stmt.where_clause {
            let Projection { header, rows } = full_projection;

            let iter = rows
                .into_iter()
                .map(|row| Result::<_, Error>::Ok(Some(row)));
            let filtered_iter =
                self.apply_where_clause(Box::new(iter), where_clause, &header);

            let rows = filtered_iter
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .filter_map(|x| x)
                .collect::<Vec<_>>();

            Projection { header, rows }
        } else {
            full_projection
        };

        let mut projection = Projection::default();

        // NOTE: looping over full_projection.rows for each selected column seems is probably slow
        for selected_column in &stmt.columns {
            match selected_column {
                Column::Absolute(absolute) => {
                    let table_name = &absolute.table;

                    match &absolute.column {
                        sql::RelativeColumn::Star(_) => {
                            let (cell_idxs, columns) = full_projection
                                .header
                                .iter()
                                .enumerate()
                                .filter(|(_, column)| {
                                    &column.table == table_name
                                })
                                .unzip::<_, _, Vec<_>, Vec<_>>();

                            if columns.is_empty() {
                                return Err(Error::TableMissingFromSource {
                                    name: table_name.clone(),
                                });
                            }

                            // extend header
                            projection.header.extend(
                                columns.iter().map(|col| (*col).clone()),
                            );

                            // extend rows
                            if projection.rows.is_empty() {
                                let rows = full_projection
                                    .rows
                                    .iter()
                                    .map(|row| {
                                        cell_idxs
                                            .iter()
                                            .map(move |idx| {
                                                row.values[*idx].clone()
                                            })
                                            .collect::<Row>()
                                    })
                                    .map(Cow::Owned)
                                    .collect::<Vec<_>>();

                                projection.rows.extend(rows);
                            } else {
                                full_projection
                                    .rows
                                    .iter()
                                    .zip(&mut projection.rows)
                                    .for_each(|(full_row, projected_row)| {
                                        let projected_row =
                                            projected_row.to_mut();
                                        let values =
                                            cell_idxs.iter().map(|cell_idx| {
                                                full_row.values[*cell_idx]
                                                    .clone()
                                            });
                                        projected_row.values.extend(values);
                                    });
                            }
                        }
                        sql::RelativeColumn::Ident(column_name) => {
                            let mut matching_columns = full_projection
                                .header
                                .iter()
                                .enumerate()
                                .filter(|(_, name)| {
                                    &name.table == table_name
                                        && &name.column == column_name
                                })
                                .collect::<Vec<_>>();

                            match matching_columns.len() {
                                0 => {
                                    return Err(Error::UndefinedColumn {
                                        table_name: Some(table_name.clone()),
                                        column_name: column_name.clone(),
                                    })
                                }
                                1 => {
                                    let (cell_idx, column_name) =
                                        matching_columns.remove(0);

                                    // extend header
                                    projection.header.push(column_name.clone());

                                    // extend rows
                                    if projection.rows.is_empty() {
                                        for row in &full_projection.rows {
                                            let value =
                                                row.values[cell_idx].clone();
                                            let row = Row {
                                                values: vec![value],
                                            };
                                            projection
                                                .rows
                                                .push(Cow::Owned(row));
                                        }
                                    } else {
                                        full_projection
                                            .rows
                                            .iter()
                                            .zip(&mut projection.rows)
                                            .for_each(
                                                |(full_row, projected_row)| {
                                                    let projected_row =
                                                        projected_row.to_mut();
                                                    let value = full_row.values
                                                        [cell_idx]
                                                        .clone();
                                                    projected_row
                                                        .values
                                                        .push(value);
                                                },
                                            );
                                    }
                                }
                                _ => todo!("what does this mean?"),
                            }
                        }
                    }
                }
                Column::Relative(relative) => match relative {
                    RelativeColumn::Star(_) => {
                        // extend header
                        projection
                            .header
                            .extend(full_projection.header.iter().cloned());

                        // extend rows
                        if projection.rows.is_empty() {
                            projection
                                .rows
                                .extend(full_projection.rows.iter().cloned());
                        } else {
                            full_projection
                                .rows
                                .iter()
                                .zip(&mut projection.rows)
                                .for_each(|(full_row, projected_row)| {
                                    let projected_row = projected_row.to_mut();
                                    projected_row.values.extend(
                                        full_row.values.iter().cloned(),
                                    );
                                });
                        }
                    }
                    RelativeColumn::Ident(column_name) => {
                        let mut matching_columns = full_projection
                            .header
                            .iter()
                            .enumerate()
                            .filter(|(_, name)| &name.column == column_name)
                            .collect::<Vec<_>>();

                        match matching_columns.len() {
                            0 => {
                                return Err(Error::UndefinedColumn {
                                    table_name: None,
                                    column_name: column_name.clone(),
                                })
                            }
                            1 => {
                                let (cell_idx, column_name) =
                                    matching_columns.remove(0);

                                // extend header
                                projection.header.push(column_name.clone());

                                // extend rows
                                if projection.rows.is_empty() {
                                    for row in &full_projection.rows {
                                        let value =
                                            row.values[cell_idx].clone();
                                        let row = Row {
                                            values: vec![value],
                                        };
                                        projection.rows.push(Cow::Owned(row));
                                    }
                                } else {
                                    full_projection
                                        .rows
                                        .iter()
                                        .zip(&mut projection.rows)
                                        .for_each(
                                            |(full_row, projected_row)| {
                                                let projected_row =
                                                    projected_row.to_mut();
                                                let value = full_row.values
                                                    [cell_idx]
                                                    .clone();
                                                projected_row
                                                    .values
                                                    .push(value);
                                            },
                                        );
                                }
                            }
                            _ => {
                                todo!("unambiguous relative column projection")
                            }
                        }
                    }
                },
            }
        }

        Ok(projection)
    }

    pub fn get_table(&self, name: &Ident) -> Result<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| Error::UndefinedTable { name: name.clone() })
    }

    fn apply_where_clause<'query, 'db: 'query>(
        &'db self,
        iter: Box<dyn Iterator<Item = Result<Option<Cow<'db, Row>>>> + 'query>,
        clause: &'query sql::WhereClause,
        header: &'query [AbsoluteColumn],
    ) -> Box<dyn Iterator<Item = Result<Option<Cow<'db, Row>>>> + 'query> {
        let new_iter = iter.map(move |row| {
            let row = row?;

            let cond: Option<bool> = row
                .as_ref()
                .map(|row| self.eval_where_clause(row, clause, header))
                .transpose()?;

            if cond.unwrap_or_else(|| false) {
                Ok(row)
            } else {
                Ok(None)
            }
        });

        Box::new(new_iter)
    }

    fn eval_where_clause<'query, 'db: 'query>(
        &'db self,
        row: &'db Cow<'db, Row>,
        clause: &'query sql::WhereClause,
        header: &[AbsoluteColumn],
    ) -> Result<bool> {
        use sql::Expr;
        use sql::WhereClause::*;

        match clause {
            Grouped(inner) => self.eval_where_clause(row, &*inner, header),

            And(lhs, rhs) => {
                let cond = self.eval_where_clause(row, &*lhs, header)?
                    && self.eval_where_clause(row, &*rhs, header)?;
                Ok(cond)
            }
            Or(lhs, rhs) => {
                let cond = self.eval_where_clause(row, &*lhs, header)?
                    || self.eval_where_clause(row, &*rhs, header)?;
                Ok(cond)
            }

            BinOp(op, lhs, rhs) => match (lhs, rhs) {
                (Expr::Value(lhs), Expr::Value(rhs)) => Ok(op.eval(lhs, rhs)),
                (Expr::Value(lhs), Expr::Column(rhs_ref)) => {
                    let rhs = self.column_value(rhs_ref, header, row)?;
                    Ok(op.eval(lhs, rhs))
                }
                (Expr::Column(lhs_ref), Expr::Value(rhs)) => {
                    let lhs = self.column_value(lhs_ref, header, row)?;
                    Ok(op.eval(lhs, rhs))
                }
                (Expr::Column(lhs_ref), Expr::Column(rhs_ref)) => {
                    let lhs = self.column_value(lhs_ref, header, row)?;
                    let rhs = self.column_value(rhs_ref, header, row)?;
                    Ok(op.eval(lhs, rhs))
                }
            },
        }
    }

    fn column_value<'a>(
        &self,
        column: &sql::Column,
        header: &[AbsoluteColumn],
        row: &'a Cow<Row>,
    ) -> Result<&'a Value> {
        match column {
            Column::Absolute(absolute) => {
                let sql::AbsoluteColumn {
                    table,
                    column: relative_column,
                } = absolute;

                let column = match relative_column {
                    sql::RelativeColumn::Ident(ident) => ident,
                    sql::RelativeColumn::Star(_) => {
                        return Err(Error::StarInWhereClause {
                            table_name: Some(table.clone()),
                        })
                    }
                };

                let idx = header
                    .iter()
                    .enumerate()
                    .find(|(_, header_col)| {
                        &header_col.table == table
                            && &header_col.column == column
                    })
                    .map(|(idx, _)| idx)
                    .ok_or_else(|| Error::UndefinedColumn {
                        table_name: Some(table.clone()),
                        column_name: column.clone(),
                    })?;

                Ok(&row.values[idx])
            }
            Column::Relative(relative) => {
                let column = match relative {
                    sql::RelativeColumn::Ident(ident) => ident,
                    sql::RelativeColumn::Star(_) => {
                        return Err(Error::StarInWhereClause {
                            table_name: None,
                        })
                    }
                };

                let idxs = header
                    .iter()
                    .enumerate()
                    .filter(|(_, header_col)| column == &header_col.column)
                    .map(|(idx, _)| idx)
                    .collect::<Vec<_>>();

                match idxs.len() {
                    0 => {
                        return Err(Error::UndefinedColumn {
                            table_name: None,
                            column_name: column.clone(),
                        })
                    }
                    1 => {
                        let idx = idxs[0];
                        Ok(&row.values[idx])
                    }
                    _ => todo!("unambiguous relative column projection"),
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Table {
    pub name: Ident,
    pub rows: Vec<usize>,
    pub schema: schema::Table,
    prev_id: AtomicI64,
}

impl Table {
    fn next_id(&self) -> i64 {
        let next_id = self.prev_id.fetch_add(1, Ordering::SeqCst);
        next_id + 1
    }
}

impl Id for Table {
    type Id = Ident;

    fn id(&self) -> Self::Id {
        self.name.id()
    }
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
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

impl From<i64> for Value {
    fn from(x: i64) -> Value {
        Value::Int(x)
    }
}

impl<'a> From<&'a str> for Value {
    fn from(x: &'a str) -> Value {
        Value::String(x.to_string())
    }
}

#[cfg(test)]
impl PartialEq<i64> for &Value {
    fn eq(&self, other: &i64) -> bool {
        &&Value::from(*other) == self
    }
}

#[cfg(test)]
impl<'a> PartialEq<&'a str> for Value {
    fn eq(&self, other: &&'a str) -> bool {
        &Value::from(*other) == self
    }
}

#[derive(Debug, Default)]
pub struct Projection<'a> {
    pub header: Vec<AbsoluteColumn>,
    pub rows: Vec<Cow<'a, Row>>,
}

impl<'a> Projection<'a> {
    fn extend_header_with_columns_from_table(&mut self, table: &Table) {
        let columns = table.schema.columns().into_iter().map(|(ident, _)| {
            AbsoluteColumn {
                table: table.name.clone(),
                column: ident.clone(),
            }
        });
        self.header.extend(columns);
    }

    fn extend_rows<I>(&mut self, rows: I)
    where
        I: IntoIterator<Item = &'a Row>,
    {
        self.rows.extend(rows.into_iter().map(Cow::Borrowed));
    }

    #[cfg(test)]
    pub fn to_tuples(self) -> Vec<(AbsoluteColumn, Value)> {
        let mut acc = vec![];
        for row in self.rows {
            let row = row.into_owned();
            for (idx, value) in row.values.into_iter().enumerate() {
                let column = self.header[idx].clone();
                acc.push((column, value));
            }
        }
        acc
    }

    pub fn tsv(&'a self) -> Tsv<'a> {
        Tsv(self)
    }
}

pub struct Tsv<'a>(&'a Projection<'a>);

impl<'a> fmt::Display for Tsv<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "{}",
            self.0
                .header
                .iter()
                .map(|c| format!("{}", c))
                .collect::<Vec<_>>()
                .join("\t")
        )?;

        let len = self.0.rows.len();
        for (idx, row) in self.0.rows.iter().enumerate() {
            write!(
                f,
                "{}",
                row.values
                    .iter()
                    .map(|c| format!("{}", c))
                    .collect::<Vec<_>>()
                    .join("\t")
            )?;

            if !(idx == len - 1) {
                write!(f, "\n")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AbsoluteColumn {
    pub table: Ident,
    pub column: Ident,
}

impl fmt::Display for AbsoluteColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.table, self.column)
    }
}

#[cfg(test)]
impl<'a> From<&'a str> for AbsoluteColumn {
    fn from(other: &'a str) -> Self {
        let mut parts = other.split(".").collect::<Vec<_>>();
        let table = Ident::new(parts.remove(0));
        let column = Ident::new(parts.remove(0));
        assert!(parts.is_empty());
        AbsoluteColumn { table, column }
    }
}
