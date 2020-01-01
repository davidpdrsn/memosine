use crate::database::Value;
use crate::error::{Error, Result};
use crate::sql::{CreateTable, Ident};
use crate::utils::{Id, IdMap};
use std::collections::HashMap;
use std::fmt;

#[derive(Debug)]
pub struct Schema {
    tables: IdMap<Table>,
}

impl Schema {
    pub fn get_table(&self, name: &Ident) -> Result<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| Error::UndefinedTable { name: name.clone() })
    }
}

#[derive(Debug)]
pub struct Table {
    name: Ident,
    id: (Ident, Type),
    columns: Vec<(Ident, Type)>,
}

impl Id for Table {
    type Id = Ident;

    fn id(&self) -> Self::Id {
        self.name.id()
    }
}

impl Table {
    pub fn create(stmt: CreateTable) -> Self {
        let table = Table {
            name: stmt.name,
            id: stmt.id,
            columns: stmt.columns.into_iter().collect(),
        };

        table
    }

    pub fn has_column(&self, name: &Ident) -> bool {
        self.columns.iter().any(|(key, _)| key == name)
    }

    pub fn type_of_column(&self, column: &Ident) -> Result<&Type> {
        if column == &self.id.0 {
            Ok(&self.id.1)
        } else {
            self.columns
                .iter()
                .find(|(key, _)| key == column)
                .map(|(_, v)| v)
                .ok_or_else(|| Error::UndefinedColumn {
                    table_name: Some(self.name.clone()),
                    column_name: column.clone(),
                })
        }
    }

    pub fn columns(&self) -> Vec<(&Ident, &Type)> {
        let mut map = Vec::default();
        map.push((&self.id.0, &self.id.1));
        for (key, value) in &self.columns {
            map.push((key, value));
        }
        map
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Type {
    Named(Ident),
    Option(Ident),
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Type::Named(ident) => write!(f, "{}", ident),
            Type::Option(ty) => write!(f, "Option<{}>", ty),
        }
    }
}

#[cfg(test)]
mod test {
    #[allow(unused_imports)]
    use super::*;
    use crate::tests::*;

    #[test]
    fn schema_is_consistent() {
        for _ in 0..100 {
            let db = setup();
            let users_table = db.get_table(&Ident::new("users")).unwrap();
            let columns = &users_table.schema.columns();
            assert_eq!(columns.len(), 4);
            assert_eq!(
                &columns[0],
                &(&Ident::new("id"), &Type::Named(Ident::new("Int")))
            );
            assert_eq!(
                &columns[1],
                &(&Ident::new("name"), &Type::Named(Ident::new("String")))
            );
            assert_eq!(
                &columns[2],
                &(&Ident::new("age"), &Type::Named(Ident::new("Int")))
            );
            assert_eq!(
                &columns[3],
                &(&Ident::new("country_id"), &Type::Named(Ident::new("Int")))
            );
        }
    }
}
