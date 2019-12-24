use std::collections::HashMap;
use std::fmt;
use crate::sql::Ident;

#[derive(Debug)]
struct Schema {
    tables: HashMap<Ident, Table>,
}

#[derive(Debug)]
struct Table {
    name: Ident,
    id: (Ident, Type),
    columns: HashMap<Ident, Type>,
}

#[derive(Debug, Clone)]
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
