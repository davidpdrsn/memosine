use crate::schema::Type;
use crate::utils::Either;
use std::collections::HashMap;
use std::fmt;
use crate::utils::parse::*;

use crate::database::Value;

pub fn parse_sql_query<'a>(query: &'a str) -> Result<Statement, ParseError<'a>> {
    parse(&Statement::parser(), query)
}

pub fn parse_sql_queries<'a>(query: &'a str) -> Result<Vec<Statement>, ParseError<'a>> {
    parse(&many(Statement::parser()), query)
}

static KEYWORDS: &[&str] = &[
    "select", "from", "join", "inner", "outer", "where", "and", "or",
];

#[derive(Debug)]
pub enum Statement {
    Select(Select),
    Insert(Insert),
    // Update(Update),
    // Delete(Delete),
    CreateTable(CreateTable),
    // DropTable(DropTable),
    // AlterTable(AlterTable),
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Statement::Select(inner) => write!(f, "{}", inner)?,
            Statement::CreateTable(inner) => write!(f, "{}", inner)?,
            Statement::Insert(inner) => write!(f, "{}", inner)?,
        }

        write!(f, ";")?;

        Ok(())
    }
}

impl Statement {
    pub fn parser() -> impl Parser<Output = Self> {
        let select = Select::parser().map(Statement::Select);
        let create_table = CreateTable::parser().map(Statement::CreateTable);
        let insert = Insert::parser().map(Statement::Insert);

        let parser = select.or(create_table).or(insert);

        whitespace()
            .zip_right(parser)
            .zip_left(maybe(whitespace()))
            .zip_left(maybe(char(';')))
            .zip_left(maybe(whitespace()))
    }
}

#[derive(Debug, Clone)]
pub struct Select {
    pub columns: Vec<Column>,
    pub source: Source,
    pub joins: Vec<Join>,
    pub where_clause: Option<WhereClause>,
}

impl fmt::Display for Select {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let columns = self
            .columns
            .iter()
            .map(|c| format!("{}", c))
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "select {} from {}", columns, self.source)?;

        if let Some(where_clause) = &self.where_clause {
            write!(f, " where {}", where_clause)?;
        }

        for join in &self.joins {
            write!(f, " {}", join)?;
        }

        Ok(())
    }
}

impl Select {
    fn parser() -> impl Parser<Output = Self> {
        let columns_parser = string("select")
            .whitespace()
            .zip_right(list_of(Column::parser()));

        let source_parser =
            string("from").whitespace().zip_right(Source::parser());

        let joins_parser = many(Join::parser());

        let where_clause_parser = string("where")
            .whitespace()
            .zip_right(WhereClause::parser());

        columns_parser
            .zip_left(whitespace())
            .zip(source_parser)
            .zip_left(whitespace())
            .zip(joins_parser)
            .zip_left(whitespace())
            .zip(maybe(where_clause_parser))
            .map(|(((columns, source), joins), where_clause)| Select {
                columns: columns.clone(),
                source,
                joins,
                where_clause,
            })
    }
}

#[derive(Debug, Clone)]
pub enum Source {
    Table(Ident),
    Query { query: Box<Select>, alias: Ident },
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Source::Table(inner) => write!(f, "{}", inner),
            Source::Query { query, alias } => {
                write!(f, "({}) {}", query, alias)
            }
        }
    }
}

impl Source {
    fn parser() -> impl Parser<Output = Self> {
        SourceParser
    }
}

#[derive(Debug)]
struct SourceParser;

impl Parser for SourceParser {
    type Output = Source;

    fn parse<'a>(
        &self,
        input: &'a str,
        pos: Pos,
    ) -> ParseResult<'a, Self::Output> {
        let parser = {
            let table = Ident::parser().map(|ident| Source::Table(ident));

            let query = char('(')
                .whitespace()
                .zip_right(Select::parser())
                .zip_left(char(')'))
                .whitespace()
                .zip(Ident::parser())
                .map(|(select, alias)| Source::Query {
                    query: Box::new(select),
                    alias,
                });

            table.or(query)
        };

        parser.parse(input, pos)
    }
}

#[derive(Debug, Clone)]
pub struct Join {
    pub ty: JoinType,
    pub source: Source,
    pub where_clause: WhereClause,
}

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.ty {
            JoinType::Inner => write!(f, "inner ")?,
            JoinType::Outer => write!(f, "outer ")?,
        }

        write!(f, "join {} on {}", self.source, self.where_clause)?;

        Ok(())
    }
}

impl Join {
    fn parser() -> impl Parser<Output = Self> {
        let inner = string("inner").map(|_| JoinType::Inner);
        let outer = string("outer").map(|_| JoinType::Outer);

        (inner.or(outer))
            .whitespace()
            .zip_left(string("join"))
            .whitespace()
            .zip(Source::parser())
            .whitespace()
            .zip_left(string("on"))
            .whitespace()
            .zip(WhereClause::parser())
            .map(|((ty, source), where_clause)| Join {
                ty,
                source,
                where_clause,
            })
    }
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Outer,
}

#[derive(Debug, Clone)]
pub struct OuterJoin {
    source: Source,
    where_clause: WhereClause,
}

#[derive(Debug, Clone)]
pub enum Column {
    Relative(RelativeColumn),
    Absolute(AbsoluteColumn),
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Column::Relative(inner) => write!(f, "{}", inner),
            Column::Absolute(inner) => write!(f, "{}", inner),
        }
    }
}

impl Column {
    fn parser() -> impl Parser<Output = Self> {
        let relative =
            RelativeColumn::parser().map(|ident| Column::Relative(ident));

        let absolute = Ident::parser()
            .zip_left(char('.'))
            .zip(RelativeColumn::parser())
            .map(|(table, column)| {
                Column::Absolute(AbsoluteColumn { table, column })
            });

        absolute.or(relative)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AbsoluteColumn {
    pub table: Ident,
    pub column: RelativeColumn,
}

impl fmt::Display for AbsoluteColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.table, self.column)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RelativeColumn {
    Ident(Ident),
    Star(Star),
}

impl fmt::Display for RelativeColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RelativeColumn::Ident(inner) => write!(f, "{}", inner),
            RelativeColumn::Star(inner) => write!(f, "{}", inner),
        }
    }
}

impl RelativeColumn {
    fn parser() -> impl Parser<Output = Self> {
        let ident = Ident::parser().map(|ident| RelativeColumn::Ident(ident));
        let star = char('*').map(|_| RelativeColumn::Star(Star));
        ident.or(star)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Star;

impl fmt::Display for Star {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "*")
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct Ident {
    value: String,
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl Ident {
    pub fn new(value: &str) -> Self {
        Ident {
            value: value.to_string(),
        }
    }

    fn parser() -> impl Parser<Output = Self> {
        let ident_char = any_char()
            .when(|c| !c.is_whitespace() && (c.is_alphanumeric() || c == &'_'));

        many1(ident_char)
            .map(|chars| chars.iter().collect::<String>())
            .when(|value| !KEYWORDS.contains(&value.as_str()))
            .map(|value| Ident { value })
    }
}

impl PartialEq<str> for Ident {
    fn eq(&self, other: &str) -> bool {
        self.value == other
    }
}

impl<'a> PartialEq<str> for &'a Ident {
    fn eq(&self, other: &str) -> bool {
        self.value == other
    }
}

fn list_of<P: Parser>(p: P) -> impl Parser<Output = Vec<P::Output>> {
    p.sep_by_allow_trailing(char(',').whitespace())
}

#[derive(Debug, Clone)]
pub enum WhereClause {
    Grouped(Box<WhereClause>),
    And(Box<WhereClause>, Box<WhereClause>),
    Or(Box<WhereClause>, Box<WhereClause>),

    Eq(Expr, Expr),
    NotEq(Expr, Expr),
    Lt(Expr, Expr),
    Gt(Expr, Expr),
    LtEq(Expr, Expr),
    GtEq(Expr, Expr),
}

impl fmt::Display for WhereClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WhereClause::Grouped(inner) => write!(f, "({})", inner),
            WhereClause::And(lhs, rhs) => write!(f, "{} and {}", lhs, rhs),
            WhereClause::Or(lhs, rhs) => write!(f, "{} or {}", lhs, rhs),

            WhereClause::Eq(lhs, rhs) => write!(f, "{} = {}", lhs, rhs),
            WhereClause::NotEq(lhs, rhs) => write!(f, "{} != {}", lhs, rhs),
            WhereClause::Lt(lhs, rhs) => write!(f, "{} < {}", lhs, rhs),
            WhereClause::Gt(lhs, rhs) => write!(f, "{} > {}", lhs, rhs),
            WhereClause::LtEq(lhs, rhs) => write!(f, "{} <= {}", lhs, rhs),
            WhereClause::GtEq(lhs, rhs) => write!(f, "{} >= {}", lhs, rhs),
        }
    }
}

impl WhereClause {
    #[inline]
    fn parser() -> impl Parser<Output = Self> {
        WhereClauseParser
    }
}

struct WhereClauseParser;

impl Parser for WhereClauseParser {
    type Output = WhereClause;

    fn parse<'a>(
        &self,
        input: &'a str,
        pos: Pos,
    ) -> ParseResult<'a, Self::Output> {
        {
            let operator =
                |op: StringParser, f: fn(Expr, Expr) -> WhereClause| {
                    Expr::parser()
                        .whitespace()
                        .zip_left(op)
                        .whitespace()
                        .zip(Expr::parser())
                        .zip_left(maybe(whitespace()))
                        .map(move |(lhs, rhs)| f(lhs, rhs))
                };

            let eq = operator(string("="), WhereClause::Eq);
            let not_eq = operator(string("!="), WhereClause::NotEq);
            let lt = operator(string("<"), WhereClause::Lt);
            let gt = operator(string(">"), WhereClause::Gt);
            let lt_eq = operator(string("<="), WhereClause::LtEq);
            let gt_eq = operator(string(">="), WhereClause::GtEq);

            let lhs_rhs_parser = |s| {
                whitespace()
                    .zip_right(string(s))
                    .whitespace()
                    .zip_right(WhereClause::parser())
            };

            let and = lhs_rhs_parser("and");
            let or = lhs_rhs_parser("or");

            let grouped = char('(')
                .whitespace()
                .zip_right(WhereClause::parser())
                .zip_left(char(')'))
                .zip_left(maybe(whitespace()))
                .map(|wc| WhereClause::Grouped(Box::new(wc)));

            let base = eq.or(not_eq).or(lt).or(gt).or(lt_eq).or(gt_eq);

            (base.or(grouped))
                .zip(maybe(and))
                .map(|(lhs, rhs)| match rhs {
                    Some(rhs) => WhereClause::And(Box::new(lhs), Box::new(rhs)),
                    None => lhs,
                })
                .zip(maybe(or))
                .map(|(lhs, rhs)| match rhs {
                    Some(rhs) => WhereClause::Or(Box::new(lhs), Box::new(rhs)),
                    None => lhs,
                })
        }
        .parse(input, pos)
    }
}

#[derive(Debug, Clone)]
pub enum Expr {
    Column(Column),
    Value(Value),
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Column(inner) => write!(f, "{}", inner),
            Expr::Value(inner) => write!(f, "{}", inner),
        }
    }
}

impl Expr {
    fn parser() -> impl Parser<Output = Self> {
        let column = Column::parser().map(|col| Expr::Column(col));
        let value = Value::parser().map(|lit| Expr::Value(lit));

        column.or(value)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Int(inner) => write!(f, "{}", inner),
            Value::String(inner) => write!(f, "{:?}", inner),
        }
    }
}

impl Value {
    fn parser() -> impl Parser<Output = Self> {
        let int = many1(any_char().when(|c| c.is_numeric())).map(|digits| {
            let digits = digits.iter().collect::<String>().parse().unwrap();
            Value::Int(digits)
        });

        let string = char('"')
            .zip_right(many(any_char().when(|c| c != &'"')))
            .zip_left(char('"'))
            .map(|chars| chars.iter().collect::<String>())
            .map(|s| Value::String(s));

        int.or(string).whitespace()
    }
}

#[derive(Debug, Clone)]
pub struct CreateTable {
    pub name: Ident,
    pub id: (Ident, Type),
    pub columns: Vec<(Ident, Type)>,
}

impl fmt::Display for CreateTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "create table {} (", self.name)?;
        for (col, ty) in &self.columns {
            writeln!(f, "  {} {},", col, ty)?;
        }
        write!(f, ")")?;

        Ok(())
    }
}

impl CreateTable {
    fn parser() -> impl Parser<Output = Self> {
        let column_parser = Ident::parser().whitespace().zip(Type::parser());

        string("create")
            .whitespace()
            .zip_right(string("table"))
            .whitespace()
            .zip_right(Ident::parser())
            .whitespace()
            .zip_left(char('('))
            .whitespace()
            .zip(column_parser.sep_by_allow_trailing(char(',').whitespace()))
            .zip_left(char(')'))
            .and_then(|(name, columns)| {
                let mut columns = columns.into_iter().collect::<Vec<_>>();

                let id_ident = Ident::new("id");
                let mut id_ty = None;
                let mut non_id_columns = Vec::with_capacity(columns.len() - 1);

                for (name, ty) in columns.drain(..) {
                  if name == id_ident {
                    id_ty = Some(ty);
                  } else {
                    non_id_columns.push((name, ty));
                  }
                }

                if let Some(id_ty) = id_ty {
                    Either::A(pure(CreateTable {
                        name,
                        id: (id_ident, id_ty),
                        columns: non_id_columns,
                    }))
                } else {
                    Either::B(error(format!(
                        "Cannot create `{}` table since it doesn't have an id column",
                        name
                    )))
                }
            })
    }
}

impl Type {
    fn parser() -> impl Parser<Output = Self> {
        let named = Ident::parser().map(|id| Type::Named(id));
        let option = string("Option")
            .whitespace()
            .zip(char('<'))
            .whitespace()
            .zip_right(Ident::parser())
            .whitespace()
            .zip_left(char('>'))
            .map(|id| Type::Option(id));

        option.or(named).whitespace()
    }
}

#[derive(Debug, Clone)]
pub struct Insert {
    pub table: Ident,
    pub values: HashMap<Ident, Value>,
}

impl Insert {
    fn parser() -> impl Parser<Output = Self> {
        let value_parser = Ident::parser()
            .whitespace()
            .zip_left(char('='))
            .whitespace()
            .zip(Value::parser());

        string("insert")
            .whitespace()
            .zip_right(string("into"))
            .whitespace()
            .zip_right(Ident::parser())
            .whitespace()
            .zip_left(char('('))
            .whitespace()
            .zip(
                value_parser
                    .sep_by_allow_trailing(char(',').whitespace())
                    .map(|values| {
                        values.into_iter().collect::<HashMap<_, _>>()
                    }),
            )
            .zip_left(char(')'))
            .map(|(table, values)| Insert { table, values })
    }
}

impl fmt::Display for Insert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "insert into {}", self.table)?;

        let values = self
            .values
            .iter()
            .map(|(key, value)| format!("{} = {}", key, value))
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}", values)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn single_ident() {
        test_parse("select id from users");
        test_parse(" select id from users ");
    }

    #[test]
    fn multiple_idents() {
        test_parse("select id, name from users");
        test_parse("select id, name, from users");
    }

    #[test]
    fn absolute_selection_path() {
        test_parse("select users.id, name from users");
    }

    #[test]
    fn sub_query() {
        test_parse("select t.id from (select id from users) t");
    }

    #[test]
    fn star() {
        test_parse("select * from users");
    }

    #[test]
    fn absolute_star() {
        test_parse("select users.* from users");
    }

    #[test]
    fn simple_where() {
        test_parse("select id from users where id = 1");
        test_parse("select id from users where id = id");
        test_parse("select id from users where users.id = id");
        test_parse("select id from users where id = users.id");
    }

    #[test]
    fn compex_where_and() {
        test_parse("select id from users where id = 1 and 2 = 2");
        test_parse("select id from users where id = 1 and 2 = 2 and 3 = 3");
    }

    #[test]
    fn complex_where_or() {
        test_parse("select id from users where id = 1 or 2 = 2");
        test_parse("select id from users where id = 1 or 2 = 2 or 3 = 3");
    }

    #[test]
    fn nested_ands_and_ors() {
        test_parse("select id from users where id = 1 and 2 = 2 or 3 = 3");
        test_parse("select id from users where id = 1 or 2 = 2 and 3 = 3");
    }

    #[test]
    fn grouped_where() {
        test_parse("select id from users where (id = 1)");
        test_parse("select id from users where (id = 1) and id = 2");
        test_parse("select id from users where ((id = 1 ) and id = 2)");
    }

    #[test]
    fn inner_joins() {
        test_parse(
            r#"
                select users.id, countries.id
                from users
                inner join countries on countries.id = users.country_id
            "#,
        );

        test_parse(
            r#"
                select users.id, countries.id
                from users
                inner join countries on countries.id = users.country_id
                inner join claims on claims.user_id = users.id
            "#,
        );
    }

    #[test]
    fn outer_joins() {
        test_parse(
            r#"
                select users.id, countries.id
                from users
                outer join countries on countries.id = users.country_id
                outer join claims on claims.user_id = users.id
            "#,
        );
    }

    #[test]
    fn mixed_joins() {
        test_parse(
            r#"
                select users.id, countries.id
                from users
                inner join countries on countries.id = users.country_id
                outer join countries on countries.id = users.country_id
            "#,
        );
    }

    #[test]
    fn complex_query() {
        test_parse(
            r#"
                select *
                from (
                    select users.*
                    from users
                    inner join (select users.id from users) t on users.id = users.id
                    outer join users on users.id = users.id
                ) t
                outer join countries on countries.id = users.country_id
                inner join countries on countries.id = users.country_id
                inner join (select * from users) u on
                    countries.id = u.country_id
                    and (1 = 1 or 2 != 2)
                where
                    users.id = 1
                    and 1 = 1
                    or (2 < 1 and 1 <= users.id)
                    or (2 > 1 and 1 >= users.id)
            "#,
        );
    }

    #[test]
    fn create_table() {
        test_parse(
            r#"
                create table users (
                    id Int,
                    country_id Option<Int>,
                    name String,
                )
            "#,
        );
    }

    #[test]
    fn insert_into() {
        test_parse(
            r#"
                insert into users (
                    name = "Bob",
                    age = 20,
                );
            "#,
        );
    }

    fn test_parse(query: &str) -> Statement {
        parse(&Statement::parser(), query).unwrap_or_else(|e| panic!("{}", e))
    }
}
