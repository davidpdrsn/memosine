use crate::schema::Type;
use parse::*;
use std::collections::HashMap;
use std::fmt;

mod parse;

pub use parse::ParseError;

pub fn parse_sql_query(query: &str) -> Result<Statement, ParseError> {
    parse(&Statement::parser(), query)
}

pub fn parse_sql_queries(query: &str) -> Result<Vec<Statement>, ParseError> {
    parse(&many(Statement::parser()), query)
}

static KEYWORDS: &[&'static str] = &["select", "from"];

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

        let source_parser = string("from").whitespace().zip_right(Source::parser());

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
            Source::Query { query, alias } => write!(f, "({}) {}", query, alias),
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

    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
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
    Relative(SelectedColumn),
    Absolute {
        table: Ident,
        column: SelectedColumn,
    },
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Column::Relative(inner) => write!(f, "{}", inner),
            Column::Absolute { table, column } => write!(f, "{}.{}", table, column),
        }
    }
}

impl Column {
    fn parser() -> impl Parser<Output = Self> {
        let relative = SelectedColumn::parser().map(|ident| Column::Relative(ident));

        let absolute = Ident::parser()
            .zip_left(char('.'))
            .zip(SelectedColumn::parser())
            .map(|(table, column)| Column::Absolute { table, column });

        absolute.or(relative)
    }
}

#[derive(Debug, Clone)]
pub enum SelectedColumn {
    Ident(Ident),
    Star(Star),
}

impl fmt::Display for SelectedColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SelectedColumn::Ident(inner) => write!(f, "{}", inner),
            SelectedColumn::Star(inner) => write!(f, "{}", inner),
        }
    }
}

impl SelectedColumn {
    fn parser() -> impl Parser<Output = Self> {
        let ident = Ident::parser().map(|ident| SelectedColumn::Ident(ident));
        let star = char('*').map(|_| SelectedColumn::Star(Star));
        ident.or(star)
    }
}

#[derive(Debug, Clone, Copy)]
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
    fn new(value: &str) -> Self {
        Ident {
            value: value.to_string(),
        }
    }

    fn parser() -> impl Parser<Output = Self> {
        let ident_char =
            any_char().when(|c| !c.is_whitespace() && (c.is_alphanumeric() || c == &'_'));

        many1(ident_char)
            .map(|chars| chars.iter().collect::<String>())
            .when(|value| !KEYWORDS.contains(&value.as_str()))
            .map(|value| Ident { value })
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

    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        {
            let operator = |op: StringParser, f: fn(Expr, Expr) -> WhereClause| {
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
    Literal(Literal),
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Column(inner) => write!(f, "{}", inner),
            Expr::Literal(inner) => write!(f, "{}", inner),
        }
    }
}

impl Expr {
    fn parser() -> impl Parser<Output = Self> {
        let column = Column::parser().map(|col| Expr::Column(col));
        let literal = Literal::parser().map(|lit| Expr::Literal(lit));

        column.or(literal)
    }
}

#[derive(Debug, Clone)]
pub enum Literal {
    Int(i64),
    String(String),
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Literal::Int(inner) => write!(f, "{}", inner),
            Literal::String(inner) => write!(f, "{:?}", inner),
        }
    }
}

impl Literal {
    fn parser() -> impl Parser<Output = Self> {
        let int = many1(any_char().when(|c| c.is_numeric())).map(|digits| {
            let digits = digits.iter().collect::<String>().parse().unwrap();
            Literal::Int(digits)
        });

        let string = char('"')
            .zip_right(many(any_char().when(|c| c != &'"')))
            .zip_left(char('"'))
            .map(|chars| chars.iter().collect::<String>())
            .map(|s| Literal::String(s));

        int.or(string).whitespace()
    }
}

#[derive(Debug, Clone)]
pub struct CreateTable {
    name: Ident,
    id: (Ident, Type),
    columns: HashMap<Ident, Type>,
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
                let mut columns = columns.into_iter().collect::<HashMap<_, _>>();
                let id_ident = Ident::new("id");
                if let Some(id_ty) = columns.remove(&id_ident) {
                    Either::A(pure(CreateTable {
                        name,
                        columns,
                        id: (id_ident, id_ty),
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
    table: Ident,
    values: Vec<(Ident, Literal)>,
}

impl Insert {
    fn parser() -> impl Parser<Output = Self> {
        let value_parser = Ident::parser()
            .whitespace()
            .zip_left(char('='))
            .whitespace()
            .zip(Literal::parser());

        string("insert")
            .whitespace()
            .zip_right(string("into"))
            .whitespace()
            .zip_right(Ident::parser())
            .whitespace()
            .zip_left(char('('))
            .whitespace()
            .zip(value_parser.sep_by_allow_trailing(char(',').whitespace()))
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
