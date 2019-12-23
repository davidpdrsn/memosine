use parse::*;
use std::fmt;

mod parse;

pub use parse::parse;

static KEYWORDS: &[&'static str] = &["select", "from"];

#[derive(Debug)]
pub enum Statement {
    Select(Select),
    // Insert(Insert),
    // Update(Update),
    // Delete(Delete),
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Statement::Select(inner) => write!(f, "{}", inner),
        }
    }
}

impl Statement {
    pub fn parser() -> impl Parser<Output = Self> {
        let parser = Select::parser().map(Statement::Select);

        whitespace().zip_right(parser).zip_left(whitespace())
    }
}

#[derive(Debug)]
pub struct Select {
    pub columns: Vec<Column>,
    pub source: Source,
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

        Ok(())
    }
}

impl Select {
    fn parser() -> impl Parser<Output = Self> {
        let columns_parser = string("select")
            .whitespace()
            .zip_right(list_of(Column::parser()))
            .whitespace();

        let source_parser = string("from").whitespace().zip_right(Source::parser());

        let where_clause_parser = maybe(whitespace())
            .zip(string("where"))
            .whitespace()
            .zip_right(WhereClause::parser());

        columns_parser
            .zip(source_parser)
            .zip(maybe(where_clause_parser))
            .map(|((columns, source), where_clause)| Select {
                columns: columns.clone(),
                source,
                where_clause,
            })
    }
}

#[derive(Debug)]
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
pub enum Column {
    Relative(ColumnIdent),
    Absolute { table: Ident, column: ColumnIdent },
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
        let relative = ColumnIdent::parser().map(|ident| Column::Relative(ident));

        let absolute = Ident::parser()
            .zip_left(char('.'))
            .zip(ColumnIdent::parser())
            .map(|(table, column)| Column::Absolute { table, column });

        absolute.or(relative)
    }
}

#[derive(Debug, Clone)]
pub enum ColumnIdent {
    Ident(Ident),
    Star(Star),
}

impl fmt::Display for ColumnIdent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ColumnIdent::Ident(inner) => write!(f, "{}", inner),
            ColumnIdent::Star(inner) => write!(f, "{}", inner),
        }
    }
}

impl ColumnIdent {
    fn parser() -> impl Parser<Output = Self> {
        let ident = Ident::parser().map(|ident| ColumnIdent::Ident(ident));
        let star = char('*').map(|_| ColumnIdent::Star(Star));
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

#[derive(Debug, Clone)]
pub struct Ident {
    value: String,
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl Ident {
    fn parser() -> impl Parser<Output = Self> {
        let ident_char = any_char().when(|c| !c.is_whitespace() && c.is_alphanumeric());

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
    Eq(Expr, Expr),
    And(Box<WhereClause>, Box<WhereClause>),
    Or(Box<WhereClause>, Box<WhereClause>),
}

impl fmt::Display for WhereClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WhereClause::Grouped(inner) => write!(f, "({})", inner),
            WhereClause::Eq(lhs, rhs) => write!(f, "{} = {}", lhs, rhs),
            WhereClause::And(lhs, rhs) => write!(f, "{} and {}", lhs, rhs),
            WhereClause::Or(lhs, rhs) => write!(f, "{} or {}", lhs, rhs),
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
            let eq = Expr::parser()
                .whitespace()
                .zip_left(char('='))
                .whitespace()
                .zip(Expr::parser())
                .zip_left(maybe(whitespace()))
                .map(|(lhs, rhs)| WhereClause::Eq(lhs, rhs));

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

            (eq.or(grouped))
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
    Int(i32),
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Literal::Int(inner) => write!(f, "{}", inner),
        }
    }
}

impl Literal {
    fn parser() -> impl Parser<Output = Self> {
        let int = many1(any_char().when(|c| c.is_numeric())).map(|digits| {
            let digits = digits.iter().collect::<String>().parse().unwrap();
            Literal::Int(digits)
        });

        int
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

    fn test_parse(query: &str) -> Statement {
        parse(&Statement::parser(), query).unwrap_or_else(|e| panic!("{}", e))
    }
}
