pub mod sql;

use sql::parse::*;

fn main() {
    let query = "select id, name, from users";

    let parsed = parse(&Select::parser(), query).unwrap_or_else(|e| panic!("{}", e));
    println!("{:?}", parsed);
}

static KEYWORDS: &[&'static str] = &["select", "from"];

#[derive(Debug)]
struct Select {
    columns: Vec<Ident>,
    source: Ident,
}

impl Select {
    fn parser() -> impl Parser<Output = Self> {
        string("select")
            .whitespace()
            .zip_right(Ident::list_parser())
            .whitespace()
            .and_then(|columns| {
                string("from")
                    .whitespace()
                    .zip_right(Ident::parser())
                    .map(move |source| Select {
                        columns: columns.clone(),
                        source,
                    })
            })
    }
}

#[derive(Debug, Clone)]
struct Ident {
    value: String,
}

impl Ident {
    fn parser() -> impl Parser<Output = Self> {
        let ident_char = any_char().when(|c| !c.is_whitespace() && c.is_alphanumeric());

        many(ident_char)
            .map(|chars| chars.iter().collect::<String>())
            .when(|value| !KEYWORDS.contains(&value.as_str()))
            .map(|value| Ident { value })
    }

    fn list_parser() -> impl Parser<Output = Vec<Self>> {
        Self::parser()
            .tap_print_debug()
            .sep_by_allow_trailing(char(',').whitespace())
    }
}
