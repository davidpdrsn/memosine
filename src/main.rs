pub mod sql;

use sql::parse::*;

fn main() {
    let query = "select id from users";

    let parsed = parse(&Select::parser(), query).unwrap_or_else(|e| panic!("{}", e));
    println!("{:?}", parsed);
}

#[derive(Debug)]
struct Select {
    columns: Vec<Ident>,
    from: Ident,
}

impl Select {
    fn parser() -> impl Parser<Output = Self> {
        string("select")
            .and_then(|_| Ident::parser())
            .and_then(|column| {
                string("from")
                    .and_then(|_| Ident::parser())
                    .map(move |from| Select {
                        columns: vec![column.clone()],
                        from,
                    })
            })
    }
}

#[derive(Debug, Clone)]
struct Ident {
    name: String,
}

impl Ident {
    fn parser() -> impl Parser<Output = Self> {
        let non_whitespace = any_char().when(|c| !c.is_whitespace());

        many(non_whitespace).map(|s| Ident {
            name: s.iter().collect(),
        })
    }
}
