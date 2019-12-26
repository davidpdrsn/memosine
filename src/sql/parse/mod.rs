#![allow(dead_code)]

use crate::utils::Either;
use std::fmt;
use std::marker::PhantomData;

mod pos;

pub use pos::Pos;

pub fn parse<'a, P>(parser: &P, input: &'a str) -> Result<P::Output, ParseError<'a>>
where
    P: Parser,
{
    match parser.parse(input, Pos::default()) {
        Ok((out, pos)) => {
            if pos.value() == input.len() {
                Ok(out)
            } else {
                Err(ParseError::UnexpectedEndOfInput { pos, input })
            }
        }
        Err(err) => Err(err),
    }
}

pub type ParseResult<'a, T> = std::result::Result<(T, Pos), ParseError<'a>>;

pub trait Parser: Sized {
    type Output;

    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output>;

    #[inline]
    fn map<F, T>(self, f: F) -> Map<Self, F>
    where
        F: Fn(Self::Output) -> T,
    {
        Map(self, f)
    }

    // TODO: filter_map combinator
    // TODO: any combinator. From list of parsers to first match

    #[inline]
    fn and_then<F, P2>(self, f: F) -> AndThen<Self, F>
    where
        P2: Parser,
        F: Fn(Self::Output) -> P2,
    {
        AndThen(self, f)
    }

    #[inline]
    fn or<P: Parser>(self, other: P) -> Or<Self, P> {
        Or(self, other)
    }

    #[inline]
    fn zip<P>(self, other: P) -> Zip<Self, P>
    where
        P: Parser,
    {
        Zip(self, other)
    }

    #[inline]
    fn zip_left<P>(self, other: P) -> ZipLeft<Self, P>
    where
        P: Parser,
    {
        ZipLeft(self, other)
    }

    #[inline]
    fn zip_right<P>(self, other: P) -> ZipRight<Self, P>
    where
        P: Parser,
    {
        ZipRight(self, other)
    }

    #[inline]
    fn times(self, n: usize) -> Times<Self> {
        Times(self, n)
    }

    #[inline]
    fn when<F>(self, predicate: F) -> When<Self, F>
    where
        F: Fn(&Self::Output) -> bool,
    {
        When(self, predicate)
    }

    #[inline]
    fn debug_print<T>(self, msg: T) -> DebugParser<Self>
    where
        T: Into<String>,
    {
        DebugParser(msg.into(), self)
    }

    #[inline]
    fn named<T>(self, name: T) -> Named<Self>
    where
        T: Into<String>,
    {
        Named(name.into(), self)
    }

    #[inline]
    fn tap<F>(self, f: F) -> Tap<Self, F>
    where
        F: Fn(&Self::Output),
    {
        Tap(self, f)
    }

    #[inline]
    fn tap_print_debug(self) -> Tap<Self, Box<dyn Fn(&Self::Output)>>
    where
        Self::Output: std::fmt::Debug,
    {
        Tap(self, Box::new(|x| eprintln!("{:?}", x)))
    }

    #[inline]
    fn tap_print_display(self) -> Tap<Self, Box<dyn Fn(&Self::Output)>>
    where
        Self::Output: std::fmt::Display,
    {
        Tap(self, Box::new(|x| eprintln!("{}", x)))
    }

    #[inline]
    fn print_remaining_input(self) -> PrintRemainingInput<Self> {
        PrintRemainingInput(self)
    }

    #[inline]
    fn whitespace(self) -> Whitespace<Self> {
        Whitespace(self)
    }

    #[inline]
    fn sep_by<P>(self, sep: P) -> SepBy<Self, P>
    where
        P: Parser,
    {
        SepBy {
            item: self,
            sep,
            allow_trailing: false,
        }
    }

    #[inline]
    fn sep_by_allow_trailing<P>(self, sep: P) -> SepBy<Self, P>
    where
        P: Parser,
    {
        SepBy {
            item: self,
            sep,
            allow_trailing: true,
        }
    }
}

#[derive(Debug)]
pub enum ParseError<'a> {
    Message {
        msg: String,
        input: &'a str,
        pos: Pos,
    },
    Named {
        name: String,
        inner: Box<ParseError<'a>>,
    },
    UnexpectedEndOfInput {
        pos: Pos,
        input: &'a str,
    },
}

impl<'a> fmt::Display for ParseError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ParseError::*;

        match self {
            Named { name, inner } => write!(f, "\"{}\" failed with msg: {}", name, inner),
            Message { msg, input, pos } => {
                if let Some(string) = input.from(*pos) {
                    write!(f, "{} at {:?}", msg, string)
                } else {
                    write!(f, "{} at index {} of input", msg, pos.value())
                }
            }
            UnexpectedEndOfInput { pos, input } => {
                if let Some(string) = input.from(*pos) {
                    write!(f, "Unexpected end of input at {:?}", string)
                } else {
                    write!(
                        f,
                        "Unexpected end of input at index {}. Input length is {}",
                        pos.value(),
                        input.len()
                    )
                }
            }
        }
    }
}

impl<'a> std::error::Error for ParseError<'a> {}

trait StringSliceExt<'a> {
    fn from(self, pos: Pos) -> Option<&'a str>;

    fn to(self, pos: Pos) -> Option<&'a str>;

    fn at(self, pos: Pos) -> Option<char>;
}

impl<'a> StringSliceExt<'a> for &'a str {
    fn from(self, pos: Pos) -> Option<&'a str> {
        let idx = pos.value();
        if idx < self.len() {
            Some(&self[idx..])
        } else {
            None
        }
    }

    fn to(self, pos: Pos) -> Option<&'a str> {
        let idx = pos.value();
        if idx < self.len() {
            Some(&self[0..idx])
        } else {
            None
        }
    }

    fn at(self, pos: Pos) -> Option<char> {
        let idx = pos.value();
        let bytes = self.as_bytes();
        assert_eq!(self.len(), bytes.len());

        if idx < bytes.len() {
            Some(bytes[idx] as char)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct DebugParser<P>(String, P);

impl<P: Parser> Parser for DebugParser<P> {
    type Output = P::Output;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        eprintln!("{}", self.0);
        self.1.parse(input, pos)
    }
}

#[derive(Debug, Clone)]
pub struct Named<P>(String, P);

impl<P: Parser> Parser for Named<P> {
    type Output = P::Output;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        match self.1.parse(input, pos) {
            Ok(x) => Ok(x),
            Err(err) => Err(ParseError::Named {
                name: self.0.to_string(),
                inner: Box::new(err),
            }),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Map<P, F>(P, F);

impl<P, F, T> Parser for Map<P, F>
where
    P: Parser,
    F: Fn(P::Output) -> T,
{
    type Output = T;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        let (out, pos) = self.0.parse(input, pos)?;
        Ok(((self.1)(out), pos))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct AndThen<P, F>(P, F);

impl<P, P2, F> Parser for AndThen<P, F>
where
    P: Parser,
    P2: Parser,
    F: Fn(P::Output) -> P2,
{
    type Output = P2::Output;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        let (output, new_pos) = self.0.parse(input, pos)?;
        let p2 = (self.1)(output);
        p2.parse(input, new_pos)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Or<P1, P2>(P1, P2);

impl<P1, P2, Out> Parser for Or<P1, P2>
where
    P1: Parser<Output = Out>,
    P2: Parser<Output = Out>,
{
    type Output = Out;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        match self.0.parse(input, pos) {
            Ok(x) => Ok(x),
            _ => self.1.parse(input, pos),
        }
    }
}

pub fn maybe<P: Parser>(parser: P) -> Maybe<P> {
    Maybe(parser)
}

#[derive(Debug, Copy, Clone)]
pub struct Maybe<P>(P);

impl<P: Parser> Parser for Maybe<P> {
    type Output = Option<P::Output>;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        match self.0.parse(input, pos) {
            Ok((out, new_pos)) => Ok((Some(out), new_pos)),
            Err(_) => Ok((None, pos)),
        }
    }
}

pub fn char(c: char) -> Char {
    Char(c)
}

#[derive(Debug, Copy, Clone)]
pub struct Char(char);

impl Parser for Char {
    type Output = char;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        let head = input
            .at(pos)
            .ok_or_else(|| ParseError::UnexpectedEndOfInput { pos, input })?;

        let c = char::from(head);

        if c == self.0 {
            Ok((c, pos + 1))
        } else {
            Err(ParseError::Message {
                msg: format!("Unexpected `{}`, expected `{}`", c, self.0),
                input,
                pos,
            })
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Zip<P1, P2>(P1, P2);

impl<P1, P2> Parser for Zip<P1, P2>
where
    P1: Parser,
    P2: Parser,
{
    type Output = (P1::Output, P2::Output);

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        let (a, new_pos) = self.0.parse(input, pos)?;
        let (b, new_new_pos) = self.1.parse(input, new_pos)?;
        Ok(((a, b), new_new_pos))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ZipLeft<P1, P2>(P1, P2);

impl<P1, P2> Parser for ZipLeft<P1, P2>
where
    P1: Parser,
    P2: Parser,
{
    type Output = P1::Output;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        let (a, new_pos) = self.0.parse(input, pos)?;
        let (_, new_new_pos) = self.1.parse(input, new_pos)?;
        Ok((a, new_new_pos))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ZipRight<P1, P2>(P1, P2);

impl<P1, P2> Parser for ZipRight<P1, P2>
where
    P1: Parser,
    P2: Parser,
{
    type Output = P2::Output;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        let (_, new_pos) = self.0.parse(input, pos)?;
        let (b, new_new_pos) = self.1.parse(input, new_pos)?;
        Ok((b, new_new_pos))
    }
}

pub fn sequence<P>(parsers: Vec<P>) -> Sequence<P>
where
    P: Parser,
{
    Sequence(parsers)
}

#[derive(Debug, Clone)]
pub struct Sequence<P>(Vec<P>);

impl<P> Parser for Sequence<P>
where
    P: Parser,
{
    type Output = Vec<P::Output>;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        let init = Ok((Vec::<P::Output>::new(), pos));
        self.0.iter().fold(init, |acc, parser| {
            let (mut items, pos) = acc?;
            let (item, new_pos) = parser.parse(input, pos)?;
            items.push(item);
            Ok((items, new_pos))
        })
    }
}

#[inline]
pub fn pure<T>(value: T) -> Pure<T>
where
    T: Clone,
{
    Pure(value)
}

#[derive(Debug, Copy, Clone)]
pub struct Pure<T>(T);

impl<T> Parser for Pure<T>
where
    T: Clone,
{
    type Output = T;

    #[inline]
    fn parse<'a>(&self, _: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        Ok((self.0.clone(), pos))
    }
}

pub fn string(s: &str) -> StringParser {
    StringParser(s.to_string())
}

#[derive(Debug, Clone)]
pub struct StringParser(String);

impl Parser for StringParser {
    type Output = String;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        if let Some(sub_input) = input.from(pos) {
            if sub_input.starts_with(&self.0) {
                Ok((self.0.clone(), pos + self.0.len()))
            } else {
                Err(ParseError::Message {
                    msg: format!("Expected {:?}, found {:?}", self.0, sub_input),
                    pos,
                    input,
                })
            }
        } else {
            Err(ParseError::UnexpectedEndOfInput { pos, input })
        }
    }
}

pub fn many<P>(parser: P) -> Many<P>
where
    P: Parser,
{
    Many(parser)
}

#[derive(Debug, Clone, Copy)]
pub struct Many<P>(P);

impl<P: Parser> Parser for Many<P> {
    type Output = Vec<P::Output>;

    #[inline]
    fn parse<'a>(&self, input: &'a str, mut pos: Pos) -> ParseResult<'a, Self::Output> {
        let mut acc = vec![];

        loop {
            match self.0.parse(input, pos) {
                Ok((item, new_pos)) => {
                    pos = new_pos;
                    acc.push(item);
                }
                Err(_) => return Ok((acc, pos)),
            }
        }
    }
}

pub fn many1<P>(parser: P) -> Many1<P>
where
    P: Parser,
{
    Many1(parser)
}

#[derive(Debug, Clone, Copy)]
pub struct Many1<P>(P);

impl<P: Parser> Parser for Many1<P> {
    type Output = Vec<P::Output>;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        let (item, mut pos) = self.0.parse(input, pos)?;
        let mut acc = vec![item];
        loop {
            match self.0.parse(input, pos) {
                Ok((item, new_pos)) => {
                    pos = new_pos;
                    acc.push(item);
                }
                Err(_) => return Ok((acc, pos)),
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Times<P>(P, usize);

impl<P: Parser> Parser for Times<P> {
    type Output = Vec<P::Output>;

    #[inline]
    fn parse<'a>(&self, input: &'a str, mut pos: Pos) -> ParseResult<'a, Self::Output> {
        let mut n = self.1;
        let mut acc = vec![];

        loop {
            if n == 0 {
                return Ok((acc, pos));
            }

            let (item, new_pos) = self.0.parse(input, pos)?;
            pos = new_pos;
            acc.push(item);
            n -= 1;
        }
    }
}

pub fn any_char() -> AnyChar {
    AnyChar
}

#[derive(Debug, Copy, Clone)]
pub struct AnyChar;

impl Parser for AnyChar {
    type Output = char;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        let head = input
            .at(pos)
            .ok_or_else(|| ParseError::UnexpectedEndOfInput { pos, input })?;
        let c = char::from(head);
        Ok((c, pos + 1))
    }
}

pub fn whitespace() -> impl Parser<Output = Vec<char>> {
    many(any_char().when(|c| c.is_whitespace()))
}

#[derive(Debug, Copy, Clone)]
pub struct When<P, F>(P, F);

impl<P, F> Parser for When<P, F>
where
    P: Parser,
    F: Fn(&P::Output) -> bool,
{
    type Output = P::Output;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        let (out, new_pos) = self.0.parse(input, pos)?;
        if (self.1)(&out) {
            Ok((out, new_pos))
        } else {
            Err(ParseError::Message {
                msg: "Predicate didn't match".to_string(),
                input,
                pos,
            })
        }
    }
}

#[derive(Debug)]
pub struct PrintRemainingInput<P>(P);

impl<P: Parser> Parser for PrintRemainingInput<P> {
    type Output = P::Output;

    #[inline]
    fn parse<'a>(&self, input: &'a str, mut pos: Pos) -> ParseResult<'a, Self::Output> {
        let (out, new_pos) = self.0.parse(input, pos)?;
        pos = new_pos;

        if let Some(s) = input.from(pos) {
            eprintln!("Remaining input: {:?}", s);
        } else {
            eprintln!("No output remaining");
        }

        Ok((out, new_pos))
    }
}

#[derive(Debug)]
pub struct Whitespace<P>(P);

impl<P: Parser> Parser for Whitespace<P> {
    type Output = P::Output;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        let (out, mut pos) = self.0.parse(input, pos)?;

        loop {
            if let Some(c) = input.at(pos) {
                if c.is_whitespace() {
                    pos = pos + 1;
                } else {
                    return Ok((out, pos));
                }
            } else {
                return Err(ParseError::UnexpectedEndOfInput { pos, input });
            }
        }
    }
}

#[derive(Debug)]
pub struct SepBy<P1, P2> {
    item: P1,
    sep: P2,
    allow_trailing: bool,
}

impl<P1, P2> Parser for SepBy<P1, P2>
where
    P1: Parser,
    P2: Parser,
{
    type Output = Vec<P1::Output>;

    #[inline]
    fn parse<'a>(&self, input: &'a str, mut pos: Pos) -> ParseResult<'a, Self::Output> {
        let mut acc = vec![];
        let mut first_round = true;

        loop {
            match self.item.parse(input, pos) {
                Ok((item, new_pos)) => {
                    pos = new_pos;
                    acc.push(item);
                }
                Err(err) => {
                    if self.allow_trailing && !first_round {
                        break;
                    } else {
                        return Err(err);
                    }
                }
            }

            match self.sep.parse(input, pos) {
                Ok((_, new_pos)) => pos = new_pos,
                Err(_) => break,
            }

            first_round = false;
        }

        Ok((acc, pos))
    }
}

#[derive(Debug)]
pub struct Tap<P, F>(P, F);

impl<P, F> Parser for Tap<P, F>
where
    P: Parser,
    F: Fn(&P::Output),
{
    type Output = P::Output;

    #[inline]
    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        let (out, pos) = self.0.parse(input, pos)?;
        (self.1)(&out);
        Ok((out, pos))
    }
}

#[inline]
pub fn error<T, E>(e: E) -> impl Parser<Output = T>
where
    E: fmt::Display,
{
    Error(e, PhantomData)
}

#[derive(Debug, Copy, Clone)]
struct Error<E, T>(E, PhantomData<T>);

impl<E, T> Parser for Error<E, T>
where
    E: fmt::Display,
{
    type Output = T;

    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        Err(ParseError::Message {
            msg: format!("{}", self.0),
            pos,
            input,
        })
    }
}

impl<A, B, Out> Parser for Either<A, B>
where
    A: Parser<Output = Out>,
    B: Parser<Output = Out>,
{
    type Output = Out;

    fn parse<'a>(&self, input: &'a str, pos: Pos) -> ParseResult<'a, Self::Output> {
        match self {
            Either::A(inner) => inner.parse(input, pos),
            Either::B(inner) => inner.parse(input, pos),
        }
    }
}

#[cfg(test)]
mod test {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_parse_char() {
        let parsed = parse(&char('a'), "a").unwrap();

        assert_eq!(parsed, 'a');
    }

    #[test]
    fn test_parse_or() {
        let parser = char('a').or(char('b'));

        let parsed = parse(&parser, "a").unwrap();
        assert_eq!(parsed, 'a');

        let parsed = parse(&parser, "b").unwrap();
        assert_eq!(parsed, 'b');
    }

    #[test]
    fn test_chaining() {
        let parser =
            char('a').and_then(|a| char('b').and_then(move |b| char('c').map(move |c| (a, b, c))));
        let parsed = parse(&parser, "abc").unwrap();
        assert_eq!(parsed, ('a', 'b', 'c'))
    }

    #[test]
    fn test_sequence() {
        let parser = sequence(vec![char('a'), char('b'), char('c')]);
        let parsed = parse(&parser, "abc").unwrap();
        assert_eq!(parsed, vec!['a', 'b', 'c'])
    }

    #[test]
    fn test_parse_string() {
        let input = "abc";
        let parser = string("abc");
        let parsed = parse(&parser, input).unwrap();
        assert_eq!("abc", parsed);
    }

    #[test]
    fn test_many() {
        let input = "aaa";
        let parser = many(char('a'));
        let parsed = parse(&parser, input).unwrap();
        assert_eq!(vec!['a', 'a', 'a'], parsed);

        let input = "aaabbb";
        let parser = many(char('a')).zip(many(char('b')));
        let parsed = parse(&parser, input).unwrap();
        assert_eq!((vec!['a', 'a', 'a'], vec!['b', 'b', 'b']), parsed);
    }

    #[test]
    fn test_many_not_consuming_everything() {
        let input = "aaab";
        let parser = many(char('a'));
        assert!(parse(&parser, input).is_err());

        let input = "aaabbbbb";
        let parser = many(char('a'));
        assert!(parse(&parser, input).is_err());
    }

    #[test]
    fn test_parse_many1() {
        let input = "aaa";
        let parser = many1(char('a'));
        assert!(parse(&parser, input).is_ok());

        let input = "";
        let parser = many1(char('a'));
        assert!(parse(&parser, input).is_err());

        let input = "ab";
        let parser = many1(char('a'));
        assert!(parse(&parser, input).is_err());

        let input = "abb";
        let parser = many1(char('a'));
        assert!(parse(&parser, input).is_err());
    }

    #[test]
    fn test_times() {
        let input = "aaa";
        let parser = char('a').times(3);
        assert!(parse(&parser, input).is_ok());

        let input = "aaa";
        let parser = char('a').times(4);
        assert!(parse(&parser, input).is_err());

        let input = "aaaaa";
        let parser = char('a').times(4);
        assert!(parse(&parser, input).is_err());
    }

    #[test]
    fn test_string() {
        let input = "foo bar";
        let parser = string("foo")
            .whitespace()
            .and_then(|_| string("bar"))
            .map(|_| ());
        parse(&parser, input).unwrap();
    }

    #[test]
    fn test_sep_by() {
        let input = "a, a, a";
        let parser = char('a').sep_by(char(',').whitespace());
        let out = parse(&parser, input).unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(out, vec!['a', 'a', 'a']);

        let input = "a";
        let parser = char('a').sep_by(char(','));
        let out = parse(&parser, input).unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(out, vec!['a']);

        let input = "a,a,";
        let parser = char('a').sep_by(char(','));
        assert!(parse(&parser, input).is_err());

        let input = "a,a,";
        let parser = char('a').sep_by_allow_trailing(char(','));
        parse(&parser, input).unwrap_or_else(|e| panic!("{}", e));

        let input = "";
        let parser = char('a').sep_by(char(','));
        assert!(parse(&parser, input).is_err());

        let input = "";
        let parser = char('a').sep_by_allow_trailing(char(','));
        assert!(parse(&parser, input).is_err());
    }

    #[test]
    fn test_complex_sep_by() {
        let radix = 10;
        let digit = any_char()
            .when(|c| c.is_digit(radix))
            .map(|c| c.to_digit(radix).unwrap());

        let parser = char('[')
            .whitespace()
            .zip_right(
                digit
                    .whitespace()
                    .sep_by_allow_trailing(char(',').whitespace()),
            )
            .whitespace()
            .zip_left(char(']'));

        assert_eq!(
            parse(&parser, "[1,2,3]").unwrap_or_else(|e| panic!("{}", e)),
            vec![1, 2, 3]
        );

        assert_eq!(
            parse(&parser, "[1, 2, 3]").unwrap_or_else(|e| panic!("{}", e)),
            vec![1, 2, 3]
        );

        assert_eq!(
            parse(&parser, "[1,2,3,]").unwrap_or_else(|e| panic!("{}", e)),
            vec![1, 2, 3]
        );

        assert_eq!(
            parse(&parser, "[1, 2, 3,]").unwrap_or_else(|e| panic!("{}", e)),
            vec![1, 2, 3]
        );

        assert_eq!(
            parse(&parser, "[ 1,2,3 ]").unwrap_or_else(|e| panic!("{}", e)),
            vec![1, 2, 3]
        );

        assert_eq!(
            parse(&parser, "[ 1, 2, 3 ]").unwrap_or_else(|e| panic!("{}", e)),
            vec![1, 2, 3]
        );

        assert_eq!(
            parse(&parser, "[ 1,2,3, ]").unwrap_or_else(|e| panic!("{}", e)),
            vec![1, 2, 3]
        );

        assert_eq!(
            parse(&parser, "[ 1, 2, 3, ]").unwrap_or_else(|e| panic!("{}", e)),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn test_whitespace_empty_input() {
        let parser = char('a').zip(maybe(whitespace())).zip(maybe(char('b')));

        parse(&parser, "a").unwrap_or_else(|e| panic!("{}", e));
        parse(&parser, "a b").unwrap_or_else(|e| panic!("{}", e));
    }
}
