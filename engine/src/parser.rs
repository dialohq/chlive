//! Recursive-descent parser for the chlive dialect.
//!
//! Grammar so far:
//! ```text
//! query      := SELECT projection FROM table [';'] EOF
//! projection := '*' | column (',' column)*
//! column     := identifier
//! table      := identifier
//! ```

use crate::ast::{ColumnRef, Projection, Query, TableRef};
use crate::error::SyntaxError;
use crate::lexer::{tokenize, Token, TokenKind};

pub fn parse(src: &str) -> Result<Query, SyntaxError> {
    let tokens = tokenize(src)?;
    let mut p = Parser { tokens, pos: 0 };
    let query = p.query()?;
    Ok(query)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn next(&mut self) -> Token {
        let t = self.tokens[self.pos].clone();
        if t.kind != TokenKind::Eof {
            self.pos += 1;
        }
        t
    }

    fn expected(&self, what: &str) -> SyntaxError {
        let t = self.peek();
        SyntaxError::new(
            format!("expected {what}, found {}", t.kind.describe()),
            t.span,
        )
    }

    fn keyword(&mut self, kw: &'static str) -> Result<(), SyntaxError> {
        if self.peek().kind == TokenKind::Keyword(kw) {
            self.next();
            Ok(())
        } else {
            Err(self.expected(kw))
        }
    }

    fn query(&mut self) -> Result<Query, SyntaxError> {
        self.keyword("SELECT")?;
        let projection = self.projection()?;
        self.keyword("FROM")?;
        let from = self.table()?;
        self.end()?;
        Ok(Query { projection, from })
    }

    fn projection(&mut self) -> Result<Projection, SyntaxError> {
        if self.peek().kind == TokenKind::Star {
            self.next();
            if self.peek().kind == TokenKind::Comma {
                return Err(SyntaxError::new(
                    "cannot mix * with column names",
                    self.peek().span,
                ));
            }
            return Ok(Projection::Wildcard);
        }
        let mut columns = vec![self.column()?];
        while self.peek().kind == TokenKind::Comma {
            self.next();
            if self.peek().kind == TokenKind::Star {
                return Err(SyntaxError::new(
                    "cannot mix * with column names",
                    self.peek().span,
                ));
            }
            columns.push(self.column()?);
        }
        Ok(Projection::Columns(columns))
    }

    fn column(&mut self) -> Result<ColumnRef, SyntaxError> {
        let t = self.peek().clone();
        let TokenKind::Ident(name) = t.kind else {
            return Err(self.expected(
                if self.pos > 0 && self.tokens[self.pos - 1].kind == TokenKind::Comma {
                    "a column name"
                } else {
                    "`*` or a column name"
                },
            ));
        };
        self.next();
        match self.peek().kind {
            TokenKind::LParen => Err(SyntaxError::new(
                "expressions are not supported yet; list column names",
                t.span,
            )),
            TokenKind::Dot => Err(SyntaxError::new(
                "qualified column names are not supported",
                self.peek().span,
            )),
            _ => Ok(ColumnRef { name, span: t.span }),
        }
    }

    fn table(&mut self) -> Result<TableRef, SyntaxError> {
        let t = self.peek().clone();
        let TokenKind::Ident(name) = t.kind else {
            return Err(self.expected("a table name"));
        };
        self.next();
        if self.peek().kind == TokenKind::Dot {
            return Err(SyntaxError::new(
                "qualified table names are not supported",
                self.peek().span,
            ));
        }
        Ok(TableRef { name, span: t.span })
    }

    fn end(&mut self) -> Result<(), SyntaxError> {
        if self.peek().kind == TokenKind::Semicolon {
            self.next();
        }
        let t = self.peek().clone();
        match t.kind {
            TokenKind::Eof => Ok(()),
            TokenKind::Keyword(
                kw @ ("WHERE" | "GROUP" | "HAVING" | "ORDER" | "LIMIT" | "JOIN" | "UNION"),
            ) => Err(SyntaxError::new(
                format!("{kw} is not supported yet"),
                t.span,
            )),
            TokenKind::Comma => Err(SyntaxError::new(
                "selecting from multiple tables is not supported",
                t.span,
            )),
            _ => Err(self.expected("end of query")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(src: &str) -> String {
        parse(src)
            .unwrap_or_else(|e| panic!("{}", e.render(src)))
            .from
            .name
    }

    fn error(src: &str) -> String {
        parse(src).unwrap_err().render(src)
    }

    #[test]
    fn accepts_select_star() {
        assert_eq!(table("select * from events"), "events");
        assert_eq!(table("SELECT * FROM events;"), "events");
        assert_eq!(table("  Select\n  *\n  From\n  events  "), "events");
        assert_eq!(table("select * from \"Events\" -- comment"), "Events");
        assert_eq!(
            parse("select * from events").unwrap().projection,
            Projection::Wildcard
        );
    }

    fn columns(src: &str) -> Vec<String> {
        match parse(src)
            .unwrap_or_else(|e| panic!("{}", e.render(src)))
            .projection
        {
            Projection::Columns(cols) => cols.into_iter().map(|c| c.name).collect(),
            Projection::Wildcard => panic!("expected a column list"),
        }
    }

    #[test]
    fn accepts_column_lists() {
        assert_eq!(columns("select ts from events"), ["ts"]);
        assert_eq!(
            columns("select ts, user_id,latency_ms from events"),
            ["ts", "user_id", "latency_ms"]
        );
        assert_eq!(
            columns("SELECT\n  \"ts\",\n  `user id`\nFROM events"),
            ["ts", "user id"]
        );
        let q = parse("select ts, user_id from events").unwrap();
        let Projection::Columns(cols) = q.projection else {
            panic!()
        };
        assert_eq!(cols[1].span, crate::error::Span::new(11, 18));
    }

    #[test]
    fn rejects_bad_column_lists() {
        assert_eq!(
            error("select ts, from events"),
            "expected a column name, found FROM at 1:12"
        );
        assert_eq!(
            error("select from events"),
            "expected `*` or a column name, found FROM at 1:8"
        );
        assert_eq!(
            error("select *, ts from events"),
            "cannot mix * with column names at 1:9"
        );
        assert_eq!(
            error("select ts, * from events"),
            "cannot mix * with column names at 1:12"
        );
        assert_eq!(
            error("select count(ts) from events"),
            "expressions are not supported yet; list column names at 1:8"
        );
        assert_eq!(
            error("select e.ts from events"),
            "qualified column names are not supported at 1:9"
        );
        assert_eq!(
            error("select ts user_id from events"),
            "expected FROM, found identifier `user_id` at 1:11"
        );
    }

    #[test]
    fn rejects_with_positions() {
        assert_eq!(error(""), "expected SELECT, found end of input at 1:1");
        assert_eq!(
            error("select *"),
            "expected FROM, found end of input at 1:9"
        );
        assert_eq!(
            error("select * from"),
            "expected a table name, found end of input at 1:14"
        );
        assert_eq!(
            error("select * from events where x"),
            "WHERE is not supported yet at 1:22"
        );
        assert_eq!(
            error("select * from events limit 1"),
            "LIMIT is not supported yet at 1:22"
        );
        assert_eq!(
            error("select * from db.events"),
            "qualified table names are not supported at 1:17"
        );
        assert_eq!(
            error("select * from a, b"),
            "selecting from multiple tables is not supported at 1:16"
        );
        assert_eq!(
            error("select * from events; select"),
            "expected end of query, found SELECT at 1:23"
        );
        assert_eq!(
            error("select *\nfrom from"),
            "expected a table name, found FROM at 2:6"
        );
    }
}
