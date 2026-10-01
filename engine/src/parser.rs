//! Recursive-descent parser for the chlive dialect.
//!
//! Grammar so far:
//! ```text
//! query      := SELECT projection FROM table [WHERE or_expr] [';'] EOF
//! projection := '*' | column (',' column)*
//! column     := identifier
//! table      := identifier
//! or_expr    := and_expr (OR and_expr)*
//! and_expr   := not_expr (AND not_expr)*
//! not_expr   := NOT not_expr | predicate
//! predicate  := operand cmp operand
//!             | operand IS [NOT] NULL
//!             | operand [NOT] IN '(' literal (',' literal)* ')'
//!             | operand
//! operand    := column | literal | '(' or_expr ')'
//! literal    := number | '-' number | string | TRUE | FALSE | NULL
//! ```

use crate::ast::{CmpOp, ColumnRef, Expr, Literal, Projection, Query, TableRef};
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
        let selection = if self.peek().kind == TokenKind::Keyword("WHERE") {
            self.next();
            Some(self.or_expr()?)
        } else {
            None
        };
        self.end()?;
        Ok(Query {
            projection,
            from,
            selection,
        })
    }

    fn or_expr(&mut self) -> Result<Expr, SyntaxError> {
        let mut left = self.and_expr()?;
        while self.peek().kind == TokenKind::Keyword("OR") {
            self.next();
            let right = self.and_expr()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn and_expr(&mut self) -> Result<Expr, SyntaxError> {
        let mut left = self.not_expr()?;
        while self.peek().kind == TokenKind::Keyword("AND") {
            self.next();
            let right = self.not_expr()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn not_expr(&mut self) -> Result<Expr, SyntaxError> {
        if self.peek().kind == TokenKind::Keyword("NOT") {
            let span = self.next().span;
            let expr = self.not_expr()?;
            return Ok(Expr::Not {
                expr: Box::new(expr),
                span,
            });
        }
        self.predicate()
    }

    fn predicate(&mut self) -> Result<Expr, SyntaxError> {
        let left = self.operand()?;
        let t = self.peek().clone();
        let op = match t.kind {
            TokenKind::Eq => Some(CmpOp::Eq),
            TokenKind::NotEq => Some(CmpOp::NotEq),
            TokenKind::Lt => Some(CmpOp::Lt),
            TokenKind::LtEq => Some(CmpOp::LtEq),
            TokenKind::Gt => Some(CmpOp::Gt),
            TokenKind::GtEq => Some(CmpOp::GtEq),
            _ => None,
        };
        if let Some(op) = op {
            self.next();
            let right = self.operand()?;
            return Ok(Expr::Compare {
                op,
                left: Box::new(left),
                right: Box::new(right),
                span: t.span,
            });
        }
        match t.kind {
            TokenKind::Keyword("IS") => {
                self.next();
                let negated = self.peek().kind == TokenKind::Keyword("NOT");
                if negated {
                    self.next();
                }
                if self.peek().kind != TokenKind::Keyword("NULL") {
                    return Err(self.expected("NULL"));
                }
                self.next();
                Ok(Expr::IsNull {
                    expr: Box::new(left),
                    negated,
                    span: t.span,
                })
            }
            TokenKind::Keyword("NOT")
                if self.tokens[self.pos + 1].kind == TokenKind::Keyword("IN") =>
            {
                self.next();
                self.in_list(left, true)
            }
            TokenKind::Keyword("IN") => self.in_list(left, false),
            TokenKind::Keyword(kw @ ("LIKE" | "ILIKE" | "BETWEEN")) => Err(SyntaxError::new(
                format!("{kw} is not supported yet"),
                t.span,
            )),
            TokenKind::Plus | TokenKind::Minus | TokenKind::Star | TokenKind::Slash => {
                Err(SyntaxError::new("arithmetic is not supported yet", t.span))
            }
            _ => Ok(left),
        }
    }

    /// After `[NOT]`, at `IN`.
    fn in_list(&mut self, expr: Expr, negated: bool) -> Result<Expr, SyntaxError> {
        let span = self.next().span;
        if self.peek().kind != TokenKind::LParen {
            return Err(self.expected("`(`"));
        }
        self.next();
        let mut list = vec![self.literal()?];
        while self.peek().kind == TokenKind::Comma {
            self.next();
            list.push(self.literal()?);
        }
        if self.peek().kind != TokenKind::RParen {
            return Err(self.expected("`,` or `)`"));
        }
        self.next();
        Ok(Expr::InList {
            expr: Box::new(expr),
            list,
            negated,
            span,
        })
    }

    fn literal(&mut self) -> Result<(Literal, crate::error::Span), SyntaxError> {
        let t = self.peek().clone();
        let value = match t.kind {
            TokenKind::Number(n) => Literal::Number(n),
            TokenKind::String(v) => Literal::String(v),
            TokenKind::Keyword("TRUE") => Literal::Bool(true),
            TokenKind::Keyword("FALSE") => Literal::Bool(false),
            TokenKind::Keyword("NULL") => Literal::Null,
            TokenKind::Minus => {
                self.next();
                let n = self.peek().clone();
                let TokenKind::Number(num) = n.kind else {
                    return Err(self.expected("a number after `-`"));
                };
                self.next();
                return Ok((
                    Literal::Number(format!("-{num}")),
                    crate::error::Span::new(t.span.start, n.span.end),
                ));
            }
            _ => return Err(self.expected("a value")),
        };
        self.next();
        Ok((value, t.span))
    }

    fn operand(&mut self) -> Result<Expr, SyntaxError> {
        let t = self.peek().clone();
        match t.kind {
            TokenKind::Ident(name) => {
                self.next();
                match self.peek().kind {
                    TokenKind::LParen => {
                        Err(SyntaxError::new("functions are not supported yet", t.span))
                    }
                    TokenKind::Dot => Err(SyntaxError::new(
                        "qualified column names are not supported",
                        self.peek().span,
                    )),
                    _ => Ok(Expr::Column(ColumnRef { name, span: t.span })),
                }
            }
            TokenKind::LParen => {
                self.next();
                let inner = self.or_expr()?;
                if self.peek().kind != TokenKind::RParen {
                    return Err(self.expected("`)`"));
                }
                self.next();
                Ok(inner)
            }
            TokenKind::Number(_)
            | TokenKind::String(_)
            | TokenKind::Minus
            | TokenKind::Keyword("TRUE" | "FALSE" | "NULL") => {
                let (value, span) = self.literal()?;
                Ok(Expr::Literal { value, span })
            }
            TokenKind::Keyword("SELECT") => {
                Err(SyntaxError::new("subqueries are not supported", t.span))
            }
            _ => Err(self.expected("a column or a value")),
        }
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
                kw @ ("GROUP" | "HAVING" | "ORDER" | "LIMIT" | "JOIN" | "UNION"),
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

    fn where_of(src: &str) -> String {
        fn show(e: &Expr) -> String {
            match e {
                Expr::Column(c) => c.name.clone(),
                Expr::Literal { value, .. } => format!("{value:?}"),
                Expr::Compare {
                    op, left, right, ..
                } => format!("({} {} {})", show(left), op.symbol(), show(right)),
                Expr::And(l, r) => format!("({} AND {})", show(l), show(r)),
                Expr::Or(l, r) => format!("({} OR {})", show(l), show(r)),
                Expr::Not { expr, .. } => format!("(NOT {})", show(expr)),
                Expr::IsNull { expr, negated, .. } => format!(
                    "({} IS{} NULL)",
                    show(expr),
                    if *negated { " NOT" } else { "" }
                ),
                Expr::InList {
                    expr,
                    list,
                    negated,
                    ..
                } => format!(
                    "({}{} IN {:?})",
                    show(expr),
                    if *negated { " NOT" } else { "" },
                    list.iter().map(|(l, _)| l.clone()).collect::<Vec<_>>()
                ),
            }
        }
        show(
            parse(src)
                .unwrap_or_else(|e| panic!("{}", e.render(src)))
                .selection
                .as_ref()
                .unwrap(),
        )
    }

    #[test]
    fn where_precedence_and_forms() {
        assert_eq!(
            where_of("select * from t where a = 1 or b = 2 and c = 3"),
            r#"((a = Number("1")) OR ((b = Number("2")) AND (c = Number("3"))))"#
        );
        assert_eq!(
            where_of("select * from t where (a = 1 or b = 2) and not c <> 'x'"),
            r#"(((a = Number("1")) OR (b = Number("2"))) AND (NOT (c != String("x"))))"#
        );
        assert_eq!(
            where_of("select * from t where a is not null"),
            "(a IS NOT NULL)"
        );
        assert_eq!(
            where_of("select * from t where a not in (1, -2.5, 'x')"),
            r#"(a NOT IN [Number("1"), Number("-2.5"), String("x")])"#
        );
        assert_eq!(
            where_of("select * from t where a >= -1"),
            r#"(a >= Number("-1"))"#
        );
        assert_eq!(where_of("select * from t where flag"), "flag");
        assert_eq!(
            where_of("select * from t where flag = true;"),
            "(flag = Bool(true))"
        );
    }

    #[test]
    fn where_rejections() {
        assert_eq!(
            error("select * from t where"),
            "expected a column or a value, found end of input at 1:22"
        );
        assert_eq!(
            error("select * from t where a = 1 and"),
            "expected a column or a value, found end of input at 1:32"
        );
        assert_eq!(
            error("select * from t where lower(a) = 'x'"),
            "functions are not supported yet at 1:23"
        );
        assert_eq!(
            error("select * from t where a + 1 > 2"),
            "arithmetic is not supported yet at 1:25"
        );
        assert_eq!(
            error("select * from t where a like 'x%'"),
            "LIKE is not supported yet at 1:25"
        );
        assert_eq!(
            error("select * from t where a between 1 and 2"),
            "BETWEEN is not supported yet at 1:25"
        );
        assert_eq!(
            error("select * from t where a in 1"),
            "expected `(`, found number `1` at 1:28"
        );
        assert_eq!(
            error("select * from t where a in (select 1)"),
            "expected a value, found SELECT at 1:29"
        );
        assert_eq!(
            error("select * from t where (a = 1"),
            "expected `)`, found end of input at 1:29"
        );
        assert_eq!(
            error("select * from t where a is 1"),
            "expected NULL, found number `1` at 1:28"
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
