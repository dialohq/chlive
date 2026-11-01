//! Recursive-descent parser for the chlive dialect.
//!
//! The grammar is in the README ("Supported queries").

use crate::ast::{CallArg, Direction, Limit, NullsOrder, Order, OrderItem, SelectExpr, SelectItem};
use crate::ast::{CmpOp, ColumnRef, Expr, Literal, Projection, Query, TableRef};
use crate::calendar::{Interval, Unit};
use crate::error::{Span, SyntaxError};
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
        let query = self.query_body()?;
        self.end()?;
        Ok(query)
    }

    /// A query without what may follow it (the end, or a sub-query's `)`).
    fn query_body(&mut self) -> Result<Query, SyntaxError> {
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
        let group_by = if self.peek().kind == TokenKind::Keyword("GROUP") {
            let span = self.next().span;
            self.keyword("BY")?;
            let mut keys = vec![self.group_key()?];
            while self.peek().kind == TokenKind::Comma {
                self.next();
                keys.push(self.group_key()?);
            }
            Some((keys, span))
        } else {
            None
        };
        let order = if self.peek().kind == TokenKind::Keyword("ORDER") {
            self.next();
            self.keyword("BY")?;
            Some(self.order()?)
        } else {
            None
        };
        let (mut limit, mut offset) = (None, None);
        if self.peek().kind == TokenKind::Keyword("LIMIT") {
            self.next();
            let first = self.limit()?;
            if self.peek().kind == TokenKind::Comma {
                // ClickHouse's `LIMIT m, n`: the offset first
                self.next();
                offset = Some(first);
                limit = Some(self.limit()?);
            } else {
                limit = Some(first);
            }
        }
        if offset.is_none() && self.at_word("OFFSET") {
            self.next();
            offset = Some(self.count("OFFSET")?);
        }
        Ok(Query {
            projection,
            from,
            selection,
            group_by,
            order,
            limit,
            offset,
        })
    }

    fn group_key(&mut self) -> Result<ColumnRef, SyntaxError> {
        let t = self.peek().clone();
        match &t.kind {
            TokenKind::Number(_) => Err(SyntaxError::new(
                "grouping by position is not supported yet; name the column",
                t.span,
            )),
            TokenKind::Ident(w) if w.eq_ignore_ascii_case("ALL") => Err(SyntaxError::new(
                "GROUP BY ALL is not supported yet; name the columns",
                t.span,
            )),
            TokenKind::Ident(_) => {
                let c = self.column()?;
                match self.peek().kind {
                    TokenKind::Plus | TokenKind::Minus | TokenKind::Star | TokenKind::Slash => {
                        Err(SyntaxError::new(
                            "grouping by expressions is not supported yet; name a column",
                            self.peek().span,
                        ))
                    }
                    _ => Ok(c),
                }
            }
            TokenKind::Eof | TokenKind::Semicolon => Err(self.expected("a column name")),
            _ => Err(SyntaxError::new(
                "grouping by expressions is not supported yet; name a column",
                t.span,
            )),
        }
    }

    /// `column` or `name(arg)`, then an optional `AS alias`.
    fn select_item(&mut self) -> Result<SelectItem, SyntaxError> {
        let expr = self.select_expr()?;
        self.alias_for(expr)
    }

    /// `column` or `name(arg)` (`name()`, `name(*)`).
    fn select_expr(&mut self) -> Result<SelectExpr, SyntaxError> {
        let t = self.peek().clone();
        Ok(
            match (&t.kind, &self.tokens.get(self.pos + 1).map(|t| &t.kind)) {
                (TokenKind::Param { .. }, _) => {
                    return Err(SyntaxError::new(
                        "parameters can only be used in WHERE",
                        t.span,
                    ))
                }
                (
                    TokenKind::Number(_)
                    | TokenKind::String(_)
                    | TokenKind::Minus
                    | TokenKind::Keyword("TRUE" | "FALSE" | "NULL"),
                    _,
                ) => {
                    let (value, span) = self.literal()?;
                    SelectExpr::Literal(value, span)
                }
                (TokenKind::Ident(name), Some(TokenKind::LParen)) => {
                    self.next();
                    self.next();
                    let arg = match self.peek().kind.clone() {
                        TokenKind::RParen => CallArg::None,
                        TokenKind::Star => {
                            self.next();
                            CallArg::Star
                        }
                        TokenKind::Ident(_) => {
                            if self.tokens.get(self.pos + 1).map(|t| &t.kind)
                                == Some(&TokenKind::LParen)
                            {
                                return Err(SyntaxError::new(
                                    "nested functions are not supported yet",
                                    self.peek().span,
                                ));
                            }
                            CallArg::Column(self.column()?)
                        }
                        _ => {
                            return Err(SyntaxError::new(
                                "function arguments other than a column are not supported yet",
                                self.peek().span,
                            ))
                        }
                    };
                    if self.peek().kind == TokenKind::Comma {
                        return Err(SyntaxError::new(
                            "functions with more than one argument are not supported yet",
                            self.peek().span,
                        ));
                    }
                    if self.peek().kind != TokenKind::RParen {
                        return Err(self.expected("`)`"));
                    }
                    self.next();
                    SelectExpr::Call {
                        name: name.clone(),
                        arg,
                        span: t.span,
                    }
                }
                _ => SelectExpr::Column(self.column()?),
            },
        )
    }

    fn alias_for(&mut self, expr: SelectExpr) -> Result<SelectItem, SyntaxError> {
        let alias = if self.peek().kind == TokenKind::Keyword("AS") {
            self.next();
            match self.peek().kind.clone() {
                TokenKind::Ident(a) => {
                    self.next();
                    Some(a)
                }
                _ => return Err(self.expected("an alias")),
            }
        } else {
            None
        };
        Ok(SelectItem { expr, alias })
    }

    /// `LIMIT n`, a whole number; OFFSET, `LIMIT m, n`, WITH TIES and BY aren't supported yet.
    /// A non-negative whole number, after LIMIT or OFFSET.
    fn count(&mut self, what: &str) -> Result<Limit, SyntaxError> {
        let t = self.peek().clone();
        let value = match &t.kind {
            TokenKind::Number(n) => n.parse::<u64>().ok(),
            _ => None,
        };
        let Some(value) = value else {
            return Err(SyntaxError::new(
                format!("{what} needs a non-negative whole number"),
                t.span,
            ));
        };
        self.next();
        Ok(Limit {
            value,
            span: t.span,
        })
    }

    fn limit(&mut self) -> Result<Limit, SyntaxError> {
        let limit = self.count("LIMIT")?;
        let next = self.peek().clone();
        if self.at_word("WITH") {
            return Err(SyntaxError::new(
                "LIMIT … WITH TIES is not supported yet",
                next.span,
            ));
        }
        if next.kind == TokenKind::Keyword("BY") {
            return Err(SyntaxError::new(
                "LIMIT … BY is not supported yet",
                next.span,
            ));
        }
        Ok(limit)
    }

    /// Is the next token the (unreserved) word `word`? ASC, DESC, NULLS, FIRST, LAST and ALL
    /// are only special here, so they stay usable as column names elsewhere.
    fn at_word(&self, word: &str) -> bool {
        matches!(&self.peek().kind, TokenKind::Ident(w) if w.eq_ignore_ascii_case(word))
    }

    /// `ALL [dir] [nulls]` or `item (, item)*`.
    fn order(&mut self) -> Result<Order, SyntaxError> {
        if self.at_word("ALL") {
            let span = self.next().span;
            let (direction, nulls) = self.order_modifiers()?;
            if self.peek().kind == TokenKind::Comma {
                return Err(SyntaxError::new(
                    "ORDER BY ALL can't be combined with other columns",
                    self.peek().span,
                ));
            }
            return Ok(Order::All {
                direction,
                nulls,
                span,
            });
        }
        let mut items = vec![self.order_item()?];
        while self.peek().kind == TokenKind::Comma {
            self.next();
            items.push(self.order_item()?);
        }
        Ok(Order::Items(items))
    }

    fn order_item(&mut self) -> Result<OrderItem, SyntaxError> {
        let t = self.peek().clone();
        match &t.kind {
            TokenKind::Number(_) => {
                return Err(SyntaxError::new(
                    "ordering by position is not supported yet; name the column",
                    t.span,
                ))
            }
            TokenKind::Ident(_) => {}
            TokenKind::Eof | TokenKind::Semicolon => return Err(self.expected("a column name")),
            _ => {
                return Err(SyntaxError::new(
                    "ordering by expressions is not supported yet; name a column",
                    t.span,
                ))
            }
        }
        let expr = self.select_expr()?;
        match self.peek().kind {
            TokenKind::Plus | TokenKind::Minus | TokenKind::Star | TokenKind::Slash => {
                return Err(SyntaxError::new(
                    "ordering by expressions is not supported yet; name a column",
                    self.peek().span,
                ))
            }
            _ => {}
        }
        let (direction, nulls) = self.order_modifiers()?;
        Ok(OrderItem {
            expr,
            direction,
            nulls,
        })
    }

    fn order_modifiers(&mut self) -> Result<(Option<Direction>, Option<NullsOrder>), SyntaxError> {
        let direction = if self.at_word("ASC") {
            self.next();
            Some(Direction::Asc)
        } else if self.at_word("DESC") {
            self.next();
            Some(Direction::Desc)
        } else {
            None
        };
        let nulls = if self.at_word("NULLS") {
            self.next();
            if self.at_word("FIRST") {
                self.next();
                Some(NullsOrder::First)
            } else if self.at_word("LAST") {
                self.next();
                Some(NullsOrder::Last)
            } else {
                return Err(self.expected("FIRST or LAST"));
            }
        } else {
            None
        };
        for word in ["COLLATE", "WITH"] {
            if self.at_word(word) {
                return Err(SyntaxError::new(
                    format!("{word} in ORDER BY is not supported"),
                    self.peek().span,
                ));
            }
        }
        Ok((direction, nulls))
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
        if self.peek().kind == TokenKind::Keyword("NOT")
            && self.tokens.get(self.pos + 1).map(|t| &t.kind) == Some(&TokenKind::Keyword("EXISTS"))
        {
            self.next();
            return self.exists(true);
        }
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
        if self.peek().kind == TokenKind::Keyword("SELECT") {
            let query = self.query_body()?;
            if self.peek().kind != TokenKind::RParen {
                return Err(self.expected("`)` after the sub-query"));
            }
            self.next();
            return Ok(Expr::InSubquery {
                expr: Box::new(expr),
                query: Box::new(query),
                negated,
                span,
            });
        }
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
            TokenKind::Param { name, ty } => Literal::Param { name, ty },
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
                if self.tokens.get(self.pos + 1).map(|t| &t.kind) == Some(&TokenKind::LParen) {
                    if name.eq_ignore_ascii_case("now") {
                        return self.now_expr();
                    }
                    return Err(SyntaxError::new("functions are not supported yet", t.span));
                }
                Ok(Expr::Column(self.qualified()?))
            }
            TokenKind::Keyword("EXISTS") => self.exists(false),
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
            | TokenKind::Param { .. }
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

    /// `now()`, then `+`/`- INTERVAL n unit` any number of times, at `now`.
    fn now_expr(&mut self) -> Result<Expr, SyntaxError> {
        let span = self.next().span;
        self.next(); // `(`
        let tz = match self.peek().kind.clone() {
            TokenKind::RParen => None,
            TokenKind::String(tz) => {
                self.next();
                Some(tz)
            }
            _ => {
                return Err(SyntaxError::new(
                    "now()'s argument is a timezone, e.g. now('Europe/Warsaw')",
                    self.peek().span,
                ))
            }
        };
        if self.peek().kind != TokenKind::RParen {
            return Err(self.expected("`)`"));
        }
        self.next();
        let mut intervals = Vec::new();
        loop {
            let sign = match self.peek().kind {
                TokenKind::Plus => 1,
                TokenKind::Minus => -1,
                _ => break,
            };
            let interval = matches!(
                self.tokens.get(self.pos + 1).map(|t| &t.kind),
                Some(TokenKind::Ident(w)) if w.eq_ignore_ascii_case("INTERVAL")
            );
            if !interval {
                return Err(SyntaxError::new(
                    "only an INTERVAL can be added to or subtracted from now()",
                    self.peek().span,
                ));
            }
            self.next();
            self.next();
            let TokenKind::Number(n) = self.peek().kind.clone() else {
                return Err(self.expected("a number of units after INTERVAL"));
            };
            let at = self.next().span;
            let n: i64 = n
                .parse()
                .map_err(|_| SyntaxError::new("an INTERVAL is a whole number of units", at))?;
            let unit = self.peek().clone();
            let TokenKind::Ident(u) = &unit.kind else {
                return Err(self.expected("a unit (SECOND, MINUTE, HOUR, DAY, …)"));
            };
            let unit_of = match u.to_ascii_uppercase().as_str() {
                "MILLISECOND" => Unit::Millisecond,
                "SECOND" => Unit::Second,
                "MINUTE" => Unit::Minute,
                "HOUR" => Unit::Hour,
                "DAY" => Unit::Day,
                "WEEK" => Unit::Week,
                "MONTH" | "QUARTER" | "YEAR" => {
                    return Err(SyntaxError::new(
                        "calendar intervals (MONTH, QUARTER, YEAR) aren't supported yet",
                        unit.span,
                    ))
                }
                _ => {
                    return Err(SyntaxError::new(
                        "an INTERVAL's unit is MILLISECOND, SECOND, MINUTE, HOUR, DAY or WEEK",
                        unit.span,
                    ))
                }
            };
            self.next();
            intervals.push(Interval {
                amount: sign * n,
                unit: unit_of,
            });
        }
        Ok(Expr::Now {
            tz,
            intervals,
            span,
        })
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
        let mut items = vec![self.select_item()?];
        while self.peek().kind == TokenKind::Comma {
            self.next();
            if self.peek().kind == TokenKind::Star {
                return Err(SyntaxError::new(
                    "cannot mix * with column names",
                    self.peek().span,
                ));
            }
            items.push(self.select_item()?);
        }
        // a plain column list stays one (no functions, no aliases)
        if items
            .iter()
            .all(|i| i.alias.is_none() && matches!(i.expr, SelectExpr::Column(_)))
        {
            return Ok(Projection::Columns(
                items
                    .into_iter()
                    .map(|i| match i.expr {
                        SelectExpr::Column(c) => c,
                        _ => unreachable!("only columns here"),
                    })
                    .collect(),
            ));
        }
        Ok(Projection::Items(items))
    }

    fn column(&mut self) -> Result<ColumnRef, SyntaxError> {
        let t = self.peek().clone();
        let TokenKind::Ident(_) = t.kind else {
            return Err(self.expected(
                if self.pos > 0 && self.tokens[self.pos - 1].kind == TokenKind::Comma {
                    "a column name"
                } else {
                    "`*` or a column name"
                },
            ));
        };
        if self.tokens.get(self.pos + 1).map(|t| &t.kind) == Some(&TokenKind::LParen) {
            return Err(SyntaxError::new(
                "expressions are not supported yet; list column names",
                t.span,
            ));
        }
        self.qualified()
    }

    /// `name` or `table.name` (a table or its alias), at an identifier.
    fn qualified(&mut self) -> Result<ColumnRef, SyntaxError> {
        let t = self.next();
        let TokenKind::Ident(first) = t.kind else {
            unreachable!("called at an identifier")
        };
        if self.peek().kind != TokenKind::Dot {
            return Ok(ColumnRef {
                qualifier: None,
                name: first,
                span: t.span,
            });
        }
        self.next();
        let n = self.peek().clone();
        let TokenKind::Ident(name) = n.kind else {
            return Err(self.expected("a column name after `.`"));
        };
        self.next();
        if self.peek().kind == TokenKind::Dot {
            return Err(SyntaxError::new(
                "names are `column` or `table.column`; `db.table.column` is not supported",
                self.peek().span,
            ));
        }
        Ok(ColumnRef {
            qualifier: Some(first),
            name,
            span: Span {
                start: t.span.start,
                end: n.span.end,
            },
        })
    }

    /// At `EXISTS`: `EXISTS (SELECT …)`.
    fn exists(&mut self, negated: bool) -> Result<Expr, SyntaxError> {
        let span = self.next().span;
        if self.peek().kind != TokenKind::LParen {
            return Err(self.expected("`(` and a sub-query after EXISTS"));
        }
        self.next();
        if self.peek().kind != TokenKind::Keyword("SELECT") {
            return Err(self.expected("a sub-query (SELECT …) after EXISTS"));
        }
        let query = self.query_body()?;
        if self.peek().kind != TokenKind::RParen {
            return Err(self.expected("`)` after the sub-query"));
        }
        self.next();
        Ok(Expr::Exists {
            query: Box::new(query),
            negated,
            span,
        })
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
        let alias = if self.peek().kind == TokenKind::Keyword("AS") {
            self.next();
            match self.peek().kind.clone() {
                TokenKind::Ident(a) => {
                    self.next();
                    Some(a)
                }
                _ => return Err(self.expected("an alias for the table")),
            }
        } else if let (TokenKind::Ident(a), false) = (
            self.peek().kind.clone(),
            self.at_word("OFFSET") || self.at_word("FINAL"),
        ) {
            self.next();
            Some(a)
        } else {
            None
        };
        let final_span = self.at_word("FINAL").then(|| self.next().span);
        Ok(TableRef {
            name,
            alias,
            final_span,
            span: t.span,
        })
    }

    fn end(&mut self) -> Result<(), SyntaxError> {
        if self.peek().kind == TokenKind::Semicolon {
            self.next();
        }
        let t = self.peek().clone();
        match t.kind {
            TokenKind::Eof => Ok(()),
            TokenKind::Keyword(kw @ ("HAVING" | "JOIN" | "UNION")) => Err(SyntaxError::new(
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
    use crate::error::Span;

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
            Projection::Wildcard | Projection::Items(_) => panic!("expected a column list"),
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
            error("select count(lower(ts)) from events"),
            "nested functions are not supported yet at 1:14"
        );
        assert_eq!(
            error("select a.e.ts from events"),
            "names are `column` or `table.column`; `db.table.column` is not supported at 1:11"
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
                Expr::Now { tz, intervals, .. } => format!(
                    "now({}{})",
                    tz.as_deref().unwrap_or(""),
                    intervals
                        .iter()
                        .map(|i| format!(" {:+} {}", i.amount, i.unit.name()))
                        .collect::<String>()
                ),
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
                Expr::Exists { query, negated, .. } => format!(
                    "({}EXISTS (SELECT … FROM {}{}))",
                    if *negated { "NOT " } else { "" },
                    query.from.name,
                    query
                        .selection
                        .as_ref()
                        .map(|w| format!(" WHERE {}", show(w)))
                        .unwrap_or_default()
                ),
                Expr::InSubquery {
                    expr,
                    query,
                    negated,
                    ..
                } => format!(
                    "({}{} IN (SELECT … FROM {}{}))",
                    show(expr),
                    if *negated { " NOT" } else { "" },
                    query.from.name,
                    query
                        .selection
                        .as_ref()
                        .map(|w| format!(" WHERE {}", show(w)))
                        .unwrap_or_default()
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
    fn final_after_the_table_and_its_alias() {
        let from = |src: &str| {
            parse(src)
                .unwrap_or_else(|e| panic!("{}", e.render(src)))
                .from
        };
        let f = from("select * from products final");
        assert_eq!((f.alias, f.final_span), (None, Some(Span::new(23, 28))));
        let f = from("select * from products p FINAL");
        assert_eq!(
            (f.alias.as_deref(), f.final_span),
            (Some("p"), Some(Span::new(25, 30)))
        );
        let f = from("select * from products as p final where p.x = 1");
        assert_eq!(f.alias.as_deref(), Some("p"));
        assert!(f.final_span.is_some());
        assert_eq!(from("select * from products").final_span, None);
        // still a column name elsewhere
        assert_eq!(columns("select final from t final"), ["final"]);
        let q = parse("select * from o where c in (select c from p final)").unwrap();
        let Some(Expr::InSubquery { query, .. }) = q.selection else {
            panic!("a sub-query")
        };
        assert!(query.from.final_span.is_some());
        assert_eq!(
            error("select * from t final final"),
            "expected end of query, found identifier `final` at 1:23"
        );
    }

    #[test]
    fn now_and_intervals() {
        assert_eq!(where_of("select * from t where ts > now()"), "(ts > now())");
        assert_eq!(
            where_of("select * from t where ts > NOW() - interval 1 hour"),
            "(ts > now( -1 HOUR))"
        );
        assert_eq!(
            where_of(
                "select * from t where now() - INTERVAL 1 HOUR - INTERVAL 30 MINUTE + INTERVAL 2 SECOND < ts"
            ),
            "(now( -1 HOUR -30 MINUTE +2 SECOND) < ts)"
        );
        assert_eq!(
            where_of("select * from t where ts >= now() + interval 1 week and x = 1"),
            "((ts >= now( +1 WEEK)) AND (x = Number(\"1\")))"
        );
        // a timezone, as in ClickHouse; the intervals in order
        assert_eq!(
            where_of("select * from t where ts > now('UTC')"),
            "(ts > now(UTC))"
        );
        assert_eq!(
            where_of(
                "select * from t where ts > now('Europe/Warsaw') - interval 1 day + interval 2 hour"
            ),
            "(ts > now(Europe/Warsaw -1 DAY +2 HOUR))"
        );
        for (src, want) in [
            (
                "select * from t where ts > now() - interval 1 month",
                "calendar intervals (MONTH, QUARTER, YEAR) aren't supported yet at 1:47",
            ),
            (
                "select * from t where ts > now(500)",
                "now()'s argument is a timezone, e.g. now('Europe/Warsaw') at 1:32",
            ),
            (
                "select * from t where ts > now(x)",
                "now()'s argument is a timezone, e.g. now('Europe/Warsaw') at 1:32",
            ),
            (
                "select * from t where ts > now('UTC', 1)",
                "expected `)`, found `,` at 1:37",
            ),
            (
                "select * from t where ts > now() - 5",
                "only an INTERVAL can be added to or subtracted from now() at 1:34",
            ),
            (
                "select * from t where ts > now() - interval 1 fortnight",
                "an INTERVAL's unit is MILLISECOND, SECOND, MINUTE, HOUR, DAY or WEEK at 1:47",
            ),
            (
                "select * from t where ts > lower(x)",
                "functions are not supported yet at 1:28",
            ),
        ] {
            assert_eq!(error(src), want, "{src}");
        }
        // still ordinary names elsewhere
        assert_eq!(columns("select now, interval from t"), ["now", "interval"]);
    }

    #[test]
    fn aliases_qualified_names_and_constants() {
        let q = parse("select e.ts, user_id from events e where e.x > 1 order by e.ts").unwrap();
        assert_eq!(q.from.alias.as_deref(), Some("e"));
        let Projection::Columns(cols) = &q.projection else {
            panic!("columns")
        };
        assert_eq!(
            (
                cols[0].qualifier.as_deref(),
                cols[0].name.as_str(),
                cols[0].span.start,
                cols[0].span.end
            ),
            (Some("e"), "ts", 7, 11)
        );
        assert_eq!(cols[1].qualifier, None);
        assert_eq!(
            parse("select * from events as ev")
                .unwrap()
                .from
                .alias
                .as_deref(),
            Some("ev")
        );
        assert_eq!(
            parse("select * from events where a = 1")
                .unwrap()
                .from
                .alias,
            None
        );
        let Projection::Items(items) = parse("select 1, 'x' from t").unwrap().projection else {
            panic!("items")
        };
        assert!(matches!(
            items[0].expr,
            SelectExpr::Literal(Literal::Number(_), _)
        ));
        assert_eq!(
            where_of("select * from events e where exists (select 1 from c where c.k = e.k)"),
            "(EXISTS (SELECT … FROM c WHERE (k = k)))"
        );
        assert_eq!(
            where_of("select * from e where not exists (select * from c)"),
            "(NOT EXISTS (SELECT … FROM c))"
        );
        assert_eq!(
            error("select * from e where exists c"),
            "expected `(` and a sub-query after EXISTS, found identifier `c` at 1:30"
        );
        assert_eq!(
            error("select * from e where exists (1)"),
            "expected a sub-query (SELECT …) after EXISTS, found number `1` at 1:31"
        );
        assert_eq!(
            error("select * from e as"),
            "expected an alias for the table, found end of input at 1:19"
        );
    }

    #[test]
    fn in_subqueries() {
        assert_eq!(
            where_of("select * from events where a > 1 and user_id in (select id from users where country = 'PL')"),
            "((a > Number(\"1\")) AND (user_id IN (SELECT … FROM users WHERE (country = String(\"PL\")))))"
        );
        assert_eq!(
            where_of("select * from e where k not in (select k from f)"),
            "(k NOT IN (SELECT … FROM f))"
        );
        assert_eq!(
            error("select * from e where k in (select k from f"),
            "expected `)` after the sub-query, found end of input at 1:44"
        );
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
            // a sub-query now (selecting a constant), missing its FROM
            error("select * from t where a in (select 1)"),
            "expected FROM, found `)` at 1:37"
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
    fn parameters_are_literals_in_where() {
        let q = parse("select * from t where id = {id:UUID} and n in (1, {n:UInt8})").unwrap();
        let Some(Expr::And(l, r)) = q.selection else {
            panic!("an AND")
        };
        assert!(matches!(*l, Expr::Compare { ref right, .. }
            if matches!(**right, Expr::Literal { value: Literal::Param { ref name, ref ty }, .. }
                if name == "id" && ty == "UUID")));
        assert!(matches!(*r, Expr::InList { ref list, .. }
            if matches!(list[1].0, Literal::Param { ref name, .. } if name == "n")));
        assert_eq!(
            error("select {x:UInt8} from t"),
            "parameters can only be used in WHERE at 1:8"
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

    fn order(src: &str) -> Order {
        parse(src)
            .unwrap_or_else(|e| panic!("{}", e.render(src)))
            .order
            .expect("an ORDER BY")
    }

    #[test]
    fn order_by_forms() {
        let item = |name: &str, d, n| OrderItem {
            expr: SelectExpr::Column(ColumnRef {
                qualifier: None,
                name: name.into(),
                span: Span::default(),
            }),
            direction: d,
            nulls: n,
        };
        let strip = |o: Order| match o {
            Order::Items(items) => Order::Items(
                items
                    .into_iter()
                    .map(|mut i| {
                        match &mut i.expr {
                            SelectExpr::Column(c) => c.span = Span::default(),
                            SelectExpr::Call { span, .. } | SelectExpr::Literal(_, span) => {
                                *span = Span::default()
                            }
                        }
                        i
                    })
                    .collect(),
            ),
            Order::All {
                direction, nulls, ..
            } => Order::All {
                direction,
                nulls,
                span: Span::default(),
            },
        };
        assert_eq!(
            strip(order(
                "select * from t where a > 1 order by a, b desc, c asc nulls first, d NULLS LAST"
            )),
            Order::Items(vec![
                item("a", None, None),
                item("b", Some(Direction::Desc), None),
                item("c", Some(Direction::Asc), Some(NullsOrder::First)),
                item("d", None, Some(NullsOrder::Last)),
            ])
        );
        assert_eq!(
            strip(order("SELECT a FROM t ORDER BY ALL DESC NULLS FIRST;")),
            Order::All {
                direction: Some(Direction::Desc),
                nulls: Some(NullsOrder::First),
                span: Span::default()
            }
        );
        // the ORDER BY words aren't reserved: they're still column names elsewhere
        assert_eq!(
            strip(order("select first, last, desc from t order by last desc")),
            Order::Items(vec![item("last", Some(Direction::Desc), None)])
        );
        assert!(parse("select * from t").unwrap().order.is_none());
    }

    #[test]
    fn order_by_rejections() {
        assert_eq!(
            error("select * from t order by 1"),
            "ordering by position is not supported yet; name the column at 1:26"
        );
        assert_eq!(
            error("select * from t order by a + 1"),
            "ordering by expressions is not supported yet; name a column at 1:28"
        );
        // calls are fine (the binder decides where); expressions around them aren't
        assert!(matches!(
            &parse("select * from t order by count() desc").unwrap().order,
            Some(Order::Items(items)) if matches!(&items[0].expr, SelectExpr::Call { name, .. } if name == "count")
                && items[0].direction == Some(Direction::Desc)
        ));
        assert_eq!(
            error("select * from t order by f(a) + 1"),
            "ordering by expressions is not supported yet; name a column at 1:31"
        );
        assert_eq!(
            error("select * from t order by a nulls"),
            "expected FIRST or LAST, found end of input at 1:33"
        );
        assert_eq!(
            error("select * from t order by a collate 'de'"),
            "COLLATE in ORDER BY is not supported at 1:28"
        );
        assert_eq!(
            error("select * from t order by a with fill"),
            "WITH in ORDER BY is not supported at 1:28"
        );
        assert_eq!(
            error("select * from t order by all, a"),
            "ORDER BY ALL can't be combined with other columns at 1:29"
        );
        assert_eq!(
            error("select * from t order by a,"),
            "expected a column name, found end of input at 1:28"
        );
        assert_eq!(
            error("select * from t order a"),
            "expected BY, found identifier `a` at 1:23"
        );
        // ORDER BY comes after WHERE
        assert_eq!(
            error("select * from t order by a where b = 1"),
            "expected end of query, found WHERE at 1:28"
        );
    }

    #[test]
    fn limit_forms_and_rejections() {
        let limit = |src: &str| parse(src).unwrap().limit.map(|l| l.value);
        assert_eq!(limit("select * from t limit 10"), Some(10));
        assert_eq!(
            limit("select * from t where a = 1 order by a desc limit 0;"),
            Some(0)
        );
        assert_eq!(limit("select * from t"), None);
        assert_eq!(
            error("select * from t limit -1"),
            "LIMIT needs a non-negative whole number at 1:23"
        );
        assert_eq!(
            error("select * from t limit 1.5"),
            "LIMIT needs a non-negative whole number at 1:23"
        );
        assert_eq!(
            error("select * from t limit n"),
            "LIMIT needs a non-negative whole number at 1:23"
        );
        assert_eq!(
            error("select * from t limit"),
            "LIMIT needs a non-negative whole number at 1:22"
        );
        // OFFSET: after LIMIT, alone, or ClickHouse's `LIMIT m, n` (the offset first)
        let both = |src: &str| {
            let q = parse(src).unwrap();
            (q.limit.map(|l| l.value), q.offset.map(|o| o.value))
        };
        assert_eq!(both("select * from t limit 2 offset 1"), (Some(2), Some(1)));
        assert_eq!(both("select * from t limit 1, 2"), (Some(2), Some(1)));
        assert_eq!(both("select * from t order by a offset 3"), (None, Some(3)));
        assert_eq!(both("select * from t offset 0"), (None, Some(0)));
        // OFFSET isn't taken as a table alias
        assert_eq!(both("select * from t offset 5").1, Some(5));
        assert_eq!(parse("select * from t offset 5").unwrap().from.alias, None);
        assert_eq!(
            error("select * from t limit 2 offset x"),
            "OFFSET needs a non-negative whole number at 1:32"
        );
        assert_eq!(
            error("select * from t limit 1, -2"),
            "LIMIT needs a non-negative whole number at 1:26"
        );
        assert_eq!(
            error("select * from t limit 1, 2 offset 3"),
            "expected end of query, found identifier `offset` at 1:28"
        );
        assert_eq!(
            error("select * from t offset 1 limit 2"),
            "expected end of query, found LIMIT at 1:26"
        );
        assert_eq!(
            error("select * from t order by a limit 2 with ties"),
            "LIMIT … WITH TIES is not supported yet at 1:36"
        );
        assert_eq!(
            error("select * from t limit 2 by a"),
            "LIMIT … BY is not supported yet at 1:25"
        );
        // LIMIT comes after ORDER BY
        assert_eq!(
            error("select * from t limit 2 order by a"),
            "expected end of query, found ORDER at 1:25"
        );
    }

    #[test]
    fn group_by_and_calls() {
        let q = parse("select country, count() as n, avg(latency_ms), count(*) from t where a = 1 group by country, path").unwrap();
        let (keys, _) = q.group_by.unwrap();
        assert_eq!(
            keys.iter().map(|k| k.name.as_str()).collect::<Vec<_>>(),
            ["country", "path"]
        );
        let Projection::Items(items) = q.projection else {
            panic!("items")
        };
        assert_eq!(items.len(), 4);
        assert_eq!(items[1].alias.as_deref(), Some("n"));
        assert!(
            matches!(&items[1].expr, SelectExpr::Call { name, arg: CallArg::None, .. } if name == "count")
        );
        assert!(
            matches!(&items[2].expr, SelectExpr::Call { arg: CallArg::Column(c), .. } if c.name == "latency_ms")
        );
        assert!(matches!(
            &items[3].expr,
            SelectExpr::Call {
                arg: CallArg::Star,
                ..
            }
        ));
        // a plain list stays a column list
        assert!(matches!(
            parse("select a, b from t group by a, b")
                .unwrap()
                .projection,
            Projection::Columns(_)
        ));
        assert_eq!(
            error("select count() from t group by 1"),
            "grouping by position is not supported yet; name the column at 1:32"
        );
        assert_eq!(
            error("select count() from t group by all"),
            "GROUP BY ALL is not supported yet; name the columns at 1:32"
        );
        assert_eq!(
            error("select sum(a, b) from t"),
            "functions with more than one argument are not supported yet at 1:13"
        );
        assert_eq!(
            error("select sum(1) from t"),
            "function arguments other than a column are not supported yet at 1:12"
        );
        assert_eq!(
            error("select count() as from t"),
            "expected an alias, found FROM at 1:19"
        );
        assert_eq!(
            error("select count() from t group by a having count() > 1"),
            "HAVING is not supported yet at 1:34"
        );
    }
}
