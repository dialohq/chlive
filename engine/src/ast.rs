//! Syntax tree of the chlive dialect. Deliberately small: it grows with the dialect.

use crate::error::Span;

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub projection: Projection,
    pub from: TableRef,
    /// The WHERE condition.
    pub selection: Option<Expr>,
    /// `GROUP BY col, …`; `span` is the `GROUP`'s.
    pub group_by: Option<(Vec<ColumnRef>, Span)>,
    /// The ORDER BY clause.
    pub order: Option<Order>,
    /// `LIMIT n`.
    pub limit: Option<Limit>,
    /// `OFFSET m` (also `LIMIT m, n`): the rows the result starts after.
    pub offset: Option<Limit>,
}

/// `LIMIT n`; `span` is the number's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limit {
    pub value: u64,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Order {
    /// `ORDER BY ALL`: every selected column, in SELECT order. `span` is the `ALL`'s.
    All {
        direction: Option<Direction>,
        nulls: Option<NullsOrder>,
        span: Span,
    },
    Items(Vec<OrderItem>),
}

/// `column|name(arg) [ASC|DESC] [NULLS FIRST|LAST]` (calls: with GROUP BY or aggregates)
#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: SelectExpr,
    pub direction: Option<Direction>,
    pub nulls: Option<NullsOrder>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// `*`: every column of the table.
    Wildcard,
    /// `a, b, …`: these columns, in this order.
    Columns(Vec<ColumnRef>),
    /// A list with function calls or aliases, e.g. `country, count() AS n, avg(latency_ms)`.
    Items(Vec<SelectItem>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectItem {
    pub expr: SelectExpr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectExpr {
    Column(ColumnRef),
    /// A constant (`SELECT 1`): only in EXISTS sub-queries, which ignore their SELECT list.
    Literal(Literal, Span),
    /// `name(arg)`, `name()` or `name(*)`; `span` is the name's.
    Call {
        name: String,
        arg: CallArg,
        span: Span,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum CallArg {
    None,
    Star,
    Column(ColumnRef),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    /// `e` in `e.user_id`: a table or its alias.
    pub qualifier: Option<String>,
    pub name: String,
    /// The whole reference (`e.user_id`).
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name: String,
    /// `FROM events e` / `FROM events AS e`.
    pub alias: Option<String>,
    /// `FROM events FINAL`: where the word is.
    pub final_span: Option<Span>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl CmpOp {
    /// The same comparison with its sides swapped: `a < b` is `b > a`.
    pub fn flipped(self) -> CmpOp {
        match self {
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::LtEq => CmpOp::GtEq,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::GtEq => CmpOp::LtEq,
            op => op,
        }
    }

    pub fn symbol(self) -> &'static str {
        match self {
            CmpOp::Eq => "=",
            CmpOp::NotEq => "!=",
            CmpOp::Lt => "<",
            CmpOp::LtEq => "<=",
            CmpOp::Gt => ">",
            CmpOp::GtEq => ">=",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// As written, including a leading `-`.
    Number(String),
    String(String),
    Bool(bool),
    Null,
    /// A query parameter, `{name:Type}`: its value comes with the query, typed as declared.
    Param {
        name: String,
        ty: String,
    },
    /// A parameter with its value, as the binder resolves `Param`s before binding.
    Bound {
        name: String,
        ty: crate::types::Type,
        value: crate::types::Value,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(ColumnRef),
    Literal {
        value: Literal,
        span: Span,
    },
    /// `span` is the operator's.
    Compare {
        op: CmpOp,
        left: Box<Expr>,
        right: Box<Expr>,
        span: Span,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not {
        expr: Box<Expr>,
        span: Span,
    },
    /// `expr IS [NOT] NULL`; `span` is the `IS`'s.
    IsNull {
        expr: Box<Expr>,
        negated: bool,
        span: Span,
    },
    /// `expr [NOT] IN (literal, …)`; `span` is the `IN`'s.
    InList {
        expr: Box<Expr>,
        list: Vec<(Literal, Span)>,
        negated: bool,
        span: Span,
    },
    /// `[NOT] EXISTS (SELECT …)`; `span` is the `EXISTS`'s.
    Exists {
        query: Box<Query>,
        negated: bool,
        span: Span,
    },
    /// `now(['timezone']) [± INTERVAL n unit]…`: the server's clock, with the intervals as
    /// written (DAY and WEEK go by the calendar in the timezone); `span` is the `now`'s.
    Now {
        tz: Option<String>,
        intervals: Vec<crate::calendar::Interval>,
        span: Span,
    },
    /// `expr [NOT] IN (SELECT …)`; `span` is the `IN`'s.
    InSubquery {
        expr: Box<Expr>,
        query: Box<Query>,
        negated: bool,
        span: Span,
    },
}

impl Expr {
    /// Where to point errors about this expression.
    pub fn span(&self) -> Span {
        match self {
            Expr::Column(c) => c.span,
            Expr::Literal { span, .. }
            | Expr::Compare { span, .. }
            | Expr::Not { span, .. }
            | Expr::IsNull { span, .. }
            | Expr::InList { span, .. }
            | Expr::Exists { span, .. }
            | Expr::Now { span, .. }
            | Expr::InSubquery { span, .. } => *span,
            Expr::And(l, _) | Expr::Or(l, _) => l.span(),
        }
    }
}
