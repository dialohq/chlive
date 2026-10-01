//! Syntax tree of the chlive dialect. Deliberately small: it grows with the dialect.

use crate::error::Span;

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub projection: Projection,
    pub from: TableRef,
    /// The WHERE condition.
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// `*`: every column of the table.
    Wildcard,
    /// `a, b, …`: these columns, in this order.
    Columns(Vec<ColumnRef>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    pub name: String,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name: String,
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
            | Expr::InList { span, .. } => *span,
            Expr::And(l, _) | Expr::Or(l, _) => l.span(),
        }
    }
}
