//! Syntax tree of the chlive dialect. Deliberately small: it grows with the dialect.

use crate::error::Span;

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub projection: Projection,
    pub from: TableRef,
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
