use std::fmt;

/// Byte range in the query text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

/// A lexing or parsing error, pointing at the offending part of the query.
#[derive(Debug, Clone, PartialEq)]
pub struct SyntaxError {
    pub message: String,
    pub span: Span,
}

impl SyntaxError {
    pub fn new(message: impl Into<String>, span: Span) -> Self {
        Self {
            message: message.into(),
            span,
        }
    }

    /// `message at line:col`, both 1-based.
    pub fn render(&self, src: &str) -> String {
        let before = &src[..self.span.start.min(src.len())];
        let line = before.matches('\n').count() + 1;
        let col = before.rsplit('\n').next().map_or(0, |l| l.chars().count()) + 1;
        format!("{} at {line}:{col}", self.message)
    }
}

impl fmt::Display for SyntaxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at byte {}", self.message, self.span.start)
    }
}

impl std::error::Error for SyntaxError {}
