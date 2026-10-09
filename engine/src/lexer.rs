//! Tokens of the chlive dialect.

use crate::error::{Span, SyntaxError};

/// Words the dialect reserves. Some are not used by the grammar yet; they are recognised so
/// that queries using them get a clear "not supported yet" error instead of being mistaken for
/// identifiers.
pub const KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "LIMIT", "JOIN", "UNION", "AS",
    "AND", "OR", "NOT", "IS", "IN", "NULL", "TRUE", "FALSE", "LIKE", "ILIKE", "BETWEEN", "EXISTS",
];

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    /// A reserved word, upper-cased.
    Keyword(&'static str),
    /// An identifier; quoted identifiers have their quotes removed.
    Ident(String),
    /// A numeric literal, as written.
    Number(String),
    /// A single-quoted string literal, unescaped (`''` and `\'` are quotes).
    String(String),
    Star,
    Plus,
    Minus,
    Slash,
    Eq,
    /// `!=` or `<>`
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    LParen,
    RParen,
    Comma,
    Semicolon,
    Dot,
    Eof,
}

impl TokenKind {
    pub fn describe(&self) -> String {
        match self {
            TokenKind::Keyword(k) => k.to_string(),
            TokenKind::Ident(i) => format!("identifier `{i}`"),
            TokenKind::Number(n) => format!("number `{n}`"),
            TokenKind::String(v) => format!("string '{v}'"),
            TokenKind::Star => "`*`".into(),
            TokenKind::Plus => "`+`".into(),
            TokenKind::Minus => "`-`".into(),
            TokenKind::Slash => "`/`".into(),
            TokenKind::Eq => "`=`".into(),
            TokenKind::NotEq => "`!=`".into(),
            TokenKind::Lt => "`<`".into(),
            TokenKind::LtEq => "`<=`".into(),
            TokenKind::Gt => "`>`".into(),
            TokenKind::GtEq => "`>=`".into(),
            TokenKind::LParen => "`(`".into(),
            TokenKind::RParen => "`)`".into(),
            TokenKind::Comma => "`,`".into(),
            TokenKind::Semicolon => "`;`".into(),
            TokenKind::Dot => "`.`".into(),
            TokenKind::Eof => "end of input".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

pub fn tokenize(src: &str) -> Result<Vec<Token>, SyntaxError> {
    let bytes = src.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        let start = i;
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'=' | b'!' | b'<' | b'>' => {
                let next = bytes.get(i + 1).copied();
                let (kind, len) = match (c, next) {
                    (b'=', _) => (TokenKind::Eq, 1),
                    (b'!', Some(b'=')) => (TokenKind::NotEq, 2),
                    (b'<', Some(b'=')) => (TokenKind::LtEq, 2),
                    (b'<', Some(b'>')) => (TokenKind::NotEq, 2),
                    (b'<', _) => (TokenKind::Lt, 1),
                    (b'>', Some(b'=')) => (TokenKind::GtEq, 2),
                    (b'>', _) => (TokenKind::Gt, 1),
                    _ => {
                        return Err(SyntaxError::new(
                            "unexpected character `!`",
                            Span::new(start, start + 1),
                        ))
                    }
                };
                i += len;
                tokens.push(Token {
                    kind,
                    span: Span::new(start, i),
                });
            }
            b'-' | b'+' | b'/' => {
                let kind = match c {
                    b'-' => TokenKind::Minus,
                    b'+' => TokenKind::Plus,
                    _ => TokenKind::Slash,
                };
                i += 1;
                tokens.push(Token {
                    kind,
                    span: Span::new(start, i),
                });
            }
            b'*' | b'(' | b')' | b',' | b';' | b'.' => {
                let kind = match c {
                    b'*' => TokenKind::Star,
                    b'(' => TokenKind::LParen,
                    b')' => TokenKind::RParen,
                    b',' => TokenKind::Comma,
                    b';' => TokenKind::Semicolon,
                    _ => TokenKind::Dot,
                };
                i += 1;
                tokens.push(Token {
                    kind,
                    span: Span::new(start, i),
                });
            }
            b'"' | b'`' => {
                let quote = c;
                i += 1;
                let body_start = i;
                while i < bytes.len() && bytes[i] != quote {
                    i += 1;
                }
                if i >= bytes.len() {
                    return Err(SyntaxError::new(
                        "unterminated quoted identifier",
                        Span::new(start, bytes.len()),
                    ));
                }
                let name = &src[body_start..i];
                i += 1;
                if name.is_empty() {
                    return Err(SyntaxError::new(
                        "empty quoted identifier",
                        Span::new(start, i),
                    ));
                }
                tokens.push(Token {
                    kind: TokenKind::Ident(name.to_string()),
                    span: Span::new(start, i),
                });
            }
            b'0'..=b'9' => {
                while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
                    i += 1;
                }
                tokens.push(Token {
                    kind: TokenKind::Number(src[start..i].to_string()),
                    span: Span::new(start, i),
                });
            }
            b'\'' => {
                i += 1;
                let mut value = String::new();
                loop {
                    match bytes.get(i) {
                        None => {
                            return Err(SyntaxError::new(
                                "unterminated string literal",
                                Span::new(start, bytes.len()),
                            ))
                        }
                        Some(b'\\') if i + 1 < bytes.len() => {
                            let ch = src[i + 1..].chars().next().expect("in bounds");
                            value.push(ch);
                            i += 1 + ch.len_utf8();
                        }
                        Some(b'\'') if bytes.get(i + 1) == Some(&b'\'') => {
                            value.push('\'');
                            i += 2;
                        }
                        Some(b'\'') => {
                            i += 1;
                            break;
                        }
                        Some(_) => {
                            let ch = src[i..].chars().next().expect("in bounds");
                            value.push(ch);
                            i += ch.len_utf8();
                        }
                    }
                }
                tokens.push(Token {
                    kind: TokenKind::String(value),
                    span: Span::new(start, i),
                });
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let word = &src[start..i];
                let upper = word.to_ascii_uppercase();
                let kind = match KEYWORDS.iter().find(|k| **k == upper) {
                    Some(k) => TokenKind::Keyword(k),
                    None => TokenKind::Ident(word.to_string()),
                };
                tokens.push(Token {
                    kind,
                    span: Span::new(start, i),
                });
            }
            _ => {
                let ch = src[i..].chars().next().expect("in bounds");
                return Err(SyntaxError::new(
                    format!("unexpected character `{ch}`"),
                    Span::new(start, start + ch.len_utf8()),
                ));
            }
        }
    }
    tokens.push(Token {
        kind: TokenKind::Eof,
        span: Span::new(src.len(), src.len()),
    });
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(src: &str) -> Vec<TokenKind> {
        tokenize(src).unwrap().into_iter().map(|t| t.kind).collect()
    }

    #[test]
    fn keywords_are_case_insensitive_and_idents_keep_case() {
        assert_eq!(
            kinds("sElEcT * FrOm Events"),
            [
                TokenKind::Keyword("SELECT"),
                TokenKind::Star,
                TokenKind::Keyword("FROM"),
                TokenKind::Ident("Events".into()),
                TokenKind::Eof
            ]
        );
    }

    #[test]
    fn quoted_identifiers_and_comments() {
        assert_eq!(
            kinds("select -- all of it\n * from \"select\";"),
            [
                TokenKind::Keyword("SELECT"),
                TokenKind::Star,
                TokenKind::Keyword("FROM"),
                TokenKind::Ident("select".into()),
                TokenKind::Semicolon,
                TokenKind::Eof
            ]
        );
        assert_eq!(kinds("`my table`")[0], TokenKind::Ident("my table".into()));
    }

    #[test]
    fn operators() {
        assert_eq!(
            kinds("a = 1 != 2 <> 3 < <= > >= - -- comment\n+ /"),
            [
                TokenKind::Ident("a".into()),
                TokenKind::Eq,
                TokenKind::Number("1".into()),
                TokenKind::NotEq,
                TokenKind::Number("2".into()),
                TokenKind::NotEq,
                TokenKind::Number("3".into()),
                TokenKind::Lt,
                TokenKind::LtEq,
                TokenKind::Gt,
                TokenKind::GtEq,
                TokenKind::Minus,
                TokenKind::Plus,
                TokenKind::Slash,
                TokenKind::Eof
            ]
        );
        assert_eq!(
            kinds("is NULL true")[..3],
            [
                TokenKind::Keyword("IS"),
                TokenKind::Keyword("NULL"),
                TokenKind::Keyword("TRUE")
            ]
        );
        assert!(tokenize("a ! b").is_err());
    }

    #[test]
    fn literals() {
        assert_eq!(
            kinds("1 2.5 'it''s' 'a\\'b'"),
            [
                TokenKind::Number("1".into()),
                TokenKind::Number("2.5".into()),
                TokenKind::String("it's".into()),
                TokenKind::String("a'b".into()),
                TokenKind::Eof
            ]
        );
        assert!(tokenize("'open").is_err());
    }

    #[test]
    fn errors_have_positions() {
        let e = tokenize("select # from x").unwrap_err();
        assert_eq!(e.span, Span::new(7, 8));
        assert!(tokenize("select * from \"oops").is_err());
    }
}
