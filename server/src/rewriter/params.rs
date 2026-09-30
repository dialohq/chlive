//! ClickHouse query-parameter placeholders (`{name:Type}`).
//!
//! `sqlparser` parses `{name:Type}` as a dictionary literal, which breaks for types such as
//! `Map(String, UInt8)`. Placeholders are therefore swapped for plain identifiers
//! (`__p_<name>`) before parsing and restored in the generated SQL. See NOTES.md.

use std::collections::BTreeMap;

use serde_json::Value;

use super::RewriteError;

pub const PREFIX: &str = "__p_";

/// Declared placeholder name → ClickHouse type (as written in the SQL).
pub type ParamTypes = BTreeMap<String, String>;

/// Replace every `{name:Type}` outside string literals, quoted identifiers and comments with
/// `__p_name`. Returns the rewritten SQL and the declared types.
pub fn extract(sql: &str) -> Result<(String, ParamTypes), RewriteError> {
    let mut out = String::with_capacity(sql.len());
    let mut types = ParamTypes::new();
    let mut scanner = Scanner::new(sql);
    while let Some(tok) = scanner.next() {
        match tok {
            Tok::Opaque(s) => out.push_str(s),
            Tok::Code(c) if c == '{' => {
                let (name, ty) = scanner.placeholder()?;
                if let Some(prev) = types.get(&name) {
                    if *prev != ty {
                        return Err(RewriteError::unsupported(format!(
                            "parameter {{{name}}} is declared with two types: {prev} and {ty}"
                        )));
                    }
                }
                out.push_str(PREFIX);
                out.push_str(&name);
                types.insert(name, ty);
            }
            Tok::Code(c) => out.push(c),
        }
    }
    Ok((out, types))
}

/// Inverse of [`extract`]: turn `__p_name` identifiers back into `{name:Type}`.
pub fn restore(sql: &str, types: &ParamTypes) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut scanner = Scanner::new(sql);
    let mut prev_ident = false;
    while let Some(tok) = scanner.next() {
        match tok {
            Tok::Opaque(s) => {
                out.push_str(s);
                prev_ident = false;
            }
            Tok::Code(c) => {
                if !prev_ident && c == '_' && scanner.rest().starts_with(&PREFIX[1..]) {
                    let word = scanner.take_ident_tail();
                    let full = format!("_{word}");
                    match full.strip_prefix(PREFIX).and_then(|n| types.get_key_value(n)) {
                        Some((name, ty)) => out.push_str(&format!("{{{name}:{ty}}}")),
                        None => out.push_str(&full),
                    }
                    prev_ident = true;
                    continue;
                }
                prev_ident = c.is_ascii_alphanumeric() || c == '_';
                out.push(c);
            }
        }
    }
    out
}

/// Convert client-supplied JSON param values into the textual form ClickHouse expects for
/// `param_<name>`, checking that the set of names matches the declared placeholders exactly.
pub fn bind(
    types: &ParamTypes,
    values: &serde_json::Map<String, Value>,
) -> Result<BTreeMap<String, String>, RewriteError> {
    for name in values.keys() {
        if !types.contains_key(name) {
            return Err(RewriteError::params(format!(
                "parameter '{name}' is not used by the query"
            )));
        }
    }
    let mut bound = BTreeMap::new();
    for name in types.keys() {
        let v = values
            .get(name)
            .ok_or_else(|| RewriteError::params(format!("missing parameter '{name}'")))?;
        let text = match v {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => {
                return Err(RewriteError::params(format!(
                    "parameter '{name}' must be a string, number or boolean"
                )))
            }
        };
        bound.insert(name.clone(), text);
    }
    Ok(bound)
}

enum Tok<'a> {
    /// A string literal, quoted identifier or comment, passed through untouched.
    Opaque(&'a str),
    Code(char),
}

struct Scanner<'a> {
    src: &'a str,
    pos: usize,
}

impl<'a> Scanner<'a> {
    fn new(src: &'a str) -> Self {
        Self { src, pos: 0 }
    }

    fn rest(&self) -> &'a str {
        &self.src[self.pos..]
    }

    fn next(&mut self) -> Option<Tok<'a>> {
        let rest = self.rest();
        let c = rest.chars().next()?;
        let opaque_len = match c {
            '\'' | '"' | '`' => Some(quoted_len(rest, c)),
            '-' if rest.starts_with("--") => Some(rest.find('\n').unwrap_or(rest.len())),
            '/' if rest.starts_with("/*") => Some(rest.find("*/").map_or(rest.len(), |i| i + 2)),
            _ => None,
        };
        match opaque_len {
            Some(n) => {
                self.pos += n;
                Some(Tok::Opaque(&rest[..n]))
            }
            None => {
                self.pos += c.len_utf8();
                Some(Tok::Code(c))
            }
        }
    }

    fn take_ident_tail(&mut self) -> &'a str {
        let rest = self.rest();
        let n = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        self.pos += n;
        &rest[..n]
    }

    /// Called right after a `{`; consumes `name:Type}`.
    fn placeholder(&mut self) -> Result<(String, String), RewriteError> {
        let bad = || RewriteError::unsupported("malformed parameter placeholder; expected {name:Type}");
        let rest = self.rest();
        let name_len = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        let name = rest[..name_len].to_string();
        if name.is_empty() || name.starts_with(|c: char| c.is_ascii_digit()) {
            return Err(bad());
        }
        let after = rest[name_len..].trim_start();
        let after = after.strip_prefix(':').ok_or_else(bad)?;
        let mut depth = 0i32;
        let mut end = None;
        for (i, c) in after.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                '}' if depth == 0 => {
                    end = Some(i);
                    break;
                }
                '\'' | '{' => return Err(bad()),
                _ => {}
            }
        }
        let end = end.ok_or_else(bad)?;
        let ty = after[..end].trim().to_string();
        if ty.is_empty() {
            return Err(bad());
        }
        let consumed = rest.len() - after.len() + end + 1;
        self.pos += consumed;
        Ok((name, ty))
    }
}

/// Length of a quoted token starting at `s[0] == q`, honoring backslash escapes and doubled quotes.
fn quoted_len(s: &str, q: char) -> usize {
    let bytes = s.as_bytes();
    let q = q as u8;
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b if b == q => {
                if bytes.get(i + 1) == Some(&q) {
                    i += 2;
                } else {
                    return i + 1;
                }
            }
            _ => i += 1,
        }
    }
    s.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_and_restores() {
        let sql = "SELECT 1 FROM t WHERE a = {a:String} AND ts >= {since: DateTime64(3)} AND m = {m:Map(String, UInt8)} AND s = '{not:Param}' AND b = {a:String}";
        let (out, types) = extract(sql).unwrap();
        assert_eq!(
            out,
            "SELECT 1 FROM t WHERE a = __p_a AND ts >= __p_since AND m = __p_m AND s = '{not:Param}' AND b = __p_a"
        );
        assert_eq!(types["since"], "DateTime64(3)");
        assert_eq!(types["m"], "Map(String, UInt8)");
        assert_eq!(
            restore(&out, &types),
            "SELECT 1 FROM t WHERE a = {a:String} AND ts >= {since:DateTime64(3)} AND m = {m:Map(String, UInt8)} AND s = '{not:Param}' AND b = {a:String}"
        );
    }

    #[test]
    fn restore_leaves_strings_and_unknown_idents() {
        let types = ParamTypes::from([("x".into(), "UInt8".into())]);
        assert_eq!(
            restore("a__p_x = '__p_x' AND __p_y = __p_x", &types),
            "a__p_x = '__p_x' AND __p_y = {x:UInt8}"
        );
    }

    #[test]
    fn conflicting_types_rejected() {
        assert!(extract("SELECT {a:String}, {a:UInt8}").is_err());
        assert!(extract("SELECT {a}").is_err());
    }

    #[test]
    fn bind_checks_names() {
        let types = ParamTypes::from([("path".into(), "String".into())]);
        let ok = serde_json::json!({"path": "/x"});
        assert_eq!(bind(&types, ok.as_object().unwrap()).unwrap()["path"], "/x");
        let extra = serde_json::json!({"path": "/x", "y": 1});
        assert!(bind(&types, extra.as_object().unwrap()).is_err());
        assert!(bind(&types, &serde_json::Map::new()).is_err());
    }
}
