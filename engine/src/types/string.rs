//! `String`, and `FixedString`/`Enum8`/`Enum16`, which ClickHouse reads and writes as strings.

use std::cmp::Ordering;

use serde_json::Value as Json;

use super::{quote_string, Class, ColumnType, Literal, LiteralError, Ops, Scalar, Type, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringType;

impl ColumnType for StringType {
    /// A string; a number or a boolean is read as its text.
    fn decode(&self, json: &Json) -> Option<Value> {
        match json {
            Json::String(s) => Some(Value::String(s.clone())),
            Json::Number(n) => Some(Value::String(n.to_string())),
            Json::Bool(b) => Some(Value::String(b.to_string())),
            _ => None,
        }
    }

    fn literal(&self, lit: &Literal, _: &Type) -> Result<Value, LiteralError> {
        match lit {
            Literal::String(s) => Ok(Value::String(s.clone())),
            _ => Err(LiteralError::Mismatch),
        }
    }

    fn zero(&self) -> Value {
        Value::String(String::new())
    }

    fn class(&self) -> Option<Class> {
        Some(Class::String)
    }

    fn ops(&self) -> Ops {
        Ops::ALL
    }
}

impl Scalar for String {
    /// Byte-wise, like ClickHouse.
    fn compare(&self, other: &Self) -> Option<Ordering> {
        Some(self.as_bytes().cmp(other.as_bytes()))
    }

    fn to_json(&self) -> Json {
        Json::String(self.clone())
    }

    fn to_sql(&self) -> String {
        quote_string(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decode;
    use serde_json::json;

    #[test]
    fn numbers_are_read_as_text() {
        assert_eq!(
            decode(Some(&json!(7)), &Type::parse("String")).unwrap(),
            Value::String("7".into())
        );
    }

    #[test]
    fn orders_by_bytes() {
        // '' < 'B' < 'Z' < 'a' < 'aa' < 'é'
        let mut s: Vec<Value> = ["a", "B", "", "é", "Z", "aa"]
            .iter()
            .map(|s| Value::String(s.to_string()))
            .collect();
        s.sort_by(|a, b| a.sort_cmp(b, false, false));
        let s: Vec<String> = s.iter().map(|v| v.to_json().to_string()).collect();
        assert_eq!(s.join(" "), r#""" "B" "Z" "a" "aa" "é""#);
    }

    #[test]
    fn sql_literals_are_escaped() {
        assert_eq!(
            Value::String("it's \\ ok".into()).to_clickhouse_sql(),
            r"'it\'s \\ ok'"
        );
    }
}
