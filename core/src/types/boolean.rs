//! `Bool`: values are `bool`.

use std::cmp::Ordering;

use serde_json::Value as Json;

use super::{Class, ColumnType, Literal, LiteralError, Ops, Scalar, Type, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoolType;

impl ColumnType for BoolType {
    /// `true`/`false`, or the numbers 0 and 1.
    fn decode(&self, json: &Json) -> Option<Value> {
        match json {
            Json::Bool(b) => Some(Value::Bool(*b)),
            Json::Number(n) if n.as_u64() == Some(0) => Some(Value::Bool(false)),
            Json::Number(n) if n.as_u64() == Some(1) => Some(Value::Bool(true)),
            _ => None,
        }
    }

    fn literal(&self, lit: &Literal, _: &Type) -> Result<Value, LiteralError> {
        match lit {
            Literal::Bool(b) => Ok(Value::Bool(*b)),
            _ => Err(LiteralError::Mismatch),
        }
    }

    fn zero(&self) -> Value {
        Value::Bool(false)
    }

    fn class(&self) -> Option<Class> {
        Some(Class::Bool)
    }

    fn ops(&self) -> Ops {
        Ops::ALL
    }
}

impl Scalar for bool {
    /// `false` before `true`.
    fn compare(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }

    fn to_json(&self) -> Json {
        Json::Bool(*self)
    }

    fn to_sql(&self) -> String {
        self.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn false_sorts_first() {
        assert_eq!(
            Value::Bool(false).sort_cmp(&Value::Bool(true), false, false),
            Ordering::Less
        );
    }
}
