//! `Float32`, `Float64`: values are `f64`.

use std::cmp::Ordering;

use serde_json::Value as Json;

use super::{Class, ColumnType, Literal, LiteralError, Ops, Scalar, Type, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloatType;

impl ColumnType for FloatType {
    /// A number, a quoted one (no spaces around it; `nan`, `inf` and exponents too), or a
    /// boolean (also quoted) as 1 or 0.
    fn decode(&self, json: &Json) -> Option<Value> {
        match json {
            Json::Number(n) => n.as_f64().map(Value::Float),
            Json::String(s) => match s.as_str() {
                "true" => Some(Value::Float(1.0)),
                "false" => Some(Value::Float(0.0)),
                s => s.parse().ok().map(Value::Float),
            },
            Json::Bool(b) => Some(Value::Float(f64::from(u8::from(*b)))),
            _ => None,
        }
    }

    fn literal(&self, lit: &Literal, _: &Type) -> Result<Value, LiteralError> {
        match lit {
            Literal::Number(n) => n
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| LiteralError::Mismatch),
            _ => Err(LiteralError::Mismatch),
        }
    }

    fn zero(&self) -> Value {
        Value::Float(0.0)
    }

    fn class(&self) -> Option<Class> {
        Some(Class::Number)
    }

    fn ops(&self) -> Ops {
        Ops::ALL
    }
}

impl Scalar for f64 {
    /// `None` for NaN, which ORDER BY places on its own (`Value::sort_cmp`); `-0` equals `0`.
    fn compare(&self, other: &Self) -> Option<Ordering> {
        self.partial_cmp(other)
    }

    fn to_json(&self) -> Json {
        if self.is_finite() {
            // `{}` prints integral floats without a fraction, as ClickHouse does (`0`, not `0.0`).
            format!("{self}")
                .parse()
                .map(Json::Number)
                .unwrap_or(Json::Null)
        } else if self.is_nan() {
            // as ClickHouse renders them with output_format_json_quote_denormals=1
            Json::String("nan".into())
        } else if *self > 0.0 {
            Json::String("inf".into())
        } else {
            Json::String("-inf".into())
        }
    }

    fn to_sql(&self) -> String {
        format!("{self}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decode;
    use serde_json::json;

    #[test]
    fn decodes_quoted_numbers() {
        assert_eq!(
            decode(Some(&json!("2.5")), &Type::parse("Float64")).unwrap(),
            Value::Float(2.5)
        );
        let f = |v: Json| decode(Some(&v), &Type::parse("Float64"));
        assert_eq!(f(json!(true)).unwrap(), Value::Float(1.0));
        assert_eq!(f(json!("false")).unwrap(), Value::Float(0.0));
        assert_eq!(f(json!("1e3")).unwrap(), Value::Float(1000.0));
        assert!(f(json!(" 7")).is_err());
    }

    #[test]
    fn renders_like_clickhouse_output() {
        assert_eq!(Value::Float(0.0).to_json().to_string(), "0");
        assert_eq!(Value::Float(129.99).to_json().to_string(), "129.99");
        assert_eq!(Value::Float(f64::NAN).to_json(), json!("nan"));
        assert_eq!(Value::Float(f64::NEG_INFINITY).to_json(), json!("-inf"));
    }
}
