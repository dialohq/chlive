//! `Int8`…`Int64`, `UInt8`…`UInt64`: values are `i128`, which holds all of them.

use std::cmp::Ordering;

use serde_json::Value as Json;

use super::{Class, ColumnType, Literal, LiteralError, Ops, Scalar, Type, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntType {
    pub signed: bool,
    pub bits: u8,
}

impl ColumnType for IntType {
    /// An integer, a quoted one (no spaces around it), or a boolean (also quoted). Like
    /// ClickHouse, a value outside the type's range wraps around, except that unsigned types
    /// reject negative values.
    fn decode(&self, json: &Json) -> Option<Value> {
        let n: i128 = match json {
            Json::Number(n) => n
                .as_i64()
                .map(i128::from)
                .or_else(|| n.as_u64().map(i128::from))?,
            Json::String(s) => match s.as_str() {
                "true" => 1,
                "false" => 0,
                s => s.parse().ok()?,
            },
            Json::Bool(b) => i128::from(*b),
            _ => return None,
        };
        if !self.signed && n < 0 {
            return None;
        }
        Some(Value::Int(self.wrap(n)))
    }

    /// A number. A value outside the column's range is fine: it simply never (or always)
    /// matches, as in ClickHouse, which compares integers of different types exactly. A
    /// fraction makes it a Float.
    fn literal(&self, lit: &Literal, _: &Type) -> Result<Value, LiteralError> {
        let Literal::Number(n) = lit else {
            return Err(LiteralError::Mismatch);
        };
        if n.contains('.') {
            return n
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| LiteralError::Mismatch);
        }
        n.parse::<i128>()
            .map(Value::Int)
            .map_err(|_| LiteralError::Invalid(format!("number {n} is too large")))
    }

    fn zero(&self) -> Value {
        Value::Int(0)
    }

    fn class(&self) -> Option<Class> {
        Some(Class::Number)
    }

    fn ops(&self) -> Ops {
        Ops::ALL
    }
}

impl IntType {
    /// `n` modulo 2^bits, as the type's two's complement reads it.
    fn wrap(&self, n: i128) -> i128 {
        let m = n.rem_euclid(1i128 << self.bits);
        if self.signed && m >= 1i128 << (self.bits - 1) {
            m - (1i128 << self.bits)
        } else {
            m
        }
    }
}

impl Scalar for i128 {
    fn compare(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }

    fn to_json(&self) -> Json {
        Json::Number(self.to_string().parse().unwrap_or_else(|_| 0.into()))
    }

    fn to_sql(&self) -> String {
        self.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decode;
    use serde_json::json;

    #[test]
    fn decodes_like_clickhouse_input() {
        let u32 = Type::parse("UInt32");
        assert_eq!(decode(Some(&json!(42)), &u32).unwrap(), Value::Int(42));
        assert_eq!(decode(Some(&json!("42")), &u32).unwrap(), Value::Int(42));
        assert_eq!(decode(Some(&json!("false")), &u32).unwrap(), Value::Int(0));
        assert!(decode(Some(&json!(-1)), &u32).is_err());
        assert!(decode(Some(&json!("-1")), &u32).is_err());
        assert!(decode(Some(&json!("abc")), &u32).is_err());
        assert!(decode(Some(&json!(" 42")), &u32).is_err());
        assert!(decode(Some(&json!(1.5)), &u32).is_err());
    }

    #[test]
    fn out_of_range_values_wrap_around() {
        let int = |t: &str, v: Json| decode(Some(&v), &Type::parse(t)).unwrap();
        assert_eq!(int("UInt32", json!(4294967296u64)), Value::Int(0));
        assert_eq!(int("UInt8", json!(u64::MAX)), Value::Int(255));
        assert_eq!(int("UInt8", json!("256")), Value::Int(0));
        assert_eq!(int("Int8", json!(128)), Value::Int(-128));
        assert_eq!(int("Int8", json!(-129)), Value::Int(127));
        assert_eq!(int("Int64", json!(u64::MAX)), Value::Int(-1));
        assert_eq!(int("UInt64", json!(u64::MAX)), Value::Int(u64::MAX.into()));
    }

    #[test]
    fn renders_64_bit_values_unquoted() {
        assert_eq!(
            Value::Int(u64::MAX as i128).to_json().to_string(),
            "18446744073709551615"
        );
    }

    #[test]
    fn literals() {
        let t = Type::parse("Int32");
        let lit = |s: &str| t.literal(&Literal::Number(s.into()));
        assert_eq!(lit("-5"), Ok(Value::Int(-5)));
        assert_eq!(lit("2.5"), Ok(Value::Float(2.5)));
        assert!(matches!(
            lit("1000000000000000000000000000000000000000000"),
            Err(LiteralError::Invalid(_))
        ));
        assert_eq!(
            t.literal(&Literal::String("5".into())),
            Err(LiteralError::Mismatch)
        );
    }
}
