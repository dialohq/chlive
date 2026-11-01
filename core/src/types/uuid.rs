//! `UUID`: values are the 128 bits its text spells, first hex digit most significant.

use std::cmp::Ordering;

use serde_json::Value as Json;

use super::{quote_string, Class, ColumnType, Literal, LiteralError, Ops, Scalar, Type, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UuidType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Uuid(pub u128);

impl Uuid {
    /// As ClickHouse reads one: 36 characters with dashes at 8, 13, 18 and 23, or 32 hex
    /// digits; either case, nothing around it (no braces, no spaces).
    pub fn parse(s: &str) -> Option<Uuid> {
        let b = s.as_bytes();
        let hex: Vec<u8> = match b.len() {
            32 => b.to_vec(),
            36 if [8, 13, 18, 23].iter().all(|&i| b[i] == b'-') => b
                .iter()
                .enumerate()
                .filter(|(i, _)| ![8, 13, 18, 23].contains(i))
                .map(|(_, c)| *c)
                .collect(),
            _ => return None,
        };
        hex.iter()
            .try_fold(0u128, |n, c| {
                let digit = (*c as char).to_digit(16)?;
                Some(n << 4 | u128::from(digit))
            })
            .map(Uuid)
    }

    /// Lowercase `8-4-4-4-12`, as ClickHouse writes it.
    pub fn format(&self) -> String {
        let h = format!("{:032x}", self.0);
        format!(
            "{}-{}-{}-{}-{}",
            &h[..8],
            &h[8..12],
            &h[12..16],
            &h[16..20],
            &h[20..]
        )
    }
}

impl ColumnType for UuidType {
    /// A string, by [`Uuid::parse`]; normalized to lowercase.
    fn decode(&self, json: &Json) -> Option<Value> {
        match json {
            Json::String(s) => Uuid::parse(s).map(Value::Uuid),
            _ => None,
        }
    }

    fn literal(&self, lit: &Literal, _: &Type) -> Result<Value, LiteralError> {
        let Literal::String(s) = lit else {
            return Err(LiteralError::Mismatch);
        };
        Uuid::parse(s).map(Value::Uuid).ok_or_else(|| {
            LiteralError::Invalid(format!(
                "invalid UUID value '{s}' (expected 8-4-4-4-12 hex digits)"
            ))
        })
    }

    fn zero(&self) -> Value {
        Value::Uuid(Uuid(0))
    }

    fn class(&self) -> Option<Class> {
        Some(Class::Uuid)
    }

    fn ops(&self) -> Ops {
        Ops::ALL
    }
}

impl Scalar for Uuid {
    /// ClickHouse's order, which isn't the text's: the last 8 bytes first, then the first 8.
    fn compare(&self, other: &Self) -> Option<Ordering> {
        let halves = |u: &Uuid| (u.0 as u64, (u.0 >> 64) as u64);
        Some(halves(self).cmp(&halves(other)))
    }

    fn to_json(&self) -> Json {
        Json::String(self.format())
    }

    fn to_sql(&self) -> String {
        format!("toUUID({})", quote_string(&self.format()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decode;
    use serde_json::json;

    fn uuid(s: &str) -> Value {
        Value::Uuid(Uuid::parse(s).unwrap())
    }

    #[test]
    fn reads_both_forms_in_either_case() {
        let t = Type::parse("UUID");
        let lower = json!("abcdef00-0000-0000-0000-000000000001");
        for input in [
            json!("ABCDEF00-0000-0000-0000-000000000001"),
            json!("abcdef00000000000000000000000001"),
            lower.clone(),
        ] {
            assert_eq!(decode(Some(&input), &t).unwrap().to_json(), lower);
        }
    }

    #[test]
    fn rejects_what_clickhouse_rejects() {
        let t = Type::parse("UUID");
        for input in [
            json!("{abcdef00-0000-0000-0000-000000000001}"),
            json!(" abcdef00-0000-0000-0000-000000000001"),
            json!("abcdef00-0000-0000-0000-000000000001 "),
            json!("abcdef00-0000-0000-0000-00000000000"),
            json!("abcdef00000000000000-000000000001aa"),
            json!("abcdeg00-0000-0000-0000-000000000001"),
            json!(""),
            json!(123),
        ] {
            assert!(decode(Some(&input), &t).is_err(), "{input}");
        }
    }

    #[test]
    fn orders_by_the_second_half_first() {
        let mut v = [
            uuid("00000000-0000-0002-0000-000000000001"),
            uuid("00000000-0000-0001-0000-000000000001"),
            uuid("ffffffff-ffff-ffff-0000-000000000000"),
        ];
        v.sort_by(|a, b| a.sort_cmp(b, false, false));
        let v: Vec<Json> = v.iter().map(Value::to_json).collect();
        assert_eq!(
            v,
            [
                json!("ffffffff-ffff-ffff-0000-000000000000"),
                json!("00000000-0000-0001-0000-000000000001"),
                json!("00000000-0000-0002-0000-000000000001"),
            ]
        );
    }

    #[test]
    fn literals() {
        let t = Type::parse("UUID");
        assert_eq!(
            t.literal(&Literal::String(
                "ABCDEF00-0000-0000-0000-000000000001".into()
            )),
            Ok(uuid("abcdef00-0000-0000-0000-000000000001"))
        );
        assert!(matches!(
            t.literal(&Literal::String("nope".into())),
            Err(LiteralError::Invalid(_))
        ));
        assert_eq!(
            t.literal(&Literal::Number("5".into())),
            Err(LiteralError::Mismatch)
        );
        assert_eq!(
            uuid("abcdef00-0000-0000-0000-000000000001").to_clickhouse_sql(),
            "toUUID('abcdef00-0000-0000-0000-000000000001')"
        );
    }
}
