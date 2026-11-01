//! Column types and typed values.
//!
//! Types come from ClickHouse. Event JSON is decoded by those types following ClickHouse's
//! `JSONEachRow` input rules, and values are rendered back the way ClickHouse's JSON output
//! does, so a row from the stream looks exactly like the same row read from history.
//!
//! Each supported type lives in its own module and spells out all of its behavior, with no
//! defaults to fall back on: [`ColumnType`] says what a column of it accepts (event JSON,
//! literals, its zero value) and allows (filtering, ordering, …), and [`Scalar`] how its
//! values compare and render. `engine/tests/clickhouse.rs` checks every type against ClickHouse.

mod boolean;
mod datetime;
mod float;
mod int;
mod other;
mod string;
mod uuid;

use std::cmp::Ordering;
use std::fmt;

use serde_json::Value as Json;

use crate::ast::Literal;
use crate::rules::KafkaField;

pub use self::boolean::BoolType;
pub use self::datetime::{
    format_date, format_datetime, parse_date, parse_datetime, Date, DateTime, DateTimeType,
    DateType,
};
pub use self::float::FloatType;
pub use self::int::IntType;
pub use self::other::OtherType;
pub use self::string::StringType;
pub use self::uuid::{Uuid, UuidType};

/// What a column of one ClickHouse type accepts and allows.
pub trait ColumnType {
    /// An event's JSON value (never null) under ClickHouse's `JSONEachRow` input rules,
    /// validated and normalized; `None` if ClickHouse would reject it.
    fn decode(&self, json: &Json) -> Option<Value>;
    /// A dialect literal (never NULL) compared with a column of this type, checked at bind
    /// time; `ty` is the column's full type, for messages.
    fn literal(&self, lit: &Literal, ty: &Type) -> Result<Value, LiteralError>;
    /// A missing field in a non-Nullable column, as ClickHouse stores it.
    fn zero(&self) -> Value;
    /// Which types its values compare with (`=`, `<`, IN, semi-join keys); `None`: none.
    fn class(&self) -> Option<Class>;
    /// What queries may do with its columns.
    fn ops(&self) -> Ops;
}

/// How the values of one representation compare and render, as ClickHouse does.
pub trait Scalar {
    /// As ClickHouse compares two of them; `None` only where ClickHouse can't order them (NaN).
    fn compare(&self, other: &Self) -> Option<Ordering>;
    /// As ClickHouse's JSON output renders it (64-bit integers unquoted, which is how the
    /// history side asks ClickHouse for them).
    fn to_json(&self) -> Json;
    /// As a ClickHouse SQL literal (used for the history query).
    fn to_sql(&self) -> String;
}

/// What kind of values can be compared with each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Number,
    String,
    DateTime,
    Date,
    Bool,
    Uuid,
}

/// What queries may do with a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ops {
    /// In WHERE (and as an IN/EXISTS key).
    pub filter: bool,
    pub order: bool,
    pub group: bool,
    /// As `min`/`max`/`any`/`anyLast`'s argument.
    pub min_max: bool,
}

impl Ops {
    pub const ALL: Ops = Ops {
        filter: true,
        order: true,
        group: true,
        min_max: true,
    };
    pub const NONE: Ops = Ops {
        filter: false,
        order: false,
        group: false,
        min_max: false,
    };
}

/// Why a literal can't stand for a value of a column's type.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralError {
    /// The wrong kind of literal (a string for a number, …).
    Mismatch,
    /// The right kind, but not a valid value; the message says why.
    Invalid(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Type {
    pub kind: Kind,
    pub nullable: bool,
    /// The ClickHouse type as reported, e.g. `LowCardinality(String)`.
    pub clickhouse: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    Int(IntType),
    Float(FloatType),
    /// Also `FixedString` and `Enum8`/`Enum16`, which ClickHouse reads and writes as strings.
    String(StringType),
    /// `DateTime` (precision 0) or `DateTime64(precision)`.
    DateTime(DateTimeType),
    Date(DateType),
    Bool(BoolType),
    Uuid(UuidType),
    /// Anything else (Array, Map, Decimal, …): passed through undecoded, not filterable.
    Other(OtherType),
}

impl Kind {
    pub fn column_type(&self) -> &dyn ColumnType {
        match self {
            Kind::Int(t) => t,
            Kind::Float(t) => t,
            Kind::String(t) => t,
            Kind::DateTime(t) => t,
            Kind::Date(t) => t,
            Kind::Bool(t) => t,
            Kind::Uuid(t) => t,
            Kind::Other(t) => t,
        }
    }

    pub fn is_numeric(&self) -> bool {
        self.column_type().class() == Some(Class::Number)
    }
}

impl Type {
    /// From a ClickHouse type name.
    pub fn parse(clickhouse: &str) -> Type {
        let mut t = clickhouse.trim();
        let mut nullable = false;
        loop {
            if let Some(inner) = strip_wrapper(t, "LowCardinality") {
                t = inner;
            } else if let Some(inner) = strip_wrapper(t, "Nullable") {
                nullable = true;
                t = inner;
            } else {
                break;
            }
        }
        let int = |signed, bits| Kind::Int(IntType { signed, bits });
        let kind = match t {
            "UInt8" => int(false, 8),
            "UInt16" => int(false, 16),
            "UInt32" => int(false, 32),
            "UInt64" => int(false, 64),
            "Int8" => int(true, 8),
            "Int16" => int(true, 16),
            "Int32" => int(true, 32),
            "Int64" => int(true, 64),
            "Float32" | "Float64" => Kind::Float(FloatType),
            "String" => Kind::String(StringType),
            "Bool" => Kind::Bool(BoolType),
            "Date" | "Date32" => Kind::Date(DateType),
            "UUID" => Kind::Uuid(UuidType),
            "DateTime" => Kind::DateTime(DateTimeType { precision: 0 }),
            _ if t.starts_with("FixedString(")
                || t.starts_with("Enum8(")
                || t.starts_with("Enum16(") =>
            {
                Kind::String(StringType)
            }
            _ if t.starts_with("DateTime(") => Kind::DateTime(DateTimeType { precision: 0 }),
            _ if t.starts_with("DateTime64(") => {
                let args = &t["DateTime64(".len()..t.len() - 1];
                let precision = args
                    .split(',')
                    .next()
                    .and_then(|p| p.trim().parse().ok())
                    .unwrap_or(3);
                Kind::DateTime(DateTimeType { precision })
            }
            _ => Kind::Other(OtherType),
        };
        Type {
            kind,
            nullable,
            clickhouse: clickhouse.to_string(),
        }
    }

    /// The value of a missing (or JSON `null`) field: NULL if the column is Nullable, the
    /// type's zero value otherwise, as ClickHouse's ingestion stores it.
    pub fn default_value(&self) -> Value {
        if self.nullable {
            Value::Null
        } else {
            self.kind.column_type().zero()
        }
    }

    pub fn ops(&self) -> Ops {
        self.kind.column_type().ops()
    }

    pub fn class(&self) -> Option<Class> {
        self.kind.column_type().class()
    }

    /// A literal as a value of this type (NULL for NULL).
    pub fn literal(&self, lit: &Literal) -> Result<Value, LiteralError> {
        match lit {
            Literal::Null => Ok(Value::Null),
            lit => self.kind.column_type().literal(lit, self),
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.clickhouse)
    }
}

fn strip_wrapper<'a>(t: &'a str, name: &str) -> Option<&'a str> {
    t.strip_prefix(name)?.strip_prefix('(')?.strip_suffix(')')
}

/// A typed value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int(i128),
    Float(f64),
    String(String),
    DateTime(DateTime),
    Date(Date),
    Bool(bool),
    Uuid(Uuid),
    /// A value of an unsupported type, kept as JSON.
    Raw(Json),
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("cannot read {found} as {ty}")]
pub struct DecodeError {
    pub ty: String,
    pub found: String,
}

/// Decode an event field (`None` if the key is missing) by its column type.
pub fn decode(json: Option<&Json>, ty: &Type) -> Result<Value, DecodeError> {
    match json {
        None | Some(Json::Null) => Ok(ty.default_value()),
        Some(json) => ty.kind.column_type().decode(json).ok_or_else(|| {
            let mut found = json.to_string();
            if found.len() > 40 {
                found.truncate(40);
                found.push('…');
            }
            DecodeError {
                ty: ty.clickhouse.clone(),
                found,
            }
        }),
    }
}

/// Whether Kafka Connect's `InsertField` stores a Kafka field into a column of this type exactly:
/// offsets and partitions in integer columns, timestamps (milliseconds) in `DateTime64(3)` or a
/// 64-bit integer. A `DateTime` or another precision reads the milliseconds as its own unit.
pub fn kafka_accepts(field: KafkaField, ty: &Type) -> bool {
    match (field, &ty.kind) {
        (KafkaField::Offset | KafkaField::Partition, Kind::Int(_)) => true,
        (KafkaField::Timestamp, Kind::Int(t)) => t.bits == 64,
        (KafkaField::Timestamp, Kind::DateTime(t)) => t.precision == 3,
        _ => false,
    }
}

/// A Kafka field's value (`None`: a message without a timestamp, which `InsertField` leaves
/// out) as its column stores it; the column's type passed [`kafka_accepts`].
pub fn decode_kafka(
    field: KafkaField,
    value: Option<i64>,
    ty: &Type,
) -> Result<Value, DecodeError> {
    match (value, &ty.kind) {
        (Some(ms), Kind::DateTime(_)) if field == KafkaField::Timestamp => {
            Ok(Value::DateTime(DateTime {
                ticks: ms,
                precision: 3,
            }))
        }
        (value, _) => decode(value.map(Json::from).as_ref(), ty),
    }
}

impl Value {
    /// The value as tagged JSON, exact for every variant (for checkpoints): `null`,
    /// `{"i": "-12"}` (as text: i128 doesn't fit JSON numbers), `{"f": "1.5"}` (text too: NaN and
    /// infinities), `{"s": …}`, `{"dt": [ticks, precision]}`, `{"d": days}`, `{"b": …}`,
    /// `{"u": "<32 hex digits>"}`, `{"r": <json>}`.
    pub fn save(&self) -> Json {
        match self {
            Value::Null => Json::Null,
            Value::Int(n) => serde_json::json!({ "i": n.to_string() }),
            Value::Float(f) => serde_json::json!({ "f": f.to_string() }),
            Value::String(s) => serde_json::json!({ "s": s }),
            Value::DateTime(d) => serde_json::json!({ "dt": [d.ticks, d.precision] }),
            Value::Date(d) => serde_json::json!({ "d": d.0 }),
            Value::Bool(b) => serde_json::json!({ "b": b }),
            Value::Uuid(u) => serde_json::json!({ "u": format!("{:032x}", u.0) }),
            Value::Raw(j) => serde_json::json!({ "r": j }),
        }
    }

    /// A value from [`Value::save`]'s form.
    pub fn load(json: &Json) -> Option<Value> {
        if json.is_null() {
            return Some(Value::Null);
        }
        let (tag, v) = json.as_object()?.iter().next()?;
        Some(match tag.as_str() {
            "i" => Value::Int(v.as_str()?.parse().ok()?),
            "f" => Value::Float(v.as_str()?.parse().ok()?),
            "s" => Value::String(v.as_str()?.to_string()),
            "dt" => Value::DateTime(DateTime {
                ticks: v.get(0)?.as_i64()?,
                precision: u8::try_from(v.get(1)?.as_u64()?).ok()?,
            }),
            "d" => Value::Date(Date(i32::try_from(v.as_i64()?).ok()?)),
            "b" => Value::Bool(v.as_bool()?),
            "u" => Value::Uuid(Uuid(u128::from_str_radix(v.as_str()?, 16).ok()?)),
            "r" => Value::Raw(v.clone()),
            _ => return None,
        })
    }

    /// As ClickHouse's JSON output renders it.
    pub fn to_json(&self) -> Json {
        match self {
            Value::Null => Json::Null,
            Value::Int(v) => v.to_json(),
            Value::Float(v) => v.to_json(),
            Value::String(v) => v.to_json(),
            Value::DateTime(v) => v.to_json(),
            Value::Date(v) => v.to_json(),
            Value::Bool(v) => v.to_json(),
            Value::Uuid(v) => v.to_json(),
            Value::Raw(j) => j.clone(),
        }
    }

    /// SQL-style comparison: `None` if either side is NULL or the values are incomparable.
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        use Value::*;
        match (self, other) {
            (Null, _) | (_, Null) => None,
            (Int(a), Int(b)) => a.compare(b),
            (Float(a), Float(b)) => a.compare(b),
            // integers and floats compare as numbers
            (Int(a), Float(b)) => (*a as f64).compare(b),
            (Float(a), Int(b)) => a.compare(&(*b as f64)),
            (String(a), String(b)) => a.compare(b),
            (DateTime(a), DateTime(b)) => a.compare(b),
            (Date(a), Date(b)) => a.compare(b),
            (Bool(a), Bool(b)) => a.compare(b),
            (Uuid(a), Uuid(b)) => a.compare(b),
            // values of different classes, or of unsupported types (the binder rejects both)
            _ => None,
        }
    }

    /// ORDER BY's total order, matching ClickHouse: NULL and NaN are never compared by value;
    /// with NULLS LAST they come after every value, NaN first, then NULL (with NULLS FIRST:
    /// NULL, NaN, then the values), whatever the direction. `desc` reverses only the values.
    /// `-0.0` equals `0.0`.
    pub fn sort_cmp(&self, other: &Value, desc: bool, nulls_first: bool) -> Ordering {
        // 0: a value, 1: NaN, 2: NULL; mirrored for NULLS FIRST
        let rank = |v: &Value| {
            let r = match v {
                Value::Null => 2,
                Value::Float(f) if f.is_nan() => 1,
                _ => 0,
            };
            if nulls_first {
                2 - r
            } else {
                r
            }
        };
        let (ra, rb) = (rank(self), rank(other));
        if ra != rb || ra != if nulls_first { 2 } else { 0 } {
            return ra.cmp(&rb);
        }
        let ord = self.compare(other).unwrap_or(Ordering::Equal);
        if desc {
            ord.reverse()
        } else {
            ord
        }
    }

    /// As a ClickHouse SQL literal (used for the history query).
    pub fn to_clickhouse_sql(&self) -> String {
        match self {
            Value::Null => "NULL".into(),
            Value::Int(v) => v.to_sql(),
            Value::Float(v) => v.to_sql(),
            Value::String(v) => v.to_sql(),
            Value::DateTime(v) => v.to_sql(),
            Value::Date(v) => v.to_sql(),
            Value::Bool(v) => v.to_sql(),
            Value::Uuid(v) => v.to_sql(),
            Value::Raw(j) => quote_string(&j.to_string()),
        }
    }
}

/// A ClickHouse string literal.
pub fn quote_string(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_save_and_load_exactly() {
        let values = [
            Value::Null,
            Value::Int(-170_141_183_460_469_231_731_687_303_715_884_105_728),
            Value::Int(42),
            Value::Float(0.1 + 0.2),
            Value::Float(f64::NAN),
            Value::Float(f64::NEG_INFINITY),
            Value::String("a \"b\"".into()),
            Value::DateTime(DateTime {
                ticks: -5,
                precision: 3,
            }),
            Value::Date(Date(-1)),
            Value::Bool(true),
            Value::Uuid(Uuid(u128::MAX - 7)),
            Value::Raw(serde_json::json!([1, {"x": null}])),
        ];
        for v in values {
            let back = Value::load(&v.save()).expect("loads");
            match (&v, &back) {
                (Value::Float(a), Value::Float(b)) if a.is_nan() => assert!(b.is_nan()),
                _ => assert_eq!(back, v),
            }
        }
        assert_eq!(Value::load(&serde_json::json!({"x": 1})), None);
        assert_eq!(Value::load(&serde_json::json!({"i": "nope"})), None);
    }
    use serde_json::json;

    #[test]
    fn parses_clickhouse_types() {
        let t = Type::parse("LowCardinality(String)");
        assert_eq!((t.kind, t.nullable), (Kind::String(StringType), false));
        let t = Type::parse("Nullable(String)");
        assert_eq!((t.kind, t.nullable), (Kind::String(StringType), true));
        let t = Type::parse("LowCardinality(Nullable(String))");
        assert_eq!((t.kind, t.nullable), (Kind::String(StringType), true));
        let dt = |precision| Kind::DateTime(DateTimeType { precision });
        assert_eq!(Type::parse("DateTime64(3)").kind, dt(3));
        assert_eq!(Type::parse("DateTime64(6, 'UTC')").kind, dt(6));
        assert_eq!(Type::parse("DateTime").kind, dt(0));
        assert_eq!(
            Type::parse("UInt32").kind,
            Kind::Int(IntType {
                signed: false,
                bits: 32
            })
        );
        assert_eq!(Type::parse("Float64").kind, Kind::Float(FloatType));
        assert_eq!(Type::parse("UUID").kind, Kind::Uuid(UuidType));
        let t = Type::parse("Nullable(UUID)");
        assert_eq!((t.kind, t.nullable), (Kind::Uuid(UuidType), true));
        assert_eq!(Type::parse("Array(String)").kind, Kind::Other(OtherType));
        assert_eq!(Type::parse("Decimal(10, 2)").kind, Kind::Other(OtherType));
    }

    #[test]
    fn missing_and_null_follow_nullability() {
        let u32 = Type::parse("UInt32");
        let ns = Type::parse("Nullable(String)");
        assert_eq!(decode(None, &u32).unwrap(), Value::Int(0));
        assert_eq!(decode(Some(&Json::Null), &u32).unwrap(), Value::Int(0));
        assert_eq!(decode(None, &ns).unwrap(), Value::Null);
        assert_eq!(
            decode(None, &Type::parse("String")).unwrap(),
            Value::String(String::new())
        );
        assert_eq!(
            decode(None, &Type::parse("DateTime64(3)"))
                .unwrap()
                .to_json(),
            json!("1970-01-01 00:00:00.000")
        );
        assert_eq!(
            decode(None, &Type::parse("UUID")).unwrap().to_json(),
            json!("00000000-0000-0000-0000-000000000000")
        );
        assert_eq!(
            decode(None, &Type::parse("Nullable(UUID)")).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn kafka_fields_as_insert_field_stores_them() {
        let ty = Type::parse;
        assert!(kafka_accepts(KafkaField::Offset, &ty("UInt64")));
        assert!(kafka_accepts(KafkaField::Partition, &ty("Int32")));
        assert!(kafka_accepts(KafkaField::Timestamp, &ty("DateTime64(3)")));
        assert!(kafka_accepts(
            KafkaField::Timestamp,
            &ty("Nullable(UInt64)")
        ));
        // seconds, or another unit: InsertField's milliseconds would be misread
        assert!(!kafka_accepts(KafkaField::Timestamp, &ty("DateTime")));
        assert!(!kafka_accepts(KafkaField::Timestamp, &ty("DateTime64(6)")));
        assert!(!kafka_accepts(KafkaField::Timestamp, &ty("UInt32")));
        assert!(!kafka_accepts(KafkaField::Offset, &ty("String")));
        assert_eq!(
            decode_kafka(KafkaField::Offset, Some(42), &ty("UInt64")).unwrap(),
            Value::Int(42)
        );
        assert_eq!(
            decode_kafka(
                KafkaField::Timestamp,
                Some(1_793_456_286_588),
                &ty("DateTime64(3)")
            )
            .unwrap()
            .to_json(),
            json!("2026-10-31 14:18:06.588")
        );
        assert_eq!(
            decode_kafka(KafkaField::Timestamp, Some(1_793_456_286_588), &ty("Int64")).unwrap(),
            Value::Int(1_793_456_286_588)
        );
        // no timestamp: left out, so the column's default
        let none = decode_kafka(KafkaField::Timestamp, None, &ty("DateTime64(3)")).unwrap();
        assert_eq!(none.to_json(), json!("1970-01-01 00:00:00.000"));
        let none = decode_kafka(KafkaField::Timestamp, None, &ty("Nullable(DateTime64(3))"));
        assert_eq!(none.unwrap(), Value::Null);
    }

    #[test]
    fn decode_errors_name_the_type_and_the_value() {
        let e = decode(Some(&json!("abc")), &Type::parse("UInt32")).unwrap_err();
        assert_eq!(e.to_string(), r#"cannot read "abc" as UInt32"#);
    }

    #[test]
    fn compares_like_sql() {
        assert_eq!(
            Value::Int(3).compare(&Value::Float(2.5)),
            Some(Ordering::Greater)
        );
        assert_eq!(Value::Null.compare(&Value::Int(1)), None);
        assert_eq!(Value::Int(1).compare(&Value::String("1".into())), None);
    }

    #[test]
    fn sort_order_matches_clickhouse() {
        // ClickHouse: values, then NaN, then NULL (NULLS LAST, the default for ASC and DESC);
        // NULL, NaN, then values with NULLS FIRST; -0 ties with 0
        let vals = [
            Value::Float(1.0),
            Value::Null,
            Value::Float(f64::NAN),
            Value::Float(f64::NEG_INFINITY),
            Value::Float(0.0),
            Value::Float(f64::INFINITY),
            Value::Int(-1),
        ];
        let sorted = |desc, nulls_first| {
            let mut v = vals.to_vec();
            v.sort_by(|a, b| a.sort_cmp(b, desc, nulls_first));
            v.iter()
                .map(|v| v.to_json().to_string())
                .collect::<Vec<_>>()
                .join(" ")
        };
        assert_eq!(sorted(false, false), r#""-inf" -1 0 1 "inf" "nan" null"#);
        assert_eq!(sorted(true, false), r#""inf" 1 0 -1 "-inf" "nan" null"#);
        assert_eq!(sorted(false, true), r#"null "nan" "-inf" -1 0 1 "inf""#);
        assert_eq!(sorted(true, true), r#"null "nan" "inf" 1 0 -1 "-inf""#);
        assert_eq!(
            Value::Float(-0.0).sort_cmp(&Value::Float(0.0), false, false),
            Ordering::Equal
        );
    }
}
