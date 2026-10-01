//! Column types and typed values.
//!
//! Types come from ClickHouse. Event JSON is decoded by those types following ClickHouse's
//! `JSONEachRow` input rules, and values are rendered back the way ClickHouse's JSON output
//! does, so a row from the stream looks exactly like the same row read from history.

use std::cmp::Ordering;
use std::fmt;

use serde_json::Value as Json;

#[derive(Debug, Clone, PartialEq)]
pub struct Type {
    pub kind: Kind,
    pub nullable: bool,
    /// The ClickHouse type as reported, e.g. `LowCardinality(String)`.
    pub clickhouse: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    Int {
        signed: bool,
        bits: u8,
    },
    Float,
    String,
    /// `DateTime` (precision 0) or `DateTime64(precision)`.
    DateTime {
        precision: u8,
    },
    Date,
    Bool,
    /// Anything else (Array, Map, Decimal, …): passed through undecoded, not filterable.
    Other,
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
        let kind = match t {
            "UInt8" => Kind::Int {
                signed: false,
                bits: 8,
            },
            "UInt16" => Kind::Int {
                signed: false,
                bits: 16,
            },
            "UInt32" => Kind::Int {
                signed: false,
                bits: 32,
            },
            "UInt64" => Kind::Int {
                signed: false,
                bits: 64,
            },
            "Int8" => Kind::Int {
                signed: true,
                bits: 8,
            },
            "Int16" => Kind::Int {
                signed: true,
                bits: 16,
            },
            "Int32" => Kind::Int {
                signed: true,
                bits: 32,
            },
            "Int64" => Kind::Int {
                signed: true,
                bits: 64,
            },
            "Float32" | "Float64" => Kind::Float,
            "String" => Kind::String,
            "Bool" => Kind::Bool,
            "Date" | "Date32" => Kind::Date,
            "DateTime" => Kind::DateTime { precision: 0 },
            _ if t.starts_with("FixedString(")
                || t.starts_with("Enum8(")
                || t.starts_with("Enum16(") =>
            {
                Kind::String
            }
            _ if t.starts_with("DateTime(") => Kind::DateTime { precision: 0 },
            _ if t.starts_with("DateTime64(") => {
                let args = &t["DateTime64(".len()..t.len() - 1];
                let precision = args
                    .split(',')
                    .next()
                    .and_then(|p| p.trim().parse().ok())
                    .unwrap_or(3);
                Kind::DateTime { precision }
            }
            _ => Kind::Other,
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
            return Value::Null;
        }
        match self.kind {
            Kind::Int { .. } => Value::Int(0),
            Kind::Float => Value::Float(0.0),
            Kind::String => Value::String(String::new()),
            Kind::DateTime { precision } => Value::DateTime {
                ticks: 0,
                precision,
            },
            Kind::Date => Value::Date(0),
            Kind::Bool => Value::Bool(false),
            Kind::Other => Value::Raw(Json::Null),
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
    /// Ticks of 10^-precision seconds since the Unix epoch, UTC.
    DateTime {
        ticks: i64,
        precision: u8,
    },
    /// Days since the Unix epoch.
    Date(i32),
    Bool(bool),
    /// A value of an unsupported type, kept as JSON.
    Raw(Json),
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("cannot read {found} as {ty}")]
pub struct DecodeError {
    pub ty: String,
    pub found: String,
}

fn decode_error(ty: &Type, found: &Json) -> DecodeError {
    let mut found = found.to_string();
    if found.len() > 40 {
        found.truncate(40);
        found.push('…');
    }
    DecodeError {
        ty: ty.clickhouse.clone(),
        found,
    }
}

/// Decode an event field (`None` if the key is missing) by its column type.
pub fn decode(json: Option<&Json>, ty: &Type) -> Result<Value, DecodeError> {
    let json = match json {
        None | Some(Json::Null) => return Ok(ty.default_value()),
        Some(j) => j,
    };
    let err = || decode_error(ty, json);
    Ok(match &ty.kind {
        Kind::Int { signed, bits } => {
            let n: i128 = match json {
                Json::Number(n) => n
                    .as_i64()
                    .map(i128::from)
                    .or_else(|| n.as_u64().map(i128::from))
                    .ok_or_else(err)?,
                Json::String(s) => s.trim().parse().map_err(|_| err())?,
                Json::Bool(b) => i128::from(*b),
                _ => return Err(err()),
            };
            if !int_fits(n, *signed, *bits) {
                return Err(err());
            }
            Value::Int(n)
        }
        Kind::Float => match json {
            Json::Number(n) => Value::Float(n.as_f64().ok_or_else(err)?),
            Json::String(s) => Value::Float(s.trim().parse().map_err(|_| err())?),
            _ => return Err(err()),
        },
        Kind::String => match json {
            Json::String(s) => Value::String(s.clone()),
            Json::Number(n) => Value::String(n.to_string()),
            Json::Bool(b) => Value::String(b.to_string()),
            _ => return Err(err()),
        },
        Kind::DateTime { precision } => match json {
            Json::String(s) => {
                let ticks = parse_datetime(s, *precision).ok_or_else(err)?;
                Value::DateTime {
                    ticks,
                    precision: *precision,
                }
            }
            // a Unix timestamp in seconds
            Json::Number(n) => {
                let secs = n.as_i64().ok_or_else(err)?;
                Value::DateTime {
                    ticks: secs * 10i64.pow(u32::from(*precision)),
                    precision: *precision,
                }
            }
            _ => return Err(err()),
        },
        Kind::Date => match json {
            Json::String(s) => Value::Date(parse_date(s).ok_or_else(err)?),
            _ => return Err(err()),
        },
        Kind::Bool => match json {
            Json::Bool(b) => Value::Bool(*b),
            Json::Number(n) if n.as_u64() == Some(0) => Value::Bool(false),
            Json::Number(n) if n.as_u64() == Some(1) => Value::Bool(true),
            _ => return Err(err()),
        },
        Kind::Other => Value::Raw(json.clone()),
    })
}

pub fn int_fits(n: i128, signed: bool, bits: u8) -> bool {
    if signed {
        let max = (1i128 << (bits - 1)) - 1;
        n >= -max - 1 && n <= max
    } else {
        n >= 0 && n < (1i128 << bits)
    }
}

impl Value {
    /// As ClickHouse's JSON output renders it (64-bit integers unquoted, which is how the
    /// history side asks ClickHouse for them).
    pub fn to_json(&self) -> Json {
        match self {
            Value::Null => Json::Null,
            Value::Int(n) => {
                let text = n.to_string();
                Json::Number(text.parse().unwrap_or_else(|_| 0.into()))
            }
            Value::Float(f) if f.is_finite() => {
                // `{}` prints integral floats without a fraction, as ClickHouse does (`0`, not `0.0`).
                format!("{f}")
                    .parse()
                    .map(Json::Number)
                    .unwrap_or(Json::Null)
            }
            Value::Float(_) => Json::Null,
            Value::String(s) => Json::String(s.clone()),
            Value::DateTime { ticks, precision } => {
                Json::String(format_datetime(*ticks, *precision))
            }
            Value::Date(days) => Json::String(format_date(*days)),
            Value::Bool(b) => Json::Bool(*b),
            Value::Raw(j) => j.clone(),
        }
    }

    /// SQL-style comparison: `None` if either side is NULL or the values are incomparable.
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        use Value::*;
        match (self, other) {
            (Null, _) | (_, Null) => None,
            (Int(a), Int(b)) => Some(a.cmp(b)),
            (Int(a), Float(b)) => (*a as f64).partial_cmp(b),
            (Float(a), Int(b)) => a.partial_cmp(&(*b as f64)),
            (Float(a), Float(b)) => a.partial_cmp(b),
            // byte-wise, like ClickHouse
            (String(a), String(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
            (
                DateTime {
                    ticks: a,
                    precision: pa,
                },
                DateTime {
                    ticks: b,
                    precision: pb,
                },
            ) => {
                let p = (*pa).max(*pb);
                let scale = |t: i64, from: u8| i128::from(t) * 10i128.pow(u32::from(p - from));
                Some(scale(*a, *pa).cmp(&scale(*b, *pb)))
            }
            (Date(a), Date(b)) => Some(a.cmp(b)),
            (Bool(a), Bool(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    /// As a ClickHouse SQL literal (used for the history query).
    pub fn to_clickhouse_sql(&self) -> String {
        match self {
            Value::Null => "NULL".into(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => format!("{f}"),
            Value::String(s) => quote_string(s),
            Value::DateTime {
                ticks,
                precision: 0,
            } => format!("toDateTime({})", quote_string(&format_datetime(*ticks, 0))),
            Value::DateTime { ticks, precision } => {
                format!(
                    "toDateTime64({}, {precision})",
                    quote_string(&format_datetime(*ticks, *precision))
                )
            }
            Value::Date(days) => format!("toDate({})", quote_string(&format_date(*days))),
            Value::Bool(b) => b.to_string(),
            Value::Raw(j) => quote_string(&j.to_string()),
        }
    }
}

/// A ClickHouse string literal.
pub fn quote_string(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// `YYYY-MM-DD[ hh:mm:ss[.f…]]` (or with `T`) in UTC, as ticks at `precision`. Extra fraction
/// digits are truncated, as ClickHouse does.
pub fn parse_datetime(s: &str, precision: u8) -> Option<i64> {
    let s = s.trim();
    let (date, time) = match s.find([' ', 'T']) {
        Some(i) => (&s[..i], Some(&s[i + 1..])),
        None => (s, None),
    };
    let days = i64::from(parse_date(date)?);
    let (mut secs_of_day, mut frac) = (0i64, 0i64);
    if let Some(time) = time {
        let (hms, fraction) = match time.split_once('.') {
            Some((hms, f)) => (hms, Some(f)),
            None => (time, None),
        };
        let mut parts = hms.split(':');
        let h: i64 = parts.next()?.parse().ok()?;
        let m: i64 = parts.next()?.parse().ok()?;
        let sec: i64 = parts.next()?.parse().ok()?;
        if parts.next().is_some() || h > 23 || m > 59 || sec > 59 {
            return None;
        }
        secs_of_day = h * 3600 + m * 60 + sec;
        if let Some(f) = fraction {
            if f.is_empty() || !f.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let digits: String = f
                .chars()
                .chain(std::iter::repeat('0'))
                .take(usize::from(precision))
                .collect();
            frac = if precision == 0 {
                0
            } else {
                digits.parse().ok()?
            };
        }
    }
    let scale = 10i64.pow(u32::from(precision));
    Some((days * 86_400 + secs_of_day) * scale + frac)
}

/// `YYYY-MM-DD` as days since the epoch.
pub fn parse_date(s: &str) -> Option<i32> {
    let mut parts = s.trim().split('-');
    let y: i64 = parts.next()?.parse().ok()?;
    let m: u32 = parts.next()?.parse().ok()?;
    let d: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || !(1..=12).contains(&m) || d == 0 || d > days_in_month(y, m) {
        return None;
    }
    i32::try_from(days_from_civil(y, m, d)).ok()
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        _ => 28,
    }
}

// Howard Hinnant's civil-from-days / days-from-civil.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(m);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

pub fn format_date(days: i32) -> String {
    let (y, m, d) = civil_from_days(i64::from(days));
    format!("{y:04}-{m:02}-{d:02}")
}

pub fn format_datetime(ticks: i64, precision: u8) -> String {
    let scale = 10i64.pow(u32::from(precision));
    let (secs, frac) = (ticks.div_euclid(scale), ticks.rem_euclid(scale));
    let (days, sod) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, d) = civil_from_days(days);
    let base = format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    );
    if precision == 0 {
        base
    } else {
        format!("{base}.{frac:0width$}", width = usize::from(precision))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_clickhouse_types() {
        let t = Type::parse("LowCardinality(String)");
        assert_eq!((t.kind, t.nullable), (Kind::String, false));
        let t = Type::parse("Nullable(String)");
        assert_eq!((t.kind, t.nullable), (Kind::String, true));
        let t = Type::parse("LowCardinality(Nullable(String))");
        assert_eq!((t.kind, t.nullable), (Kind::String, true));
        assert_eq!(
            Type::parse("DateTime64(3)").kind,
            Kind::DateTime { precision: 3 }
        );
        assert_eq!(
            Type::parse("DateTime64(6, 'UTC')").kind,
            Kind::DateTime { precision: 6 }
        );
        assert_eq!(
            Type::parse("DateTime").kind,
            Kind::DateTime { precision: 0 }
        );
        assert_eq!(
            Type::parse("UInt32").kind,
            Kind::Int {
                signed: false,
                bits: 32
            }
        );
        assert_eq!(Type::parse("Float64").kind, Kind::Float);
        assert_eq!(Type::parse("Array(String)").kind, Kind::Other);
        assert_eq!(Type::parse("Decimal(10, 2)").kind, Kind::Other);
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
    }

    #[test]
    fn decodes_like_clickhouse_input() {
        let u32 = Type::parse("UInt32");
        assert_eq!(decode(Some(&json!(42)), &u32).unwrap(), Value::Int(42));
        assert_eq!(decode(Some(&json!("42")), &u32).unwrap(), Value::Int(42));
        assert!(decode(Some(&json!(-1)), &u32).is_err());
        assert!(decode(Some(&json!(4294967296u64)), &u32).is_err());
        assert!(decode(Some(&json!("abc")), &u32).is_err());
        assert!(decode(Some(&json!(1.5)), &u32).is_err());
        assert_eq!(
            decode(Some(&json!(7)), &Type::parse("String")).unwrap(),
            Value::String("7".into())
        );
        assert_eq!(
            decode(Some(&json!("2.5")), &Type::parse("Float64")).unwrap(),
            Value::Float(2.5)
        );
        let dt = Type::parse("DateTime64(3)");
        let v = decode(Some(&json!("2026-10-01 12:00:01.5")), &dt).unwrap();
        assert_eq!(v.to_json(), json!("2026-10-01 12:00:01.500"));
        assert!(decode(Some(&json!("2026-13-01 00:00:00")), &dt).is_err());
        assert!(decode(Some(&json!("yesterday")), &dt).is_err());
    }

    #[test]
    fn renders_like_clickhouse_output() {
        assert_eq!(Value::Float(0.0).to_json().to_string(), "0");
        assert_eq!(Value::Float(129.99).to_json().to_string(), "129.99");
        assert_eq!(
            Value::Int(u64::MAX as i128).to_json().to_string(),
            "18446744073709551615"
        );
        assert_eq!(
            format_datetime(parse_datetime("2026-10-01", 3).unwrap(), 3),
            "2026-10-01 00:00:00.000"
        );
        assert_eq!(
            format_datetime(parse_datetime("2026-10-01 12:34:56.789123", 3).unwrap(), 3),
            "2026-10-01 12:34:56.789"
        );
        assert_eq!(format_date(parse_date("2024-02-29").unwrap()), "2024-02-29");
        assert!(parse_date("2023-02-29").is_none());
    }

    #[test]
    fn compares_like_sql() {
        assert_eq!(
            Value::Int(3).compare(&Value::Float(2.5)),
            Some(Ordering::Greater)
        );
        assert_eq!(Value::Null.compare(&Value::Int(1)), None);
        assert_eq!(
            Value::String("B".into()).compare(&Value::String("a".into())),
            Some(Ordering::Less)
        );
        let a = Value::DateTime {
            ticks: 1_500,
            precision: 3,
        };
        let b = Value::DateTime {
            ticks: 1,
            precision: 0,
        };
        assert_eq!(a.compare(&b), Some(Ordering::Greater));
    }

    #[test]
    fn sql_literals_are_escaped() {
        assert_eq!(
            Value::String("it's \\ ok".into()).to_clickhouse_sql(),
            r"'it\'s \\ ok'"
        );
        assert_eq!(
            Value::DateTime {
                ticks: parse_datetime("2026-10-01 12:00:00", 3).unwrap(),
                precision: 3
            }
            .to_clickhouse_sql(),
            "toDateTime64('2026-10-01 12:00:00.000', 3)"
        );
    }
}
