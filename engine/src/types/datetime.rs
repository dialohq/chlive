//! `DateTime`, `DateTime64(p)` (ticks of 10^-p seconds since the Unix epoch, UTC) and `Date`,
//! `Date32` (days since the epoch).

use std::cmp::Ordering;

use serde_json::Value as Json;

use super::{quote_string, Class, ColumnType, Literal, LiteralError, Ops, Scalar, Type, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTimeType {
    pub precision: u8,
}

/// An instant: ticks of 10^-precision seconds since the Unix epoch, UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTime {
    pub ticks: i64,
    pub precision: u8,
}

impl ColumnType for DateTimeType {
    /// `YYYY-MM-DD[ hh:mm:ss[.f…]]`, or an integer: like ClickHouse, ticks at the column's
    /// precision, so seconds for `DateTime` (kept within its range, 1970 to 2106) but e.g.
    /// milliseconds for `DateTime64(3)`.
    fn decode(&self, json: &Json) -> Option<Value> {
        let precision = self.precision;
        let ticks = match json {
            Json::String(s) => parse_datetime(s, precision)?,
            Json::Number(n) if precision == 0 => match n.as_i64() {
                Some(secs) => secs.clamp(0, i64::from(u32::MAX)),
                None => n.as_u64().map(|_| i64::from(u32::MAX))?,
            },
            Json::Number(n) => n.as_i64()?,
            _ => return None,
        };
        Some(Value::DateTime(DateTime { ticks, precision }))
    }

    fn literal(&self, lit: &Literal, ty: &Type) -> Result<Value, LiteralError> {
        let Literal::String(s) = lit else {
            return Err(LiteralError::Mismatch);
        };
        let precision = self.precision;
        parse_datetime(s, precision)
            .map(|ticks| Value::DateTime(DateTime { ticks, precision }))
            .ok_or_else(|| {
                LiteralError::Invalid(format!(
                    "invalid {ty} value '{s}' (expected YYYY-MM-DD[ hh:mm:ss[.fff]])"
                ))
            })
    }

    fn zero(&self) -> Value {
        Value::DateTime(DateTime {
            ticks: 0,
            precision: self.precision,
        })
    }

    fn class(&self) -> Option<Class> {
        Some(Class::DateTime)
    }

    fn ops(&self) -> Ops {
        Ops::ALL
    }
}

impl Scalar for DateTime {
    /// By instant, whatever the precisions.
    fn compare(&self, other: &Self) -> Option<Ordering> {
        let p = self.precision.max(other.precision);
        let scale = |t: i64, from: u8| i128::from(t) * 10i128.pow(u32::from(p - from));
        Some(scale(self.ticks, self.precision).cmp(&scale(other.ticks, other.precision)))
    }

    fn to_json(&self) -> Json {
        Json::String(format_datetime(self.ticks, self.precision))
    }

    fn to_sql(&self) -> String {
        let text = quote_string(&format_datetime(self.ticks, self.precision));
        match self.precision {
            0 => format!("toDateTime({text})"),
            p => format!("toDateTime64({text}, {p})"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateType;

/// Days since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date(pub i32);

impl ColumnType for DateType {
    /// `YYYY-MM-DD`.
    fn decode(&self, json: &Json) -> Option<Value> {
        match json {
            Json::String(s) => parse_date(s).map(|d| Value::Date(Date(d))),
            _ => None,
        }
    }

    fn literal(&self, lit: &Literal, _: &Type) -> Result<Value, LiteralError> {
        let Literal::String(s) = lit else {
            return Err(LiteralError::Mismatch);
        };
        parse_date(s).map(|d| Value::Date(Date(d))).ok_or_else(|| {
            LiteralError::Invalid(format!("invalid Date value '{s}' (expected YYYY-MM-DD)"))
        })
    }

    fn zero(&self) -> Value {
        Value::Date(Date(0))
    }

    fn class(&self) -> Option<Class> {
        Some(Class::Date)
    }

    fn ops(&self) -> Ops {
        Ops::ALL
    }
}

impl Scalar for Date {
    fn compare(&self, other: &Self) -> Option<Ordering> {
        Some(self.0.cmp(&other.0))
    }

    fn to_json(&self) -> Json {
        Json::String(format_date(self.0))
    }

    fn to_sql(&self) -> String {
        format!("toDate({})", quote_string(&format_date(self.0)))
    }
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
    use crate::types::decode;
    use serde_json::json;

    fn dt(ticks: i64, precision: u8) -> Value {
        Value::DateTime(DateTime { ticks, precision })
    }

    #[test]
    fn decodes_like_clickhouse_input() {
        let t = Type::parse("DateTime64(3)");
        let v = decode(Some(&json!("2026-10-01 12:00:01.5")), &t).unwrap();
        assert_eq!(v.to_json(), json!("2026-10-01 12:00:01.500"));
        assert!(decode(Some(&json!("2026-13-01 00:00:00")), &t).is_err());
        assert!(decode(Some(&json!("yesterday")), &t).is_err());
        // numbers are ticks at the column's precision; DateTime's stay within its range
        assert_eq!(decode(Some(&json!(2)), &t).unwrap(), dt(2, 3));
        let secs = Type::parse("DateTime");
        assert_eq!(decode(Some(&json!(-5)), &secs).unwrap(), dt(0, 0));
        assert_eq!(
            decode(Some(&json!(u64::MAX)), &secs).unwrap(),
            dt(u32::MAX.into(), 0)
        );
    }

    #[test]
    fn formats_like_clickhouse_output() {
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
    fn different_precisions_compare_by_instant() {
        assert_eq!(dt(1_500, 3).compare(&dt(1, 0)), Some(Ordering::Greater));
        assert_eq!(
            dt(1_000, 3).sort_cmp(&dt(1, 0), false, false),
            Ordering::Equal
        );
    }

    #[test]
    fn sql_literals() {
        let at = parse_datetime("2026-10-01 12:00:00", 3).unwrap();
        assert_eq!(
            dt(at, 3).to_clickhouse_sql(),
            "toDateTime64('2026-10-01 12:00:00.000', 3)"
        );
        assert_eq!(
            dt(at / 1000, 0).to_clickhouse_sql(),
            "toDateTime('2026-10-01 12:00:00')"
        );
        assert_eq!(
            Value::Date(Date(0)).to_clickhouse_sql(),
            "toDate('1970-01-01')"
        );
    }

    #[test]
    fn literals_are_checked() {
        let t = Type::parse("DateTime64(3)");
        assert_eq!(
            t.literal(&Literal::String("soon".into())),
            Err(LiteralError::Invalid(
                "invalid DateTime64(3) value 'soon' (expected YYYY-MM-DD[ hh:mm:ss[.fff]])".into()
            ))
        );
        assert_eq!(
            t.literal(&Literal::Number("1".into())),
            Err(LiteralError::Mismatch)
        );
    }
}
