//! Native aggregate functions, matching ClickHouse: result types, NULL handling, integer
//! wrapping, NaN for an empty `avg`, zero values for an empty non-NULL-able `min`/`max`.

use std::any::Any;
use std::cmp::Ordering;

use super::{AggregateFunction, AggregateState};
use crate::types::{IntType, Kind, Type, Value};

fn uint64() -> Type {
    Type::parse("UInt64")
}

/// `count(x)` as SQL, or `count()`.
fn count_sql(arg: Option<&str>) -> String {
    format!("count({})", arg.unwrap_or(""))
}

// ------------------------------------------------------------------------------------- count

/// `count()`: rows; `count(x)`: non-NULL values.
pub struct Count;

#[derive(Clone, Default)]
struct CountState(u64);

impl AggregateFunction for Count {
    fn history_sql(&self, arg: Option<&str>, _: Option<&Type>, _: &Type) -> Vec<(String, Type)> {
        vec![(count_sql(arg), uint64())]
    }

    fn new_state(&self, _: Option<&Type>, _: &Type) -> Box<dyn AggregateState> {
        Box::new(CountState(0))
    }
}

impl AggregateState for CountState {
    fn merge_history(&mut self, values: &[Value]) {
        if let Some(Value::Int(n)) = values.first() {
            self.0 += *n as u64;
        }
    }

    fn update(&mut self, value: Option<&Value>) {
        if !matches!(value, Some(Value::Null)) {
            self.0 += 1;
        }
    }

    fn merge(&mut self, other: &dyn AggregateState) {
        if let Some(o) = other.as_any().downcast_ref::<Self>() {
            self.0 += o.0;
        }
    }

    fn finalize(&self) -> Value {
        Value::Int(self.0.into())
    }

    fn save(&self) -> serde_json::Value {
        serde_json::json!(self.0)
    }

    fn load(&mut self, json: &serde_json::Value) -> Result<(), String> {
        self.0 = json
            .as_u64()
            .ok_or_else(|| format!("checkpoint: bad count {json}"))?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn box_clone(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

// --------------------------------------------------------------------------------- sum / avg

/// A running sum in ClickHouse's `sum` result type: Int64 and UInt64 wrap around, Float64 adds.
#[derive(Clone, Copy)]
enum Total {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

impl Total {
    fn zero(result_ty: &Type) -> Self {
        match result_ty.kind {
            Kind::Int(IntType { signed: true, .. }) => Total::Signed(0),
            Kind::Int(IntType { signed: false, .. }) => Total::Unsigned(0),
            _ => Total::Float(0.0),
        }
    }

    fn add(&mut self, v: &Value) {
        match (self, v) {
            (Total::Signed(t), Value::Int(n)) => *t = t.wrapping_add(*n as i64),
            (Total::Unsigned(t), Value::Int(n)) => *t = t.wrapping_add(*n as u64),
            (Total::Float(t), Value::Float(f)) => *t += f,
            (Total::Float(t), Value::Int(n)) => *t += *n as f64,
            _ => {}
        }
    }

    fn merge(&mut self, other: Total) {
        match (self, other) {
            (Total::Signed(a), Total::Signed(b)) => *a = a.wrapping_add(b),
            (Total::Unsigned(a), Total::Unsigned(b)) => *a = a.wrapping_add(b),
            (Total::Float(a), Total::Float(b)) => *a += b,
            _ => {}
        }
    }

    fn value(self) -> Value {
        match self {
            Total::Signed(n) => Value::Int(n.into()),
            Total::Unsigned(n) => Value::Int(n.into()),
            Total::Float(f) => Value::Float(f),
        }
    }

    fn save(self) -> serde_json::Value {
        match self {
            Total::Signed(n) => serde_json::json!({ "i": n }),
            Total::Unsigned(n) => serde_json::json!({ "u": n }),
            Total::Float(f) => serde_json::json!({ "f": f.to_string() }),
        }
    }

    fn load(json: &serde_json::Value) -> Result<Self, String> {
        let bad = || format!("checkpoint: bad sum {json}");
        if let Some(n) = json.get("i") {
            return n.as_i64().map(Total::Signed).ok_or_else(bad);
        }
        if let Some(n) = json.get("u") {
            return n.as_u64().map(Total::Unsigned).ok_or_else(bad);
        }
        json.get("f")
            .and_then(|f| f.as_str()?.parse().ok())
            .map(Total::Float)
            .ok_or_else(bad)
    }

    fn as_f64(self) -> f64 {
        match self {
            Total::Signed(n) => n as f64,
            Total::Unsigned(n) => n as f64,
            Total::Float(f) => f,
        }
    }
}

/// The type `sum(x)` has for an argument of type `arg` (what ClickHouse returns for it).
fn sum_type(arg: &Type) -> Type {
    let base = match arg.kind {
        Kind::Int(IntType { signed: true, .. }) => "Int64",
        Kind::Int(IntType { signed: false, .. }) => "UInt64",
        _ => "Float64",
    };
    Type::parse(&if arg.nullable {
        format!("Nullable({base})")
    } else {
        base.to_string()
    })
}

/// `sum(x)`: NULLs skipped; NULL when a NULL-able column had no values.
pub struct Sum;

#[derive(Clone)]
struct SumState {
    total: Total,
    seen: bool,
    nullable: bool,
}

impl AggregateFunction for Sum {
    fn history_sql(
        &self,
        arg: Option<&str>,
        _: Option<&Type>,
        result_ty: &Type,
    ) -> Vec<(String, Type)> {
        vec![(format!("sum({})", arg.unwrap_or("")), result_ty.clone())]
    }

    fn new_state(&self, _: Option<&Type>, result_ty: &Type) -> Box<dyn AggregateState> {
        Box::new(SumState {
            total: Total::zero(result_ty),
            seen: false,
            nullable: result_ty.nullable,
        })
    }
}

impl AggregateState for SumState {
    fn merge_history(&mut self, values: &[Value]) {
        match values.first() {
            Some(Value::Null) | None => {}
            Some(v) => {
                self.total.add(v);
                self.seen = true;
            }
        }
    }

    fn update(&mut self, value: Option<&Value>) {
        if let Some(v) = value.filter(|v| **v != Value::Null) {
            self.total.add(v);
            self.seen = true;
        }
    }

    fn merge(&mut self, other: &dyn AggregateState) {
        if let Some(o) = other.as_any().downcast_ref::<Self>() {
            self.total.merge(o.total);
            self.seen |= o.seen;
        }
    }

    fn finalize(&self) -> Value {
        if self.nullable && !self.seen {
            Value::Null
        } else {
            self.total.value()
        }
    }

    fn save(&self) -> serde_json::Value {
        serde_json::json!({ "total": self.total.save(), "seen": self.seen })
    }

    fn load(&mut self, json: &serde_json::Value) -> Result<(), String> {
        self.total = Total::load(&json["total"])?;
        self.seen = json["seen"].as_bool().ok_or("checkpoint: bad sum")?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn box_clone(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// `avg(x)`: the sum (in `sum`'s type) over the count of non-NULL values, as Float64; NaN for
/// none, or NULL for a NULL-able column.
pub struct Avg;

#[derive(Clone)]
struct AvgState {
    total: Total,
    count: u64,
    nullable: bool,
}

impl AggregateFunction for Avg {
    fn history_sql(
        &self,
        arg: Option<&str>,
        arg_ty: Option<&Type>,
        _: &Type,
    ) -> Vec<(String, Type)> {
        let ty = arg_ty
            .map(sum_type)
            .unwrap_or_else(|| Type::parse("Float64"));
        vec![
            (format!("sum({})", arg.unwrap_or("")), ty),
            (count_sql(arg), uint64()),
        ]
    }

    fn new_state(&self, arg_ty: Option<&Type>, result_ty: &Type) -> Box<dyn AggregateState> {
        let sum_ty = arg_ty
            .map(sum_type)
            .unwrap_or_else(|| Type::parse("Float64"));
        Box::new(AvgState {
            total: Total::zero(&sum_ty),
            count: 0,
            nullable: result_ty.nullable,
        })
    }
}

impl AggregateState for AvgState {
    fn merge_history(&mut self, values: &[Value]) {
        if let [sum, Value::Int(n), ..] = values {
            if *sum != Value::Null {
                self.total.add(sum);
            }
            self.count += *n as u64;
        }
    }

    fn update(&mut self, value: Option<&Value>) {
        if let Some(v) = value.filter(|v| **v != Value::Null) {
            self.total.add(v);
            self.count += 1;
        }
    }

    fn merge(&mut self, other: &dyn AggregateState) {
        if let Some(o) = other.as_any().downcast_ref::<Self>() {
            self.total.merge(o.total);
            self.count += o.count;
        }
    }

    fn finalize(&self) -> Value {
        match (self.count, self.nullable) {
            (0, true) => Value::Null,
            (0, false) => Value::Float(f64::NAN),
            (n, _) => Value::Float(self.total.as_f64() / n as f64),
        }
    }

    fn save(&self) -> serde_json::Value {
        serde_json::json!({ "total": self.total.save(), "count": self.count })
    }

    fn load(&mut self, json: &serde_json::Value) -> Result<(), String> {
        self.total = Total::load(&json["total"])?;
        self.count = json["count"].as_u64().ok_or("checkpoint: bad avg")?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn box_clone(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

// ------------------------------------------------------------------------------- min / max

/// `min(x)` / `max(x)`: NULLs skipped; with no values, NULL for a NULL-able column, otherwise
/// the type's zero value (as ClickHouse gives for an empty global aggregate).
pub struct MinMax {
    pub max: bool,
}

#[derive(Clone)]
struct MinMaxState {
    best: Option<Value>,
    max: bool,
    empty: Value,
}

impl AggregateFunction for MinMax {
    fn history_sql(
        &self,
        arg: Option<&str>,
        _: Option<&Type>,
        result_ty: &Type,
    ) -> Vec<(String, Type)> {
        let f = if self.max { "max" } else { "min" };
        // the count tells a real extreme from the zero value of an empty group
        vec![
            (format!("{f}({})", arg.unwrap_or("")), result_ty.clone()),
            (count_sql(arg), uint64()),
        ]
    }

    fn new_state(&self, _: Option<&Type>, result_ty: &Type) -> Box<dyn AggregateState> {
        Box::new(MinMaxState {
            best: None,
            max: self.max,
            empty: if result_ty.nullable {
                Value::Null
            } else {
                result_ty.default_value()
            },
        })
    }
}

impl MinMaxState {
    fn offer(&mut self, v: &Value) {
        if *v == Value::Null {
            return;
        }
        let better = match &self.best {
            None => true,
            Some(b) => match v.compare(b) {
                Some(Ordering::Less) => !self.max,
                Some(Ordering::Greater) => self.max,
                _ => false,
            },
        };
        if better {
            self.best = Some(v.clone());
        }
    }
}

impl AggregateState for MinMaxState {
    fn merge_history(&mut self, values: &[Value]) {
        if let [v, Value::Int(n), ..] = values {
            if *n > 0 {
                self.offer(v);
            }
        }
    }

    fn update(&mut self, value: Option<&Value>) {
        if let Some(v) = value {
            self.offer(v);
        }
    }

    fn merge(&mut self, other: &dyn AggregateState) {
        if let Some(Some(b)) = other.as_any().downcast_ref::<Self>().map(|o| &o.best) {
            self.offer(&b.clone());
        }
    }

    fn finalize(&self) -> Value {
        self.best.clone().unwrap_or_else(|| self.empty.clone())
    }

    fn save(&self) -> serde_json::Value {
        // `[value]` or `null`: a best value that is itself NULL stays one
        serde_json::json!({ "best": self.best.as_ref().map(|v| [v.save()]) })
    }

    fn load(&mut self, json: &serde_json::Value) -> Result<(), String> {
        self.best = match &json["best"] {
            serde_json::Value::Null => None,
            v => Some(
                v.get(0)
                    .and_then(Value::load)
                    .ok_or_else(|| format!("checkpoint: bad min/max {v}"))?,
            ),
        };
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn box_clone(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::{native, result_type};
    use crate::binder::AggFunc;

    fn state(func: AggFunc, arg: &str) -> (Box<dyn AggregateState>, Type) {
        let arg = Type::parse(arg);
        let rt = result_type(func, Some(&arg)).unwrap();
        (native(func).unwrap().new_state(Some(&arg), &rt), rt)
    }

    #[test]
    fn count_and_sum() {
        let mut c = Count.new_state(None, &uint64());
        c.merge_history(&[Value::Int(5)]);
        c.update(None);
        c.update(Some(&Value::Null)); // count(x) skips NULLs
        assert_eq!(c.finalize(), Value::Int(6));

        let (mut s, rt) = state(AggFunc::Sum, "UInt64");
        assert_eq!(rt.clickhouse, "UInt64");
        s.merge_history(&[Value::Int(u64::MAX as i128)]);
        s.update(Some(&Value::Int(2)));
        assert_eq!(
            s.finalize(),
            Value::Int(1),
            "UInt64 wraps, as in ClickHouse"
        );

        let (mut s, _) = state(AggFunc::Sum, "Int32");
        s.update(Some(&Value::Int(-7)));
        s.update(Some(&Value::Int(3)));
        assert_eq!(s.finalize(), Value::Int(-4));

        let (s, rt) = state(AggFunc::Sum, "Nullable(UInt8)");
        assert_eq!(rt.clickhouse, "Nullable(UInt64)");
        assert_eq!(s.finalize(), Value::Null, "no values in a NULL-able column");
    }

    #[test]
    fn avg() {
        let (mut a, rt) = state(AggFunc::Avg, "UInt32");
        assert_eq!(rt.clickhouse, "Float64");
        assert!(matches!(a.finalize(), Value::Float(f) if f.is_nan()));
        a.merge_history(&[Value::Int(18), Value::Int(3)]);
        a.update(Some(&Value::Int(10)));
        a.update(Some(&Value::Null));
        assert_eq!(a.finalize(), Value::Float(7.0));
        let (mut b, _) = state(AggFunc::Avg, "UInt32");
        b.update(Some(&Value::Int(2)));
        a.merge(b.as_ref());
        assert_eq!(a.finalize(), Value::Float(6.0));
        let (n, _) = state(AggFunc::Avg, "Nullable(Float64)");
        assert_eq!(n.finalize(), Value::Null);
    }

    #[test]
    fn min_max() {
        let (mut m, rt) = state(AggFunc::Min, "LowCardinality(String)");
        assert_eq!(rt.clickhouse, "String");
        assert_eq!(
            m.finalize(),
            Value::String(String::new()),
            "empty: the zero value"
        );
        // an empty group's zero value from ClickHouse isn't a real minimum
        m.merge_history(&[Value::String(String::new()), Value::Int(0)]);
        m.update(Some(&Value::String("b".into())));
        m.update(Some(&Value::String("a".into())));
        m.update(Some(&Value::Null));
        assert_eq!(m.finalize(), Value::String("a".into()));
        let (mut x, _) = state(AggFunc::Max, "Nullable(Int8)");
        assert_eq!(x.finalize(), Value::Null);
        x.merge_history(&[Value::Int(-3), Value::Int(2)]);
        x.update(Some(&Value::Int(-5)));
        assert_eq!(x.finalize(), Value::Int(-3));
        let (mut y, _) = state(AggFunc::Max, "Nullable(Int8)");
        y.update(Some(&Value::Int(9)));
        x.merge(y.as_ref());
        assert_eq!(x.finalize(), Value::Int(9));
    }

    #[test]
    fn fallback_types_and_errors() {
        let t = |f, a: &str| result_type(f, Some(&Type::parse(a))).map(|t| t.clickhouse);
        assert_eq!(t(AggFunc::UniqExact, "String").unwrap(), "UInt64");
        assert_eq!(
            t(AggFunc::Median, "Nullable(Int8)").unwrap(),
            "Nullable(Float64)"
        );
        assert_eq!(t(AggFunc::Any, "LowCardinality(String)").unwrap(), "String");
        assert!(t(AggFunc::Avg, "String").is_err());
        assert!(native(AggFunc::UniqExact).is_none());
    }
}
