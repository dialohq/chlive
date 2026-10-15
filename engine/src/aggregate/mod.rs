//! Aggregate functions: what each one computes, and how.
//!
//! Every function has a ClickHouse name and a result type. Most also have a **native**
//! implementation ([`AggregateFunction`]): its per-group state lives in the engine, is seeded from
//! ClickHouse (plain SQL, e.g. `sum(x), count(x)` for `avg`), updated per event and finalized in
//! Rust. A query whose aggregates are all native runs without chDB; any other query runs entirely
//! in chDB, on ClickHouse's own `-State`/`-Merge` (see `chdb_run`).

pub mod chdb_run;
pub mod native;
pub mod native_run;
pub mod result;

use std::any::Any;

use crate::binder::AggFunc;
use crate::types::{IntType, Kind, Type, Value};

/// One aggregate function, implemented natively.
pub trait AggregateFunction: Send + Sync + 'static {
    /// ClickHouse expressions, with their types, whose values for one group make up its state
    /// (e.g. `avg(x)`: `sum(x)` and `count(x)`). `arg` is the argument's SQL.
    fn history_sql(
        &self,
        arg: Option<&str>,
        arg_ty: Option<&Type>,
        result_ty: &Type,
    ) -> Vec<(String, Type)>;
    /// A new, empty state for one group.
    fn new_state(&self, arg_ty: Option<&Type>, result_ty: &Type) -> Box<dyn AggregateState>;
}

/// One group's state of one aggregate.
pub trait AggregateState: Send + Sync {
    /// Merge in a state read from ClickHouse: the `history_sql` columns' values, decoded.
    fn merge_history(&mut self, values: &[Value]);
    /// One event: its argument's value (`Value::Null` for NULL); `None` for `count()`.
    fn update(&mut self, value: Option<&Value>);
    /// Merge another state of the same function.
    fn merge(&mut self, other: &dyn AggregateState);
    /// The result, as ClickHouse computes it.
    fn finalize(&self) -> Value;
    /// The state as JSON, for checkpoints.
    fn save(&self) -> serde_json::Value;
    /// Back to a saved state (this one made by `new_state` for the same aggregate).
    fn load(&mut self, json: &serde_json::Value) -> Result<(), String>;
    fn as_any(&self) -> &dyn Any;
    fn box_clone(&self) -> Box<dyn AggregateState>;
}

/// A function by its name in a query (case-insensitive, as ClickHouse's are for these).
pub fn lookup(name: &str) -> Option<AggFunc> {
    Some(match name.to_ascii_lowercase().as_str() {
        "count" => AggFunc::Count,
        "sum" => AggFunc::Sum,
        "avg" => AggFunc::Avg,
        "min" => AggFunc::Min,
        "max" => AggFunc::Max,
        "uniq" => AggFunc::Uniq,
        "uniqexact" => AggFunc::UniqExact,
        "median" => AggFunc::Median,
        "any" => AggFunc::Any,
        "anylast" => AggFunc::AnyLast,
        "grouparray" | "array_agg" => AggFunc::GroupArray,
        _ => return None,
    })
}

pub const SUPPORTED: &str =
    "count, sum, avg, min, max, uniq, uniqExact, median, any, anyLast, groupArray (array_agg)";

/// The native implementation, if there is one.
pub fn native(func: AggFunc) -> Option<&'static dyn AggregateFunction> {
    match func {
        AggFunc::Count => Some(&native::Count),
        AggFunc::Sum => Some(&native::Sum),
        AggFunc::Avg => Some(&native::Avg),
        AggFunc::Min => Some(&native::MinMax { max: false }),
        AggFunc::Max => Some(&native::MinMax { max: true }),
        _ => None,
    }
}

/// The bare type: no LowCardinality, no Nullable.
fn bare(ty: &Type) -> String {
    let mut t = ty.clickhouse.as_str();
    for _ in 0..2 {
        for wrapper in ["LowCardinality(", "Nullable("] {
            if let Some(inner) = t.strip_prefix(wrapper).and_then(|s| s.strip_suffix(')')) {
                t = inner;
            }
        }
    }
    t.to_string()
}

/// The result type, as ClickHouse gives it, or why the argument doesn't fit (`count()` has
/// none). NULL-able results for NULL-able arguments (except counts).
pub fn result_type(func: AggFunc, arg: Option<&Type>) -> Result<Type, String> {
    let Some(ty) = arg else {
        return match func {
            AggFunc::Count => Ok(Type::parse("UInt64")),
            f => Err(format!("{}() needs a column", f.name())),
        };
    };
    let nullable = |t: &str| {
        Type::parse(&if ty.nullable {
            format!("Nullable({t})")
        } else {
            t.to_string()
        })
    };
    let numeric = ty.kind.is_numeric();
    match func {
        AggFunc::Count | AggFunc::Uniq | AggFunc::UniqExact => Ok(Type::parse("UInt64")),
        AggFunc::Sum | AggFunc::Avg | AggFunc::Median if !numeric => {
            Err(format!("{}() needs a number", func.name()))
        }
        AggFunc::Sum => Ok(nullable(match ty.kind {
            Kind::Int(IntType { signed: true, .. }) => "Int64",
            Kind::Int(IntType { signed: false, .. }) => "UInt64",
            _ => "Float64",
        })),
        AggFunc::Avg | AggFunc::Median => Ok(nullable("Float64")),
        // NULLs are skipped, so the elements aren't Nullable; nor LowCardinality
        AggFunc::GroupArray => Ok(Type::parse(&format!("Array({})", bare(ty)))),
        AggFunc::Min | AggFunc::Max | AggFunc::Any | AggFunc::AnyLast => {
            if !ty.ops().min_max {
                Err(format!("{}() of this type is not supported", func.name()))
            } else {
                Ok(nullable(&bare(ty)))
            }
        }
    }
}
