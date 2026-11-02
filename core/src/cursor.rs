//! The resume cursor: how far a client's result goes, as an opaque token.
//!
//! A position is the per-partition offset up to which a result is complete (a [`Watermark`]),
//! counting events that didn't match too. The token also carries a fingerprint of the plan, so
//! a cached result is only resumed against the same table, columns, types and query, and,
//! from a server sharing results, the lineage of the result it came from (see `chlive-server`'s
//! hub): positions only mean the same rows within one lineage.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::plan::Plan;
use crate::watermark::Watermark;

const VERSION: u64 = 2;

/// Everything the result depends on, as text: the history query (table, columns, filter,
/// ORDER BY, LIMIT, parameter values), OFFSET, a sub-query's condition, the aggregation (its
/// functions, keys, ORDER BY, LIMIT, OFFSET), the output column types and the topics. Two queries with the same identity have the same result.
pub fn identity(plan: &Plan) -> String {
    let mut text = plan.history.sql(&plan.history.table);
    if let Some(m) = plan.stream.offset {
        text.push_str(&format!("|offset:{m}"));
    }
    if let Some(s) = &plan.semi {
        text.push_str(&format!("|semi:{}", s.condition(&s.inner_table)));
    }
    // GROUP BY: the aggregates, keys, filter and its ORDER BY/LIMIT/OFFSET (the plain history
    // query above is the same for any aggregate over the same table)
    if let Some(a) = &plan.aggregate {
        text.push_str(&format!("|aggregate:{a:?}"));
    }
    for c in &plan.columns {
        text.push_str(&format!("|{}:{}", c.name, c.ty.clickhouse));
    }
    for t in &plan.stream.topics {
        text.push_str(&format!("|topic:{t}"));
    }
    text
}

/// A stable hash (FNV-1a, 64 bit) of the plan's [`identity`].
pub fn fingerprint(plan: &Plan) -> String {
    hash(&identity(plan))
}

/// FNV-1a, 64 bit, as 16 hex digits.
pub fn hash(text: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in text.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{h:016x}")
}

/// A decoded token: the position, and the lineage of the result it came from, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    pub at: Watermark,
    pub lineage: Option<String>,
}

/// `{"v":2,"fp":"…","o":{"0":123,"1":98}}`
pub fn encode(plan: &Plan, at: &Watermark) -> String {
    encode_with(plan, None, at)
}

/// With the lineage of the result: `{"v":2,"fp":"…","l":"…","o":{…}}`.
pub fn encode_with(plan: &Plan, lineage: Option<&str>, at: &Watermark) -> String {
    let offsets: serde_json::Map<String, Value> =
        at.0.iter()
            .map(|(p, o)| (p.to_string(), json!(o)))
            .collect();
    let mut v = json!({"v": VERSION, "fp": fingerprint(plan), "o": offsets});
    if let Some(l) = lineage {
        v["l"] = json!(l);
    }
    v.to_string()
}

/// The position in a token, or `None` if it isn't one of ours for this plan.
pub fn decode(plan: &Plan, token: &str) -> Option<Watermark> {
    decode_cursor(plan, token).map(|c| c.at)
}

/// The position in a token without checking its plan (a client, which has the result's cursors
/// but not its plan's fingerprint), or `None` if it isn't one of ours.
pub fn position(token: &str) -> Option<Watermark> {
    let v: Value = serde_json::from_str(token).ok()?;
    if v["v"].as_u64()? != VERSION {
        return None;
    }
    offsets(&v)
}

fn offsets(v: &Value) -> Option<Watermark> {
    let mut offsets = BTreeMap::new();
    for (p, o) in v["o"].as_object()? {
        offsets.insert(p.parse().ok()?, o.as_i64()?);
    }
    Some(Watermark(offsets))
}

/// The position and lineage in a token, or `None` if it isn't one of ours for this plan (another
/// plan, another version, or garbage).
pub fn decode_cursor(plan: &Plan, token: &str) -> Option<Cursor> {
    let v: Value = serde_json::from_str(token).ok()?;
    if v["v"].as_u64()? != VERSION || v["fp"].as_str()? != fingerprint(plan) {
        return None;
    }
    Some(Cursor {
        at: offsets(&v)?,
        lineage: v["l"].as_str().map(String::from),
    })
}
