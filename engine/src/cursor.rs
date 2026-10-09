//! The resume cursor: how far a client's result goes, as an opaque token.
//!
//! A position is the per-partition offset up to which a result is complete (a [`Watermark`]),
//! counting events that didn't match too. The token also carries a fingerprint of the plan, so
//! a cached result is only resumed against the same table, columns, types and query.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::plan::Plan;
use crate::watermark::Watermark;

const VERSION: u64 = 1;

/// A stable hash (FNV-1a, 64 bit) of everything the result depends on: the history query
/// (table, columns, filter, ORDER BY, LIMIT), the output column types and the topics.
pub fn fingerprint(plan: &Plan) -> String {
    let mut text = plan.history.sql(&plan.history.table);
    if let Some(m) = plan.stream.offset {
        text.push_str(&format!("|offset:{m}"));
    }
    if let Some(s) = &plan.semi {
        text.push_str(&format!("|semi:{}", s.condition(&s.inner_table)));
    }
    for c in &plan.columns {
        text.push_str(&format!("|{}:{}", c.name, c.ty.clickhouse));
    }
    for t in &plan.stream.topics {
        text.push_str(&format!("|topic:{t}"));
    }
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in text.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{h:016x}")
}

/// `{"v":1,"fp":"…","o":{"0":123,"1":98}}`
pub fn encode(plan: &Plan, at: &Watermark) -> String {
    let offsets: serde_json::Map<String, Value> =
        at.0.iter()
            .map(|(p, o)| (p.to_string(), json!(o)))
            .collect();
    json!({"v": VERSION, "fp": fingerprint(plan), "o": offsets}).to_string()
}

/// The position in a token, or `None` if it isn't one of ours for this plan.
pub fn decode(plan: &Plan, token: &str) -> Option<Watermark> {
    let v: Value = serde_json::from_str(token).ok()?;
    if v["v"].as_u64()? != VERSION || v["fp"].as_str()? != fingerprint(plan) {
        return None;
    }
    let mut offsets = BTreeMap::new();
    for (p, o) in v["o"].as_object()? {
        offsets.insert(p.parse().ok()?, o.as_i64()?);
    }
    Some(Watermark(offsets))
}
