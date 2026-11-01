//! A running subscription's placement state at a cursor, saved so the subscription can be
//! restored later (on another day, after a restart) and continue from Kafka right after the
//! cursor, without a snapshot or a history query. The rows themselves aren't in it: whoever
//! keeps the result (the server's shared entries) keeps them.
//!
//! Only some shapes have one: the plain ones (append, LIMIT, ORDER BY, OFFSET with or without
//! ORDER BY) and native aggregates. chDB aggregates and semi-joins don't.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{json, Value as Json};

use crate::watermark::Watermark;

/// The format's version; another one doesn't load.
pub const VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct Checkpoint {
    /// The cursor the state is complete to: restoring continues from right after it.
    pub at: Watermark,
    /// The shape's own state (`{"shape": "ordered", …}`).
    pub state: Json,
}

impl Checkpoint {
    /// `append`, `window`, `ordered`, `offset`, `held` or `native`.
    pub fn shape(&self) -> &str {
        self.state["shape"].as_str().unwrap_or("")
    }

    pub fn to_json(&self) -> Json {
        let at: BTreeMap<String, i64> =
            self.at.0.iter().map(|(p, o)| (p.to_string(), *o)).collect();
        json!({ "v": VERSION, "at": at, "state": self.state })
    }

    pub fn from_json(json: &Json) -> Result<Self, String> {
        if json["v"].as_u64() != Some(VERSION) {
            return Err(format!(
                "checkpoint: version {} (this is {VERSION})",
                json["v"]
            ));
        }
        let mut at = BTreeMap::new();
        for (p, o) in json["at"].as_object().ok_or("checkpoint: no cursor")? {
            let p: i32 = p
                .parse()
                .map_err(|_| format!("checkpoint: partition {p}"))?;
            at.insert(p, o.as_i64().ok_or("checkpoint: bad offset")?);
        }
        Ok(Self {
            at: Watermark(at),
            state: json["state"].clone(),
        })
    }
}

/// Reads a running subscription's checkpoint (between iterations).
pub type Checkpointer = Arc<dyn Fn() -> Checkpoint + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_version() {
        let c = Checkpoint {
            at: Watermark([(0, 5), (2, -1)].into()),
            state: json!({"shape": "window", "len": 3}),
        };
        assert_eq!(c.shape(), "window");
        let j = c.to_json();
        assert_eq!(j["v"], 1);
        assert_eq!(Checkpoint::from_json(&j).unwrap(), c);
        let mut old = j.clone();
        old["v"] = json!(0);
        assert!(Checkpoint::from_json(&old).unwrap_err().contains("version"));
        assert!(Checkpoint::from_json(&json!({"v": 1})).is_err());
    }
}
