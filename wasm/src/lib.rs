//! chlive-core for the TS client: a result's rows ([`Store`], applying the server's snapshots and
//! diffs), and a stream plan placing rows as the server would ([`Predictor`]). Messages cross
//! the boundary as JSON text (what the WebSocket gives) or as JS values (`*_value`).

use chlive_core::bound::{BoundColumn, BoundQuery, BoundTable, ColumnId, OrderKey, Projected};
use chlive_core::event::Event;
use chlive_core::plan::{plan, StreamPlan};
use chlive_core::result::{Applied, ResultRows};
use chlive_core::rules::{ColumnSource, TopicId};
use chlive_core::state::{history_iteration, ResultState};
use chlive_core::table_engine::TableEngine;
use chlive_core::types::Type;
use chlive_core::watermark::Watermark;
use chlive_protocol::{Change, Row, ServerMsg};
use serde_json::{json, Value};
use wasm_bindgen::prelude::*;

/// A result's rows: snapshots and diffs applied as the server sends them (atomically: a diff
/// that doesn't fit changes nothing).
#[wasm_bindgen]
pub struct Store {
    rows: ResultRows<Row>,
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
impl Store {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Store {
        Store {
            rows: ResultRows::json(),
        }
    }

    /// A `snapshot` or `diff` message, as the WebSocket's text. Returns the changes as applied,
    /// as JSON (`[]` for a snapshot): `[0, index|null, row]` an insert, `[1, index]` a delete,
    /// `[2, index|null, row]` an update (the whole row now; with an index, it moved there).
    pub fn apply(&mut self, text: &str) -> Result<String, JsError> {
        let msg: ServerMsg =
            serde_json::from_str(text).map_err(|e| JsError::new(&e.to_string()))?;
        let applied = self.apply_msg(msg)?;
        Ok(serde_json::to_string(&applied).expect("JSON"))
    }

    /// [`apply`](Self::apply), with the message as a JS value (already parsed).
    pub fn apply_value(&mut self, msg: JsValue) -> Result<JsValue, JsError> {
        let msg: ServerMsg = serde_wasm_bindgen::from_value(msg)?;
        let applied = self.apply_msg(msg)?;
        Ok(serde_wasm_bindgen::to_value(&applied)?)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Every row, as JSON text.
    pub fn rows(&self) -> String {
        let all = self.rows.rows();
        let rows: Vec<&Row> = all.iter().map(|r| r.as_ref()).collect();
        serde_json::to_string(&rows).expect("JSON")
    }

    /// One row, as JSON text.
    pub fn row(&self, index: usize) -> Option<String> {
        let rows = self.rows.rows();
        rows.get(index)
            .map(|r| serde_json::to_string(r.as_ref()).expect("JSON"))
    }
}

impl Store {
    fn apply_msg(&mut self, msg: ServerMsg) -> Result<Vec<Value>, JsError> {
        let err = |e: String| JsError::new(&e);
        match msg {
            ServerMsg::Snapshot { rows, ids, .. } => {
                self.rows.snapshot(rows, ids).map_err(err)?;
                Ok(Vec::new())
            }
            ServerMsg::Diff { changes, .. } => {
                let applied = self.rows.diff(changes).map_err(err)?;
                Ok(applied.into_iter().map(compact).collect())
            }
            _ => Ok(Vec::new()),
        }
    }
}

fn compact(a: Applied<Row>) -> Value {
    match a {
        Applied::Insert { row, index, .. } => json!([0, index, row.as_ref()]),
        Applied::Delete { index, .. } => json!([1, index]),
        Applied::Update { row, index, .. } => json!([2, index, row.as_ref()]),
    }
}

/// A stream plan running on the client: the result's state seeded from its rows, then each
/// row placed exactly as the server would (the diffs it makes).
#[wasm_bindgen]
pub struct Predictor {
    plan: StreamPlan,
    state: ResultState,
    at: Watermark,
    next: i64,
}

#[wasm_bindgen]
impl Predictor {
    /// For measuring, until plans come from the server: `select ts, user_id, latency_ms from
    /// events order by latency_ms desc [limit n]` (`limit` 0: none), with `events(ts
    /// DateTime64(3), user_id String, latency_ms UInt32)`.
    #[wasm_bindgen(constructor)]
    pub fn new(limit: u32) -> Predictor {
        let plan = plan(&ordered_query((limit > 0).then_some(limit as u64))).stream;
        Predictor {
            state: ResultState::for_plan(&plan),
            plan,
            at: Watermark::default(),
            next: 0,
        }
    }

    /// The result's rows (JSON text of an array), as the history.
    pub fn seed(&mut self, rows: &str) -> Result<usize, JsError> {
        let rows: Vec<Row> =
            serde_json::from_str(rows).map_err(|e| JsError::new(&e.to_string()))?;
        let it = history_iteration(&self.plan, &mut self.state, &self.at, rows)
            .map_err(|e| JsError::new(&e.to_string()))?;
        Ok(it.diffs.len())
    }

    /// A new row (JSON text of an object), as an event: the changes it makes, as JSON (the
    /// protocol's `changes`).
    pub fn place(&mut self, row: &str) -> Result<String, JsError> {
        let value: Row = serde_json::from_str(row).map_err(|e| JsError::new(&e.to_string()))?;
        self.next += 1;
        let event = Event {
            topic: TopicId("events".into()),
            partition: 0,
            offset: self.next,
            timestamp: None,
            value,
        };
        let decoded = self
            .plan
            .decode(&event)
            .map_err(|e| JsError::new(&format!("{e:?}")))?;
        let diffs = self.state.event(&self.plan, &decoded, (0, self.next));
        let changes: Vec<Change> = diffs.into_iter().map(|d| d.into_change()).collect();
        Ok(serde_json::to_string(&changes).expect("JSON"))
    }
}

fn ordered_query(limit: Option<u64>) -> BoundQuery {
    let col = |i: usize, n: &str, ty: &str| BoundColumn {
        id: ColumnId(i),
        name: n.into(),
        clickhouse: n.into(),
        source: ColumnSource::Json(n.into()),
        ty: Type::parse(ty),
    };
    let table = BoundTable {
        name: "events".into(),
        alias: None,
        clickhouse_table: "default.events".into(),
        database: "default".into(),
        clickhouse_name: "events".into(),
        topic: TopicId("events".into()),
        engine: TableEngine::MergeTree,
        internal: Vec::new(),
        final_key: None,
        denied: Vec::new(),
        columns: vec![
            col(0, "ts", "DateTime64(3)"),
            col(1, "user_id", "String"),
            col(2, "latency_ms", "UInt32"),
        ],
    };
    BoundQuery {
        projection: (0..3)
            .map(|i| Projected {
                column: ColumnId(i),
                name: table.columns[i].name.clone(),
            })
            .collect(),
        table,
        filter: None,
        order: vec![OrderKey {
            column: ColumnId(2),
            desc: true,
            nulls_first: false,
        }],
        limit,
        offset: None,
        aggregation: None,
        semi_join: None,
        windows: Vec::new(),
    }
}
