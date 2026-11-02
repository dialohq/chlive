//! chlive-core for the TS client: a result's predictions ([`Predictor`]: the client's own
//! pending rows placed as the server will place them), and a result's rows ([`Store`], applying
//! the server's snapshots and diffs; measured against the TS store in `bench/wasm.ts`).
//! Messages cross the boundary as JSON text (what the WebSocket gives) or as JS values
//! (`*_value`).

use chlive_core::plan::ClientPlan;
use chlive_core::predict;
use chlive_core::result::{Applied, ResultRows};
use chlive_protocol::{Row, ServerMsg};
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

/// A result's predictions (chlive-core's `Predictor`): the client's own pending rows, placed as
/// the server will place them, over the result's rows as the client has them.
#[wasm_bindgen]
pub struct Predictor {
    inner: predict::Predictor,
}

#[wasm_bindgen]
impl Predictor {
    /// For a subscription's plan (the `plan` message's `plan`, as JSON text), inserting as
    /// `user`.
    #[wasm_bindgen(constructor)]
    pub fn new(plan: &str, user: Option<String>) -> Result<Predictor, JsError> {
        let plan: ClientPlan = serde_json::from_str(plan).map_err(js)?;
        Ok(Predictor {
            inner: predict::Predictor::new(plan, user.as_deref()).map_err(|e| JsError::new(&e))?,
        })
    }

    /// The result's rows (JSON text of an array, as they came over the wire) and their ids (JSON
    /// text of an array, when every row has one).
    pub fn seed(&mut self, rows: &str, ids: Option<String>) -> Result<(), JsError> {
        let rows: Vec<Row> = serde_json::from_str(rows).map_err(js)?;
        let ids: Option<Vec<String>> = ids
            .map(|i| serde_json::from_str(&i))
            .transpose()
            .map_err(js)?;
        self.inner.seed(rows, ids).map_err(|e| JsError::new(&e))
    }

    /// The server's next `diff` for the result, as the WebSocket's text. On an error the rows
    /// are out of step: seed again.
    pub fn apply(&mut self, diff: &str) -> Result<(), JsError> {
        match serde_json::from_str(diff).map_err(js)? {
            ServerMsg::Diff { changes, .. } => {
                self.inner.apply(changes).map_err(|e| JsError::new(&e))
            }
            _ => Err(JsError::new("not a diff")),
        }
    }

    /// The changes (JSON text, the protocol's) that make the rows what to show: the pending rows
    /// (JSON text of `[{"row": {…}, "at": [partition, offset]?}, …]`) placed in order, at
    /// `now_ms`.
    pub fn view(&mut self, pending: &str, now_ms: f64) -> Result<String, JsError> {
        let pending: Vec<predict::Pending> = serde_json::from_str(pending).map_err(js)?;
        let changes = self.inner.view(&pending, now_ms as i64);
        Ok(serde_json::to_string(&changes).expect("JSON"))
    }
}

/// Whether a result complete to cursor `at` has the event at (`partition`, `offset`).
#[wasm_bindgen]
pub fn covers(at: &str, partition: i32, offset: f64) -> bool {
    predict::covers(at, partition, offset as i64)
}

fn js(e: serde_json::Error) -> JsError {
    JsError::new(&e.to_string())
}
