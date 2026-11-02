//! A client's own rows, shown before the server has them (optimistic updates): placed in a
//! result exactly as the server will place them, by the subscription's plan ([`ClientPlan`])
//! and the server's own placing code ([`ResultState`]), over what the client has of the result:
//! its rows, kept in step with the server's diffs.
//!
//! The server's state is seeded from the rows as a history would seed it. What the client
//! doesn't have (rows past a FINAL window, rows above an OFFSET) it can't place against, so
//! OFFSET isn't predicted at all ([`crate::plan::Plan::client`]), and FINAL windows place
//! against the rows shown.

use std::sync::Arc;

use chlive_protocol::{Change, Row};
use serde::Deserialize;

use crate::diff::Diff;
use crate::order::{SortKey, WindowState};
use crate::plan::{ClientPlan, StreamPlan};
use crate::result::ResultRows;
use crate::state::ResultState;
use crate::types::Value;

/// A result's predictions: its rows as placing needs them, and the state seeded from them.
pub struct Predictor {
    plan: Arc<StreamPlan>,
    /// The column the server stamps with the user's name, and the user.
    stamp: Option<(String, String)>,
    rows: ResultRows<Placed>,
    /// The state seeded from `rows`, until they change.
    seeded: Option<ResultState>,
}

/// What placing needs of a row the result has: its sort key (with ORDER BY), and with FINAL
/// its id and version.
struct Placed {
    key: SortKey,
    id: String,
    ver: Value,
}

/// A row of the client's own, inserted and not in the result yet.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Pending {
    /// As inserted: by column name.
    pub row: Row,
    /// Its Kafka partition and offset, once the server acknowledged it.
    #[serde(default)]
    pub at: Option<(i32, i64)>,
}

impl Predictor {
    /// For a subscription's plan, inserting as `user`.
    pub fn new(plan: ClientPlan, user: Option<&str>) -> Result<Self, String> {
        if plan.v != ClientPlan::VERSION {
            return Err(format!(
                "a plan of version {}, not {}",
                plan.v,
                ClientPlan::VERSION
            ));
        }
        let stamp = plan
            .user_column
            .zip(user)
            .map(|(column, user)| (column, user.to_string()));
        let stream = Arc::new(plan.stream);
        let read = stream.clone();
        Ok(Self {
            plan: stream,
            stamp,
            rows: ResultRows::new(move |row| placed(&read, &row)),
            seeded: None,
        })
    }

    /// The result's rows, as the client has them (a snapshot's, or since; with their ids).
    pub fn seed(&mut self, rows: Vec<Row>, ids: Option<Vec<String>>) -> Result<(), String> {
        self.seeded = None;
        self.rows.snapshot(rows, ids)
    }

    /// The server's next diff of the result. On an error the rows are out of step: seed again.
    pub fn apply(&mut self, changes: Vec<Change>) -> Result<(), String> {
        self.seeded = None;
        self.rows.diff(changes).map(drop)
    }

    /// The changes that make the result's rows what to show (`now_ms`: the time, for `now()`
    /// and a pending row's Kafka timestamp): each pending row, in order, placed as the server
    /// will place it. A row the server would refuse (or one that doesn't match) adds nothing.
    pub fn view(&mut self, pending: &[Pending], now_ms: i64) -> Vec<Change> {
        let plan = self.plan.clone();
        let base = match &self.seeded {
            Some(state) => state,
            None => self.seeded.insert(seed(&plan, &self.rows)),
        };
        let mut state = base.clone();
        let now_ns = i128::from(now_ms) * 1_000_000;
        let mut changes = Vec::new();
        for p in pending {
            let mut row = p.row.clone();
            if let Some((column, user)) = &self.stamp {
                row.insert(column.clone(), user.clone().into());
            }
            // not acknowledged yet: after every offset there is
            let at = p.at.unwrap_or((i32::MAX, i64::MAX));
            let kafka = (p.at.map_or(0, |a| a.0), at.1, Some(now_ms));
            let Ok(decoded) = plan.decode_insert(&row, kafka) else {
                continue;
            };
            let matches = match plan.timed() {
                true => plan.schedule(&decoded, now_ns).in_now,
                false => plan.matches(&decoded),
            };
            changes.extend(
                state
                    .event_matching(&plan, &decoded, at, matches)
                    .into_iter()
                    .map(Diff::into_change),
            );
        }
        changes
    }
}

/// Whether a result complete to cursor `at` has the event at (`partition`, `offset`).
pub fn covers(at: &str, partition: i32, offset: i64) -> bool {
    crate::cursor::position(at).is_some_and(|w| w.get(partition) >= offset)
}

fn placed(plan: &StreamPlan, row: &Row) -> Result<Placed, String> {
    let decoded = plan.decode_output(row).map_err(|e| e.to_string())?;
    let key = plan
        .order
        .as_ref()
        .map(|o| o.event_key(&decoded))
        .unwrap_or_default();
    let (id, ver) = match &plan.keyed {
        Some(k) => {
            let (id, ver, _) = k.event(&decoded);
            (id, ver)
        }
        None => (String::new(), Value::Null),
    };
    Ok(Placed { key, id, ver })
}

/// The state the server would have after a history of these rows.
fn seed(plan: &StreamPlan, rows: &ResultRows<Placed>) -> ResultState {
    let mut state = ResultState::for_plan(plan);
    let rows = rows.rows();
    match &mut state {
        ResultState::Append => {}
        ResultState::Window(w) => *w = WindowState::with_len(w.limit(), rows.len() as u64),
        ResultState::Ordered(o) => rows.iter().for_each(|p| o.push_history(p.key.clone())),
        ResultState::Keyed(k) => {
            k.history(
                rows.iter()
                    .map(|p| (p.id.clone(), p.ver.clone(), p.key.clone(), Row::new()))
                    .collect(),
            );
        }
        // never planned for a client
        ResultState::Offset(_) | ResultState::Held(_) => {}
    }
    state
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_covers_the_offsets_up_to_its_own() {
        let at = r#"{"v":2,"fp":"0123456789abcdef","l":"x","o":{"0":41,"2":7}}"#;
        assert!(covers(at, 0, 41));
        assert!(covers(at, 2, 3));
        assert!(!covers(at, 0, 42));
        // a partition it doesn't have: nothing of it yet
        assert!(!covers(at, 1, 0));
        assert!(!covers("garbage", 0, 0));
        assert!(!covers(r#"{"v":1,"o":{"0":41}}"#, 0, 1));
    }
}
