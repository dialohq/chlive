//! The native backend's driver: a [`NativeState`] (each group's aggregate states, in the engine)
//! fed by the history from main ClickHouse, then by batches of events, and woken by the clock
//! at the earliest deadline of a time window (plus the server's coalescing window).

use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt};

use super::native_state::NativeState;
use super::result::GroupResult;
use crate::checkpoint::{Checkpoint, Checkpointer};
use crate::clock::{Clock, SystemClock};
use crate::engine::{EngineError, Iteration, Origin};
use crate::history::HistorySource;
use crate::plan::{NativeAggregates, StreamPlan};
use crate::source::Event;
use crate::watermark::Watermark;

pub(crate) struct NativeRun {
    history: Arc<dyn HistorySource>,
    /// The history query, until it has run.
    history_sql: Option<String>,
    /// The snapshot it reads, until then.
    snapshot: Option<crate::snapshot::Lease>,
    /// Restored from a checkpoint: the first step is an empty catch-up.
    restored: bool,
    state: NativeState,
    events: BoxStream<'static, Vec<Event>>,
    failed: bool,
    /// `now()`'s clock, and how long after the earliest deadline the rows due leave together.
    clock: Arc<dyn Clock>,
    coalescing: i128,
    /// The events ended (a finite source): only the clock is left.
    ended: bool,
}

impl NativeRun {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        history: Arc<dyn HistorySource>,
        history_sql: String,
        snapshot: crate::snapshot::Lease,
        plan: NativeAggregates,
        stream: StreamPlan,
        events: BoxStream<'static, Vec<Event>>,
        at: Watermark,
        result: GroupResult,
    ) -> Self {
        Self {
            history,
            history_sql: Some(history_sql),
            snapshot: Some(snapshot),
            restored: false,
            state: NativeState::new(plan, stream, at, result),
            events,
            failed: false,
            clock: Arc::new(SystemClock),
            coalescing: 0,
            ended: false,
        }
    }

    /// `now()`'s clock and coalescing window (default: the system's, none).
    pub(crate) fn with_clock(
        mut self,
        clock: Arc<dyn Clock>,
        coalescing: std::time::Duration,
    ) -> Self {
        self.clock = clock;
        self.coalescing = coalescing.as_nanos() as i128;
        self
    }

    /// From a checkpoint (its `state`): every group's states and the ranking, at `at`.
    pub(crate) fn restored(
        history: Arc<dyn HistorySource>,
        plan: NativeAggregates,
        stream: StreamPlan,
        events: BoxStream<'static, Vec<Event>>,
        checkpoint: &Checkpoint,
        result: GroupResult,
    ) -> Result<Self, String> {
        Ok(Self {
            history,
            history_sql: None,
            snapshot: None,
            restored: true,
            state: NativeState::restored(plan, stream, checkpoint, result)?,
            events,
            failed: false,
            clock: Arc::new(SystemClock),
            coalescing: 0,
            ended: false,
        })
    }

    /// Reads the groups' states and ranking, between steps.
    pub(crate) fn checkpointer(&self) -> Checkpointer {
        self.state.checkpointer()
    }

    pub(crate) async fn step(&mut self) -> Option<Result<Iteration, EngineError>> {
        if self.failed {
            return None;
        }
        if std::mem::take(&mut self.restored) {
            return Some(Ok(Iteration {
                origin: Origin::CatchUp,
                diffs: Vec::new(),
                at: self.state.at(),
            }));
        }
        let result = match self.history_sql.take() {
            Some(sql) => self.history(&sql).await,
            None => loop {
                // wake at the earliest deadline (none: only events), re-armed every turn
                let earliest = self.state.earliest();
                let wake = crate::clock::wait_for(self.clock.as_ref(), earliest, self.coalescing);
                if self.ended {
                    // nothing more to come, by event or by clock: the end
                    earliest?;
                    wake.await;
                } else {
                    tokio::select! {
                        biased;
                        _ = wake => {}
                        batch = self.events.next() => {
                            match batch {
                                Some(batch) => break Ok(self.state.batch(batch, self.clock.now_ns())),
                                None => {
                                    self.ended = true;
                                    continue;
                                }
                            }
                        }
                    }
                }
                if let Some(it) = self.state.expire(self.clock.now_ns()) {
                    break Ok(it);
                }
            },
        };
        self.failed = result.is_err();
        Some(result)
    }

    async fn history(&mut self, sql: &str) -> Result<Iteration, EngineError> {
        let rows = self.history.fetch(sql).await;
        self.snapshot = None;
        Ok(self.state.history(rows?, self.clock.now_ns())?)
    }
}
