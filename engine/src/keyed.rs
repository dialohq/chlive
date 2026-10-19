//! FINAL results on a ReplacingMergeTree: one row per key, its latest version, as ClickHouse's
//! `SELECT … FINAL` has it. Rows have ids (their keys); each event is a new version of its key,
//! so it inserts, updates (the whole row) or deletes the key's row.
//!
//! Versions are trusted to arrive in order: an event replaces its key's row unless its version
//! is lower than the row's (ties, and tables without a version column, go to the later event,
//! as ClickHouse keeps the last inserted). That holds when each key's versions only grow and
//! all of them are in one Kafka partition (a keyed topic). Only the result's keys are kept,
//! with their versions.
//!
//! With `LIMIT n` (no ORDER BY, so any n rows will do) the result is the n most recently active
//! matching rows, newest first, and a buffer of more: a matching version of a key that isn't
//! kept comes in at the top (the last visible row slides into the buffer, whose last one is let
//! go), and a visible row that leaves is replaced by the buffer's first. When departures drain
//! the buffer, it's refilled from a newer snapshot (see [`KeyedState::refill`]).
//!
//! With ORDER BY and `LIMIT n`, the top n and the next ranks are kept, always exactly a prefix
//! of the result: a version is placed by its sort values, and one past the last kept row (the
//! bound, where what's beyond isn't known) is ignored, or leaves. Those ignored versions aren't
//! departures, so a refill can't trust a snapshot's rows as they were: it rebuilds the window
//! from the snapshot and the events since (see the engine's refills).

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use serde_json::{json, Value as Json};

use crate::diff::{Diff, Row};
use crate::order::{KeyOrder, Ranking, SortKey};
use crate::plan::{KeyedPlan, KeyedWindowPlan};
use crate::types::{decode, Value};
use crate::watermark::Watermark;

/// FINAL with LIMIT: how many rows past the window are kept to replace departures.
pub const BUFFER: u64 = 2000;
/// Departures (deleted, or no longer matching) remembered to vouch for a refill's candidates.
const GONE_LOG: usize = 100_000;
/// After a refill that couldn't be used, how long until the next.
const REFILL_RETRY: Duration = Duration::from_secs(1);

/// A new version of a key, from an event at `at` (partition, offset).
pub struct NewVersion {
    pub id: String,
    pub ver: Value,
    /// Its ORDER BY values (empty without ORDER BY).
    pub key: SortKey,
    pub deleted: bool,
    /// Whether it matches the WHERE.
    pub matches: bool,
    pub at: (i32, i64),
}

#[derive(Debug, Clone, Default)]
pub struct KeyedState {
    /// The result's rows by id: their versions (`Null` without a version column).
    versions: HashMap<String, Value>,
    /// With ORDER BY: the result's rows in order.
    ranking: Option<Ranking>,
    /// With LIMIT: the window and its buffer.
    window: Option<Box<Window>>,
    /// With ORDER BY and LIMIT: the top n and the next ranks.
    ordered_window: Option<Box<OrderedWindow>>,
    /// Events older than their key's row, ignored.
    stale: u64,
}

/// FINAL with `LIMIT n` and/or `OFFSET m`, no ORDER BY: the kept rows, newest first, the
/// first m held back, the next n visible, `capacity` more after them; and the rows of all (a
/// row moves between the parts and back). `versions` has every kept id.
#[derive(Debug, Clone)]
struct Window {
    offset: usize,
    /// `usize::MAX`: no LIMIT.
    limit: usize,
    capacity: usize,
    seq: VecDeque<String>,
    rows: HashMap<String, Row>,
    /// The kept rows are every matching row (nothing past them to refill from).
    exhaustive: bool,
    gone: Gone,
    /// A refill from a snapshot the stream hasn't reached yet: applied once it has.
    pending: Option<Refill>,
    /// A refill was asked for and hasn't come.
    requested: bool,
    /// After a refill: not before this.
    retry_at: Option<Instant>,
    /// The snapshot the last refill came from (the next needs a newer one).
    last_refill: Option<Watermark>,
    /// Each kept row's latest event, and the newest event of a row let go: a refill from a
    /// snapshot before it can't have that row, so it can't say it has every row.
    seen: HashMap<String, (i32, i64)>,
    let_go: Watermark,
}

/// FINAL with ORDER BY and `LIMIT n` and/or `OFFSET m`: ranks `[0, m + n + capacity)` of the
/// result, `[m, m+n)` visible, with their rows. `versions` has every kept id.
#[derive(Debug, Clone)]
struct OrderedWindow {
    offset: usize,
    /// `usize::MAX`: no LIMIT.
    limit: usize,
    capacity: usize,
    ranking: Ranking,
    rows: HashMap<String, Row>,
    /// Every matching row is kept: nothing past them, so no bound.
    exhaustive: bool,
    requested: bool,
    retry_at: Option<Instant>,
    last_refill: Option<Watermark>,
    /// A matching row's latest version was dropped since the last rebuild (let go, ignored past
    /// the bound, or leaving past it): a rebuild from the same snapshot can bring it back.
    lost_since: bool,
    /// A rebuild's shadow replaying the events since its snapshot: nothing is let go (what's
    /// let go is lost to it), until [`KeyedState::settle`].
    replaying: bool,
    /// How many rows its history asked for (`limit + capacity`, or a rebuild's bigger top):
    /// fewer came, so they're every matching row.
    history_limit: Option<usize>,
}

impl OrderedWindow {
    /// Whether a row with `key` would go past the last kept row, where rows aren't known.
    fn past_bound(&self, key: &SortKey) -> bool {
        !self.exhaustive && self.ranking.would_place(key) >= self.ranking.len()
    }

    /// The row at rank `i`.
    fn at(&self, i: usize) -> (String, Row) {
        let id = &self.ranking.get(i).expect("a rank").id;
        (id.clone(), self.rows[id].clone())
    }

    /// The most rows kept: the held-back ones, the window and the spare ones after it.
    fn most(&self) -> usize {
        self.offset
            .saturating_add(self.limit)
            .saturating_add(self.capacity)
    }

    fn visible(&self, i: usize) -> bool {
        i >= self.offset && i - self.offset < self.limit
    }
}

/// How many of a sequence's `len` rows are visible, when they're positions `[m, m+n)`.
fn shown(len: usize, m: usize, n: usize) -> usize {
    len.saturating_sub(m).min(n)
}

/// A row (`id`) left a sequence at position `o`; it now has `len` rows, `at(i)` the one at
/// position i, and positions `[m, m+n)` are visible. What the client's rows get: the row out
/// (if visible), or, if it was held back, the first visible one (now held back) out; then the
/// row that slid into the window's last place.
fn removed(
    o: usize,
    id: String,
    len: usize,
    (m, n): (usize, usize),
    at: impl Fn(usize) -> (String, Row),
) -> Vec<Diff> {
    let (before, after) = (shown(len + 1, m, n), shown(len, m, n));
    let mut diffs = Vec::new();
    if o < m {
        if before == 0 {
            return diffs;
        }
        diffs.push(Diff::Delete { index: 0 });
    } else if o - m < n {
        diffs.push(Diff::DeleteId { id });
    } else {
        return diffs;
    }
    if after > before - 1 {
        let (id, row) = at(m + after - 1);
        diffs.push(Diff::Insert {
            row,
            index: Some(after - 1),
            id: Some(id),
        });
    }
    diffs
}

/// A row came into a sequence at position `p`; it now has `len` rows (see [`removed`]). What
/// the client's rows get: the row in (if visible), or, if it went into the held-back part, the
/// row pushed out of it into the window's first place; then the window's last row, if it's
/// over n.
fn inserted(
    p: usize,
    len: usize,
    (m, n): (usize, usize),
    at: impl Fn(usize) -> (String, Row),
) -> Vec<Diff> {
    let (before, after) = (shown(len - 1, m, n), shown(len, m, n));
    let mut diffs = Vec::new();
    let into = if p < m {
        if after == 0 {
            return diffs;
        }
        (m, 0)
    } else if p - m < n {
        (p, p - m)
    } else {
        return diffs;
    };
    let (id, row) = at(into.0);
    diffs.push(Diff::Insert {
        row,
        index: Some(into.1),
        id: Some(id),
    });
    if before + 1 > after {
        diffs.push(Diff::Delete { index: after });
    }
    diffs
}

/// A refill: one row per key from a snapshot at `at` (id, version, row), and whether it had
/// every matching row there.
#[derive(Debug, Clone)]
pub struct Refill {
    pub rows: Vec<(String, Value, Row)>,
    pub at: Watermark,
    pub complete: bool,
}

/// The latest departure of each key, since `from`: a version deleted, or not matching, or a
/// kept row leaving. Bounded; what's let go is remembered per partition.
#[derive(Debug, Clone, Default)]
struct Gone {
    last: HashMap<String, (i32, i64)>,
    order: VecDeque<(String, i32, i64)>,
    /// Per partition, the newest offset let go.
    dropped: HashMap<i32, i64>,
    /// Where the log starts (a restore's position; before it, nothing is known).
    from: Watermark,
}

impl Gone {
    fn record(&mut self, id: &str, (partition, offset): (i32, i64)) {
        self.last.insert(id.to_string(), (partition, offset));
        self.order.push_back((id.to_string(), partition, offset));
        while self.order.len() > GONE_LOG {
            let (id, p, o) = self.order.pop_front().expect("not empty");
            if self.last.get(&id) == Some(&(p, o)) {
                self.last.remove(&id);
            }
            let d = self.dropped.entry(p).or_insert(o);
            *d = (*d).max(o);
        }
    }

    /// Whether the key left after `w`.
    fn since(&self, id: &str, w: &Watermark) -> bool {
        self.last.get(id).is_some_and(|(p, o)| *o > w.get(*p))
    }

    /// Whether the log knows every departure after `w`.
    fn covers(&self, w: &Watermark) -> bool {
        w.covers(&self.from) && self.dropped.iter().all(|(p, o)| *o <= w.get(*p))
    }
}

impl Window {
    fn new(plan: KeyedWindowPlan) -> Self {
        Self {
            offset: plan.offset as usize,
            limit: plan.limit.map_or(usize::MAX, |n| n as usize),
            capacity: plan.buffer as usize,
            seq: VecDeque::new(),
            rows: HashMap::new(),
            exhaustive: true,
            gone: Gone::default(),
            pending: None,
            requested: false,
            retry_at: None,
            last_refill: None,
            seen: HashMap::new(),
            let_go: Watermark::default(),
        }
    }

    /// The held-back and visible rows' count, and the most rows kept.
    fn front(&self) -> usize {
        self.offset.saturating_add(self.limit)
    }

    fn most(&self) -> usize {
        self.front().saturating_add(self.capacity)
    }

    /// The spare rows after the window.
    fn spare(&self) -> usize {
        self.seq.len().saturating_sub(self.front())
    }

    fn at(&self, i: usize) -> (String, Row) {
        let id = &self.seq[i];
        (id.clone(), self.rows[id].clone())
    }

    fn visible(&self, i: usize) -> bool {
        i >= self.offset && i - self.offset < self.limit
    }
}

impl KeyedState {
    /// With ORDER BY, rows are kept in its order: each insert and move has an index.
    pub fn new(order: Option<KeyOrder>) -> Self {
        Self {
            ranking: order.map(Ranking::new),
            ..Self::default()
        }
    }

    /// With `LIMIT n`: without ORDER BY the newest n rows and a buffer; with it (`order`) the
    /// top n and the next ranks.
    pub fn with_window(plan: KeyedWindowPlan, order: Option<KeyOrder>) -> Self {
        match order {
            Some(order) => Self {
                ordered_window: Some(Box::new(OrderedWindow {
                    offset: plan.offset as usize,
                    limit: plan.limit.map_or(usize::MAX, |n| n as usize),
                    capacity: plan.buffer as usize,
                    ranking: Ranking::new(order),
                    rows: HashMap::new(),
                    exhaustive: true,
                    requested: false,
                    retry_at: None,
                    last_refill: None,
                    lost_since: false,
                    replaying: false,
                    history_limit: None,
                })),
                ..Self::default()
            },
            None => Self {
                window: Some(Box::new(Window::new(plan))),
                ..Self::default()
            },
        }
    }

    /// The same kind of state, empty: a rebuild's shadow, which lets nothing go until
    /// [`Self::settle`].
    pub fn fresh(&self) -> Self {
        let mut fresh = Self {
            ranking: self
                .ranking
                .as_ref()
                .map(|r| Ranking::new(r.order().clone())),
            ..Self::default()
        };
        if let Some(w) = &self.ordered_window {
            fresh.ordered_window = Some(Box::new(OrderedWindow {
                ranking: Ranking::new(w.ranking.order().clone()),
                rows: HashMap::new(),
                exhaustive: true,
                requested: false,
                retry_at: None,
                last_refill: None,
                lost_since: false,
                replaying: true,
                history_limit: None,
                ..(**w).clone()
            }));
        }
        fresh
    }

    /// A rebuild's shadow has replayed the events since its snapshot: down to its ranks, the
    /// rows past them let go.
    pub fn settle(&mut self) {
        let Some(w) = &mut self.ordered_window else {
            return;
        };
        w.replaying = false;
        while w.ranking.len() > w.most() {
            let last = w.ranking.get(w.ranking.len() - 1).expect("over").id.clone();
            w.ranking.remove(&last);
            self.versions.remove(&last);
            w.rows.remove(&last);
            w.exhaustive = false;
            // dropped: the same snapshot can bring it back
            w.lost_since = true;
        }
    }

    /// A rebuild's shadow reads its history with this LIMIT.
    pub fn history_limit(&mut self, limit: u64) {
        if let Some(w) = &mut self.ordered_window {
            w.history_limit = Some(limit as usize);
        }
    }

    /// With ORDER BY and LIMIT: too few rows kept, with more past them (a rebuild's shadow that
    /// needs a bigger snapshot top).
    pub fn short(&self) -> bool {
        self.ordered_window.as_ref().is_some_and(|w| {
            !w.exhaustive && w.ranking.len() < w.most() - w.capacity + w.capacity / 4
        })
    }

    /// With ORDER BY and LIMIT: refills rebuild the window.
    pub fn rebuilds(&self) -> bool {
        self.ordered_window.is_some()
    }

    /// With ORDER BY and LIMIT: the visible rows, in order.
    pub fn visible_rows(&self) -> Vec<(String, Row)> {
        let Some(w) = &self.ordered_window else {
            return Vec::new();
        };
        w.ranking
            .iter()
            .skip(w.offset)
            .take(w.limit)
            .map(|e| (e.id.clone(), w.rows[&e.id].clone()))
            .collect()
    }

    /// A rebuild from the snapshot at `at` is done: the shadow takes over, and the diff that
    /// turns the visible rows into its visible rows is what the client gets.
    pub fn rebuilt(&mut self, mut shadow: KeyedState, at: Watermark) -> Vec<Diff> {
        let diffs = reorder(&self.visible_rows(), &shadow.visible_rows());
        // no wait before the next: it needs a newer snapshot or rows let go in between, and
        // departures to drain the ranks again
        if let Some(w) = &mut shadow.ordered_window {
            w.last_refill = Some(at);
        }
        shadow.stale += self.stale;
        *self = shadow;
        diffs
    }

    /// The history: one row per key, its latest version (FINAL left out deleted ones), as
    /// (id, version, sort key, row), in ClickHouse's order. The inserts come in result order.
    pub fn history(&mut self, rows: Vec<(String, Value, SortKey, Row)>) -> Vec<Diff> {
        let insert = |id: String, row: Row| Diff::Insert {
            row,
            index: None,
            id: Some(id),
        };
        if let Some(w) = &mut self.ordered_window {
            // ranked; the top n are the result
            w.exhaustive = rows.len() < w.history_limit.unwrap_or(w.most());
            let mut keys = Vec::with_capacity(rows.len());
            for (id, ver, key, row) in rows {
                self.versions.insert(id.clone(), ver);
                w.rows.insert(id.clone(), row);
                keys.push((id, key));
            }
            w.ranking.history(keys);
            return w
                .ranking
                .iter()
                .skip(w.offset)
                .take(w.limit)
                .map(|e| insert(e.id.clone(), w.rows[&e.id].clone()))
                .collect();
        }
        if let Some(w) = &mut self.window {
            // in ClickHouse's order: the first m held back, the next n the result, the rest
            // spare
            w.exhaustive = rows.len() < w.most();
            let mut diffs = Vec::new();
            for (id, ver, _, row) in rows {
                self.versions.insert(id.clone(), ver);
                if w.visible(w.seq.len()) {
                    diffs.push(insert(id.clone(), row.clone()));
                }
                w.seq.push_back(id.clone());
                w.rows.insert(id, row);
            }
            return diffs;
        }
        let Some(ranking) = &mut self.ranking else {
            return rows
                .into_iter()
                .map(|(id, ver, _, row)| {
                    self.versions.insert(id.clone(), ver);
                    insert(id, row)
                })
                .collect();
        };
        let mut by_id: HashMap<String, Row> = HashMap::with_capacity(rows.len());
        let mut keys = Vec::with_capacity(rows.len());
        for (id, ver, key, row) in rows {
            self.versions.insert(id.clone(), ver);
            by_id.insert(id.clone(), row);
            keys.push((id, key));
        }
        ranking.history(keys);
        ranking
            .iter()
            .map(|e| insert(e.id.clone(), by_id.remove(&e.id).expect("a history row")))
            .collect()
    }

    /// A new version of a key; `row` renders it.
    pub fn event(&mut self, v: NewVersion, row: impl FnOnce() -> Row) -> Vec<Diff> {
        let NewVersion {
            id,
            ver,
            key,
            deleted,
            matches,
            at,
        } = v;
        let shown = !deleted && matches;
        if let Some(old) = self.versions.get(&id) {
            if older(&ver, old) {
                self.stale += 1;
                tracing::debug!(id, ver = ?ver, row_ver = ?old, stale = self.stale, "an older version than the row's; ignored");
                return Vec::new();
            }
        }
        if self.window.is_some() {
            return self.window_event(id, ver, shown, at, row);
        }
        if self.ordered_window.is_some() {
            return self.ordered_event(id, ver, key, shown, row);
        }
        match self.versions.get_mut(&id) {
            Some(_) if !shown => {
                self.versions.remove(&id);
                if let Some(r) = &mut self.ranking {
                    r.remove(&id);
                }
                vec![Diff::DeleteId { id }]
            }
            Some(old) => {
                *old = ver;
                // a new sort key moves the row: out, then after its new ties
                let index = self.ranking.as_mut().and_then(|r| {
                    let same = r
                        .key(&id)
                        .is_some_and(|k| r.order().cmp(k, &key) == Ordering::Equal);
                    if same {
                        return None;
                    }
                    let from = r.remove(&id);
                    let to = r.place(&id, key);
                    (from != Some(to)).then_some(to)
                });
                vec![Diff::Update {
                    id,
                    set: row(),
                    index,
                    edit: Vec::new(),
                }]
            }
            None if shown => {
                self.versions.insert(id.clone(), ver);
                let index = self.ranking.as_mut().map(|r| r.place(&id, key));
                vec![Diff::Insert {
                    row: row(),
                    index,
                    id: Some(id),
                }]
            }
            None => Vec::new(),
        }
    }

    /// A new version, with LIMIT or OFFSET (no ORDER BY): kept rows are updated, or leave (the
    /// rows behind them move up a place); a matching key not kept comes in at the very front,
    /// pushing the others back a place.
    fn window_event(
        &mut self,
        id: String,
        ver: Value,
        shown: bool,
        at: (i32, i64),
        row: impl FnOnce() -> Row,
    ) -> Vec<Diff> {
        let w = self.window.as_mut().expect("a window");
        let edges = (w.offset, w.limit);
        let position = if self.versions.contains_key(&id) {
            w.seq.iter().position(|x| *x == id)
        } else {
            None
        };
        match (position, shown) {
            (Some(i), true) => {
                let row = row();
                self.versions.insert(id.clone(), ver);
                w.seen.insert(id.clone(), at);
                w.rows.insert(id.clone(), row.clone());
                if w.visible(i) {
                    return vec![Diff::Update {
                        id,
                        set: row,
                        index: None,
                        edit: Vec::new(),
                    }];
                }
                Vec::new()
            }
            (Some(i), false) => {
                self.versions.remove(&id);
                w.rows.remove(&id);
                w.seen.remove(&id);
                w.gone.record(&id, at);
                w.seq.remove(i);
                let w = &*w;
                removed(i, id, w.seq.len(), edges, |k| w.at(k))
            }
            (None, true) if w.limit > 0 => {
                let row = row();
                self.versions.insert(id.clone(), ver);
                w.seen.insert(id.clone(), at);
                w.rows.insert(id.clone(), row);
                w.seq.push_front(id);
                let diffs = {
                    let w = &*w;
                    inserted(0, w.seq.len(), edges, |k| w.at(k))
                };
                if w.seq.len() > w.most() {
                    // let go: it still matches, but it isn't kept any more
                    let out = w.seq.pop_back().expect("over capacity");
                    self.versions.remove(&out);
                    w.rows.remove(&out);
                    if let Some((p, o)) = w.seen.remove(&out) {
                        let l = w.let_go.0.entry(p).or_insert(o);
                        *l = (*l).max(o);
                    }
                    w.exhaustive = false;
                }
                diffs
            }
            (None, true) => Vec::new(),
            (None, false) => {
                w.gone.record(&id, at);
                Vec::new()
            }
        }
    }

    /// A new version, with ORDER BY and LIMIT or OFFSET: placed by its sort values within the
    /// kept ranks (crossing the window's edges as inserts and deletes there), or, past the bound,
    /// ignored (leaving, if it was kept).
    fn ordered_event(
        &mut self,
        id: String,
        ver: Value,
        key: SortKey,
        shown: bool,
        row: impl FnOnce() -> Row,
    ) -> Vec<Diff> {
        let w = self.ordered_window.as_mut().expect("an ordered window");
        let edges = (w.offset, w.limit);
        let kept = self.versions.contains_key(&id);
        if !kept && (!shown || w.limit == 0) {
            return Vec::new();
        }
        if !kept && w.past_bound(&key) {
            // a matching row past what's known: dropped
            w.lost_since = true;
            return Vec::new();
        }
        if kept {
            let same = w
                .ranking
                .key(&id)
                .is_some_and(|k| w.ranking.order().cmp(k, &key) == Ordering::Equal);
            if shown && same {
                let row = row();
                self.versions.insert(id.clone(), ver);
                w.rows.insert(id.clone(), row.clone());
                if w.ranking.position(&id).is_some_and(|o| w.visible(o)) {
                    return vec![Diff::Update {
                        id,
                        set: row,
                        index: None,
                        edit: Vec::new(),
                    }];
                }
                return Vec::new();
            }
            let o = w.ranking.remove(&id).expect("kept");
            if !shown || w.past_bound(&key) {
                // leaves: deleted, not matching, or past what's known (then dropped)
                w.lost_since |= shown;
                self.versions.remove(&id);
                w.rows.remove(&id);
                let w = &*w;
                return removed(o, id, w.ranking.len(), edges, |k| w.at(k));
            }
            let row = row();
            self.versions.insert(id.clone(), ver);
            w.rows.insert(id.clone(), row.clone());
            let p = w.ranking.would_place(&key);
            if w.visible(o) && w.visible(p) {
                // within the window: a move
                w.ranking.place(&id, key);
                return vec![Diff::Update {
                    id,
                    set: row,
                    index: (p != o).then_some(p - w.offset),
                    edit: Vec::new(),
                }];
            }
            let mut diffs = {
                let w = &*w;
                removed(o, id.clone(), w.ranking.len(), edges, |k| w.at(k))
            };
            let p = w.ranking.place(&id, key);
            let w = &*w;
            diffs.extend(inserted(p, w.ranking.len(), edges, |k| w.at(k)));
            return diffs;
        }
        let row = row();
        self.versions.insert(id.clone(), ver);
        w.rows.insert(id.clone(), row);
        let p = w.ranking.place(&id, key);
        let diffs = {
            let w = &*w;
            inserted(p, w.ranking.len(), edges, |k| w.at(k))
        };
        if w.ranking.len() > w.most() && !w.replaying {
            // let go: it still matches, but what's past it isn't known any more
            let last = w.ranking.get(w.ranking.len() - 1).expect("over").id.clone();
            w.ranking.remove(&last);
            self.versions.remove(&last);
            w.rows.remove(&last);
            w.exhaustive = false;
            w.lost_since = true;
        }
        diffs
    }

    /// With LIMIT: whether to refill the buffer now (asked once, until it comes).
    pub fn wants_refill(&mut self) -> bool {
        if let Some(w) = &mut self.ordered_window {
            let wanted = !w.exhaustive
                && w.ranking.len() < (w.most() - w.capacity).saturating_add(w.capacity / 4)
                && !w.requested
                && w.retry_at.is_none_or(|t| Instant::now() >= t);
            if wanted {
                w.requested = true;
            }
            return wanted;
        }
        let Some(w) = &mut self.window else {
            return false;
        };
        let low = w.spare() < w.capacity / 4 || shown(w.seq.len(), w.offset, w.limit) < w.limit;
        let wanted = !w.exhaustive
            && low
            && !w.requested
            && w.pending.is_none()
            && w.retry_at.is_none_or(|t| Instant::now() >= t);
        if wanted {
            w.requested = true;
        }
        wanted
    }

    /// The snapshot the last refill came from: a refill needs a newer one.
    pub fn last_refill(&self) -> Option<Watermark> {
        match (&self.window, &self.ordered_window) {
            (Some(w), _) => w.last_refill.clone(),
            // rows dropped since: the same snapshot, replayed further, brings them back
            (_, Some(w)) if w.lost_since => None,
            (_, Some(w)) => w.last_refill.clone(),
            _ => None,
        }
    }

    /// A refill couldn't be fetched, or there was no newer snapshot: try again later.
    pub fn refill_failed(&mut self) {
        if let Some(w) = &mut self.window {
            w.requested = false;
            w.retry_at = Some(Instant::now() + REFILL_RETRY);
        }
        if let Some(w) = &mut self.ordered_window {
            w.requested = false;
            w.retry_at = Some(Instant::now() + REFILL_RETRY);
        }
    }

    /// A refill from a snapshot, with the stream at `position`. Its rows were right at the
    /// snapshot; since then, a key's matching version made it kept, and a departure is in the
    /// log, so a candidate neither kept nor gone since is still a matching row as it was. Not
    /// before the stream reaches the snapshot (then it waits), nor when the log has let go of
    /// departures after it (then it's dropped, to be asked for again).
    pub fn refill(&mut self, refill: Refill, position: &Watermark) -> Vec<Diff> {
        let Some(w) = &mut self.window else {
            return Vec::new();
        };
        w.requested = false;
        if !position.covers(&refill.at) {
            w.pending = Some(refill);
            return Vec::new();
        }
        if !w.gone.covers(&refill.at) {
            tracing::info!(at = %refill.at.describe(), "a refill from a snapshot older than the departures known: asking again");
            w.retry_at = Some(Instant::now() + REFILL_RETRY);
            return Vec::new();
        }
        w.last_refill = Some(refill.at.clone());
        // every matching row: the snapshot had all of them, none was let go after it, and
        // they all fit
        let mut all = refill.complete && refill.at.covers(&w.let_go);
        let mut diffs = Vec::new();
        let edges = (w.offset, w.limit);
        for (id, ver, row) in refill.rows {
            if self.versions.contains_key(&id) || w.gone.since(&id, &refill.at) {
                continue;
            }
            if w.seq.len() >= w.most() {
                all = false;
                break;
            }
            // at the back: into the held-back part, the window or the spare rows
            w.seq.push_back(id.clone());
            self.versions.insert(id.clone(), ver);
            w.rows.insert(id, row);
            let w = &*w;
            diffs.extend(inserted(w.seq.len() - 1, w.seq.len(), edges, |k| w.at(k)));
        }
        w.exhaustive = all;
        w.retry_at = Some(Instant::now() + REFILL_RETRY);
        diffs
    }

    /// After an event: a pending refill, once the stream has reached its snapshot.
    pub fn advanced(&mut self, position: &Watermark) -> Vec<Diff> {
        let pending = match &mut self.window {
            Some(w) if w.pending.as_ref().is_some_and(|r| position.covers(&r.at)) => {
                w.pending.take()
            }
            _ => None,
        };
        match pending {
            Some(r) => self.refill(r, position),
            None => Vec::new(),
        }
    }

    /// Restored from a checkpoint at `at`: departures before it aren't known.
    pub fn restored_at(&mut self, at: &Watermark) {
        if let Some(w) = &mut self.window {
            w.gone.from = at.clone();
            // rows let go before it aren't known either
            w.let_go = at.clone();
        }
    }

    /// The version of a key's row in the result.
    pub fn version(&self, id: &str) -> Option<&Value> {
        self.versions.get(id)
    }

    pub fn len(&self) -> usize {
        self.versions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    /// For a checkpoint: `{"versions": {id: version}}`, with ORDER BY and `"ranking"`.
    pub fn save(&self) -> Json {
        let versions: serde_json::Map<String, Json> = self
            .versions
            .iter()
            .map(|(id, v)| (id.clone(), v.to_json()))
            .collect();
        let mut json = json!({ "versions": versions });
        if let Some(r) = &self.ranking {
            json["ranking"] = r.save();
        }
        if let Some(w) = &self.ordered_window {
            json["ordered_window"] = json!({
                "ranking": w.ranking.save(),
                "rows": w.rows,
                "exhaustive": w.exhaustive,
            });
        }
        if let Some(w) = &self.window {
            json["window"] = json!({
                "seq": w.seq,
                "rows": w.rows,
                "exhaustive": w.exhaustive,
            });
        }
        json
    }

    /// Back to a saved state, its versions read by the plan's version type.
    pub fn load(&mut self, plan: &KeyedPlan, json: &Json) -> Result<(), String> {
        let saved = json["versions"]
            .as_object()
            .ok_or("a keyed checkpoint without versions")?;
        self.versions = saved
            .iter()
            .map(|(id, v)| {
                let ver = match &plan.ver {
                    Some(c) => decode(Some(v), &c.ty).map_err(|e| e.to_string())?,
                    None => Value::Null,
                };
                Ok((id.clone(), ver))
            })
            .collect::<Result<_, String>>()?;
        if let Some(r) = &mut self.ranking {
            r.load(&json["ranking"])?;
        }
        if let Some(w) = &mut self.ordered_window {
            let bad = |what: &str| format!("checkpoint: bad ordered keyed window ({what})");
            let saved = &json["ordered_window"];
            w.ranking.load(&saved["ranking"])?;
            w.rows = saved["rows"]
                .as_object()
                .ok_or_else(|| bad("rows"))?
                .iter()
                .map(|(id, r)| {
                    Ok((
                        id.clone(),
                        r.as_object().ok_or_else(|| bad("rows"))?.clone(),
                    ))
                })
                .collect::<Result<_, String>>()?;
            w.exhaustive = saved["exhaustive"]
                .as_bool()
                .ok_or_else(|| bad("exhaustive"))?;
        }
        if let Some(w) = &mut self.window {
            let bad = |what: &str| format!("checkpoint: bad keyed window ({what})");
            let saved = &json["window"];
            let ids = |name: &str| -> Result<VecDeque<String>, String> {
                saved[name]
                    .as_array()
                    .ok_or_else(|| bad(name))?
                    .iter()
                    .map(|id| id.as_str().map(String::from).ok_or_else(|| bad(name)))
                    .collect()
            };
            w.seq = match saved.get("seq") {
                Some(_) => ids("seq")?,
                // before OFFSET: the visible rows, then the buffer
                None => {
                    let mut seq = ids("visible")?;
                    seq.extend(ids("buffered")?);
                    seq
                }
            };
            w.rows = saved["rows"]
                .as_object()
                .ok_or_else(|| bad("rows"))?
                .iter()
                .map(|(id, r)| {
                    Ok((
                        id.clone(),
                        r.as_object().ok_or_else(|| bad("rows"))?.clone(),
                    ))
                })
                .collect::<Result<_, String>>()?;
            w.exhaustive = saved["exhaustive"]
                .as_bool()
                .ok_or_else(|| bad("exhaustive"))?;
        }
        Ok(())
    }
}

/// The changes that turn the rows `old` into `new` (both ids in order, with rows): deletes of
/// the ids that are gone, then, in `new`'s order, a move (an update with no columns) for a
/// misplaced id and an insert for a new one.
pub fn reorder(old: &[(String, Row)], new: &[(String, Row)]) -> Vec<Diff> {
    use std::collections::HashSet;
    let keep: HashSet<&String> = new.iter().map(|(id, _)| id).collect();
    let mut list: Vec<&String> = Vec::with_capacity(old.len());
    let mut diffs = Vec::new();
    for (id, _) in old {
        if keep.contains(id) {
            list.push(id);
        } else {
            diffs.push(Diff::DeleteId { id: id.clone() });
        }
    }
    for (j, (id, row)) in new.iter().enumerate() {
        if list.get(j) == Some(&id) {
            continue;
        }
        match list.iter().position(|x| *x == id) {
            Some(i) => {
                diffs.push(Diff::Update {
                    id: id.clone(),
                    set: Row::new(),
                    index: Some(j),
                    edit: Vec::new(),
                });
                let x = list.remove(i);
                list.insert(j, x);
            }
            None => {
                diffs.push(Diff::Insert {
                    row: row.clone(),
                    index: Some(j),
                    id: Some(id.clone()),
                });
                list.insert(j, id);
            }
        }
    }
    diffs
}

/// Whether `ver` is lower than the row's `old` (never without a version column).
fn older(ver: &Value, old: &Value) -> bool {
    match (ver, old) {
        (Value::Null, _) | (_, Value::Null) => false,
        _ => ver.compare(old) == Some(Ordering::Less),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(v: i64) -> Row {
        json!({ "v": v }).as_object().unwrap().clone()
    }

    fn lines(diffs: impl IntoIterator<Item = Vec<Diff>>) -> Vec<String> {
        diffs.into_iter().flatten().map(|d| d.to_string()).collect()
    }

    /// A new version from an event at (0, 0).
    fn ev(
        s: &mut KeyedState,
        id: &str,
        ver: Value,
        key: SortKey,
        deleted: bool,
        matches: bool,
        row: Row,
    ) -> Vec<Diff> {
        let v = NewVersion {
            id: id.to_string(),
            ver,
            key,
            deleted,
            matches,
            at: (0, 0),
        };
        s.event(v, || row)
    }

    const K: &str = "[\"k\"]";

    #[test]
    fn new_versions_insert_update_and_delete_their_row() {
        let mut s = KeyedState::default();
        let v = |n| Value::Int(n);
        let out = lines([
            // not in the result: matching inserts, the rest does nothing
            ev(&mut s, K, v(1), vec![], false, false, row(1)),
            ev(&mut s, K, v(2), vec![], true, true, row(2)),
            ev(&mut s, K, v(3), vec![], false, true, row(3)),
            // in the result: a matching version updates, a tie too (the later wins)
            ev(&mut s, K, v(4), vec![], false, true, row(4)),
            ev(&mut s, K, v(4), vec![], false, true, row(5)),
            // an older one is ignored
            ev(&mut s, K, v(2), vec![], true, true, row(6)),
            // no longer matching: deleted; a deleted version likewise
            ev(&mut s, K, v(5), vec![], false, false, row(7)),
            ev(&mut s, K, v(6), vec![], false, true, row(8)),
            ev(&mut s, K, v(7), vec![], true, true, row(9)),
            // gone: a deleted key's later versions come back
            ev(&mut s, K, v(1), vec![], false, true, row(10)),
        ]);
        assert_eq!(
            out,
            [
                "+ [\"k\"] {\"v\":3}",
                "~ [\"k\"] {\"v\":4}",
                "~ [\"k\"] {\"v\":5}",
                "- [\"k\"]",
                "+ [\"k\"] {\"v\":8}",
                "- [\"k\"]",
                "+ [\"k\"] {\"v\":10}",
            ]
        );
        assert_eq!(s.stale, 1);
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn without_a_version_column_the_later_event_wins() {
        let mut s = KeyedState::default();
        let out = lines([
            ev(&mut s, K, Value::Null, vec![], false, true, row(1)),
            ev(&mut s, K, Value::Null, vec![], false, true, row(2)),
            ev(&mut s, K, Value::Null, vec![], true, true, row(3)),
        ]);
        assert_eq!(
            out,
            ["+ [\"k\"] {\"v\":1}", "~ [\"k\"] {\"v\":2}", "- [\"k\"]"]
        );
        assert!(s.is_empty());
    }

    #[test]
    fn history_rows_and_checkpoints() {
        use crate::binder::ColumnId;
        use crate::plan::KeyedColumn;
        use crate::types::Type;
        let mut s = KeyedState::default();
        assert_eq!(
            s.history(vec![(K.into(), Value::Int(3), vec![], row(1))])[0].to_string(),
            "+ [\"k\"] {\"v\":1}"
        );
        let plan = KeyedPlan {
            key: Vec::new(),
            ver: Some(KeyedColumn {
                id: ColumnId(0),
                ty: Type::parse("UInt64"),
                history_field: "__ver".into(),
            }),
            deleted: None,
            text: "k".into(),
            window: None,
        };
        let saved = serde_json::from_str(&s.save().to_string()).unwrap();
        let mut restored = KeyedState::default();
        restored.load(&plan, &saved).unwrap();
        // the version came back: an older event is still ignored
        assert!(ev(&mut restored, K, Value::Int(2), vec![], false, true, row(2)).is_empty());
        assert_eq!(restored.stale, 1);
        assert!(restored.load(&plan, &json!({})).is_err());
    }

    #[test]
    fn ordered_rows_are_inserted_moved_and_deleted_at_their_places() {
        use crate::binder::{ColumnId, OrderKey};
        let order = KeyOrder(vec![OrderKey {
            column: ColumnId(0),
            desc: false,
            nulls_first: false,
        }]);
        let mut s = KeyedState::new(Some(order.clone()));
        let k = |n: i128| vec![Value::Int(n)];
        let id = |x: &str| format!("[\"{x}\"]");
        // history in ClickHouse's order (ties as it chose): inserts in result order
        let history: Vec<String> = s
            .history(vec![
                (id("a"), Value::Int(1), k(2), row(2)),
                (id("b"), Value::Int(1), k(1), row(1)),
                (id("c"), Value::Int(1), k(2), row(2)),
            ])
            .iter()
            .map(|d| d.to_string())
            .collect();
        assert_eq!(
            history,
            [
                "+ [\"b\"] {\"v\":1}",
                "+ [\"a\"] {\"v\":2}",
                "+ [\"c\"] {\"v\":2}"
            ]
        );
        let mut e = |x: &str, ver: i128, key: i128, deleted: bool| {
            ev(
                &mut s,
                &id(x),
                Value::Int(ver),
                k(key),
                deleted,
                true,
                row(key as i64),
            )
        };
        let out = lines([
            // new, tying with a and c: after them
            e("d", 1, 2, false),
            // a new version with the same sort key: no move
            e("a", 2, 2, false),
            // a new sort key: b moves after the 2s
            e("b", 2, 2, false),
            // and to the top
            e("c", 2, 0, false),
            // deleted
            e("a", 3, 2, true),
        ]);
        assert_eq!(
            out,
            [
                "+ @3 [\"d\"] {\"v\":2}",
                "~ [\"a\"] {\"v\":2}",
                "~ [\"b\"] @3 {\"v\":2}",
                "~ [\"c\"] @0 {\"v\":0}",
                "- [\"a\"]",
            ]
        );
        // a checkpoint keeps the order within ties: c, d, b
        let mut back = KeyedState::new(Some(order));
        let plan = KeyedPlan {
            key: Vec::new(),
            ver: None,
            deleted: None,
            text: "k".into(),
            window: None,
        };
        back.load(&plan, &serde_json::from_str(&s.save().to_string()).unwrap())
            .unwrap();
        let ids = |s: &KeyedState| -> Vec<String> {
            s.ranking
                .as_ref()
                .unwrap()
                .iter()
                .map(|e| e.id.clone())
                .collect()
        };
        assert_eq!(ids(&back), [id("c"), id("d"), id("b")]);
        assert_eq!(ids(&back), ids(&s));
    }

    fn window(limit: u64, buffer: u64) -> KeyedState {
        KeyedState::with_window(
            KeyedWindowPlan {
                limit: Some(limit),
                offset: 0,
                buffer,
                ordered: false,
            },
            None,
        )
    }

    fn history(s: &mut KeyedState, ids: &[&str]) -> Vec<String> {
        let rows = ids
            .iter()
            .map(|x| (x.to_string(), Value::Int(1), vec![], row(1)))
            .collect();
        s.history(rows).iter().map(|d| d.to_string()).collect()
    }

    /// A version of `id` at offset `o` of partition 0.
    fn at(s: &mut KeyedState, id: &str, o: i64, shown: bool) -> Vec<Diff> {
        let v = NewVersion {
            id: id.to_string(),
            ver: Value::Int(o as i128),
            key: vec![],
            deleted: !shown,
            matches: true,
            at: (0, o),
        };
        s.event(v, || row(o))
    }

    /// The visible rows, and the spare ones after them.
    fn kept(s: &KeyedState) -> (Vec<String>, Vec<String>) {
        let w = s.window.as_ref().unwrap();
        let front = w.front().min(w.seq.len());
        (
            w.seq.iter().take(front).skip(w.offset).cloned().collect(),
            w.seq.iter().skip(front).cloned().collect(),
        )
    }

    fn wm(o: i64) -> Watermark {
        Watermark([(0, o)].into())
    }

    #[test]
    fn a_window_keeps_the_newest_rows_and_replaces_departures_from_its_buffer() {
        let mut s = window(2, 4);
        // the first 2 are the result, the rest the buffer; fewer than 2 + 4: every row
        assert_eq!(
            history(&mut s, &["a", "b", "c", "d"]),
            ["+ a {\"v\":1}", "+ b {\"v\":1}"]
        );
        assert!(s.window.as_ref().unwrap().exhaustive);
        let out = lines([
            // new keys come in at the top; the last visible row slides into the buffer
            at(&mut s, "e", 1, true),
            at(&mut s, "f", 2, true),
            // the buffer is over capacity: its last row (d) is let go
            at(&mut s, "g", 3, true),
            // a visible row's new version: updated; a buffered one's: kept, not sent
            at(&mut s, "g", 4, true),
            at(&mut s, "a", 5, true),
            // a visible row leaves: the buffer's first comes in at the end
            at(&mut s, "f", 6, false),
            // a buffered one leaves: nothing to send
            at(&mut s, "b", 7, false),
            // not kept, not shown: nothing
            at(&mut s, "x", 8, false),
        ]);
        assert_eq!(
            out,
            [
                "+ @0 e {\"v\":1}",
                "- @2",
                "+ @0 f {\"v\":2}",
                "- @2",
                "+ @0 g {\"v\":3}",
                "- @2",
                "~ g {\"v\":4}",
                "- f",
                "+ @1 e {\"v\":1}",
            ]
        );
        assert_eq!(
            kept(&s),
            (vec!["g".into(), "e".into()], vec!["a".into(), "c".into()])
        );
        assert!(!s.window.as_ref().unwrap().exhaustive, "d was let go");
        // the promoted row is the one kept, as last seen
        assert_eq!(s.window.as_ref().unwrap().rows["a"], row(5));
        // a checkpoint keeps both lists and the rows
        let mut back = window(2, 4);
        let plan = KeyedPlan {
            key: Vec::new(),
            ver: None,
            deleted: None,
            text: "k".into(),
            window: None,
        };
        back.load(&plan, &serde_json::from_str(&s.save().to_string()).unwrap())
            .unwrap();
        assert_eq!(kept(&back), kept(&s));
        assert_eq!(
            back.window.as_ref().unwrap().rows,
            s.window.as_ref().unwrap().rows
        );
        // LIMIT 0: never anything
        let mut none = window(0, 4);
        assert!(history(&mut none, &[]).is_empty());
        assert!(at(&mut none, "a", 1, true).is_empty());
    }

    #[test]
    fn departures_drain_the_buffer_and_a_refill_brings_rows_that_still_match() {
        let refill = |ids: &[&str], at: i64, complete: bool| Refill {
            rows: ids
                .iter()
                .map(|x| (x.to_string(), Value::Int(at as i128), row(at)))
                .collect(),
            at: wm(at),
            complete,
        };
        let mut s = window(2, 4);
        // 2 + 4 rows: maybe more past them
        history(&mut s, &["a", "b", "c", "d", "e", "f"]);
        assert!(!s.wants_refill(), "a full buffer");
        for (o, x) in [(10, "c"), (11, "d"), (12, "e"), (13, "f")] {
            assert!(at(&mut s, x, o, false).is_empty());
        }
        // a visible row leaves with an empty buffer: one short, and a refill is due
        assert_eq!(lines([at(&mut s, "b", 14, false)]), ["- b"]);
        assert!(s.wants_refill());
        assert!(!s.wants_refill(), "asked once");
        // z leaves after the snapshot (16 > 15): it isn't brought back; c left before it, but
        // the snapshot wouldn't have it anyway; a is kept already
        assert!(at(&mut s, "z", 16, false).is_empty());
        let diffs = s.refill(refill(&["a", "z", "y", "w"], 15, true), &wm(16));
        assert_eq!(lines([diffs]), ["+ @1 y {\"v\":15}"]);
        assert_eq!(kept(&s), (vec!["a".into(), "y".into()], vec!["w".into()]));
        assert!(
            s.window.as_ref().unwrap().exhaustive,
            "the snapshot had every row"
        );
        assert!(!s.wants_refill(), "nothing past them");

        // a snapshot past the stream: it waits until the stream gets there
        let mut s = window(1, 4);
        history(&mut s, &["a", "b", "c", "d", "e"]);
        for (o, x) in [(10, "b"), (11, "c"), (12, "d"), (13, "e")] {
            at(&mut s, x, o, false);
        }
        assert!(s.wants_refill());
        assert!(s.refill(refill(&["v"], 30, false), &wm(20)).is_empty());
        assert!(s.advanced(&wm(25)).is_empty());
        assert!(
            s.advanced(&wm(30)).is_empty(),
            "into the buffer, the result is full"
        );
        assert_eq!(kept(&s).1, ["v"]);

        // restored at 40: departures before it aren't known, so a snapshot at 35 can't be used
        let mut s = window(1, 4);
        history(&mut s, &["a", "b", "c", "d", "e"]);
        s.restored_at(&wm(40));
        for (o, x) in [(41, "b"), (42, "c"), (43, "d"), (44, "e")] {
            at(&mut s, x, o, false);
        }
        assert!(s.wants_refill());
        assert!(s.refill(refill(&["v"], 35, true), &wm(44)).is_empty());
        assert!(kept(&s).1.is_empty());
        assert!(!s.wants_refill(), "not right away");
    }

    /// A row let go (still matching) after a refill's snapshot isn't in it: the refill can't
    /// say it has every row, however complete the snapshot was.
    #[test]
    fn a_refill_isnt_complete_when_rows_were_let_go_after_its_snapshot() {
        let mut s = window(1, 1);
        history(&mut s, &["a", "b"]);
        for (x, o) in [("c", 5), ("d", 6), ("e", 7)] {
            at(&mut s, x, o, true);
        }
        // c (last seen at 5) was let go when e came in
        assert_eq!(kept(&s), (vec!["e".into()], vec!["d".into()]));
        at(&mut s, "e", 8, false);
        at(&mut s, "d", 9, false);
        assert!(kept(&s).0.is_empty() && s.wants_refill());
        let empty = |at: i64| Refill {
            rows: Vec::new(),
            at: wm(at),
            complete: true,
        };
        s.refill(empty(4), &wm(9));
        assert!(
            !s.window.as_ref().unwrap().exhaustive,
            "c came after the snapshot"
        );
        assert_eq!(s.last_refill(), Some(wm(4)));
        // one past it can: nothing matches now (c, d, e and the history's rows left or were
        // let go before it)
        let mut s2 = s.clone();
        s2.window.as_mut().unwrap().requested = false;
        s2.refill(empty(6), &wm(9));
        assert!(s2.window.as_ref().unwrap().exhaustive);
    }

    fn top(limit: u64, buffer: u64) -> KeyedState {
        use crate::binder::{ColumnId, OrderKey};
        let order = KeyOrder(vec![OrderKey {
            column: ColumnId(0),
            desc: false,
            nulls_first: false,
        }]);
        KeyedState::with_window(
            KeyedWindowPlan {
                limit: Some(limit),
                offset: 0,
                buffer,
                ordered: true,
            },
            Some(order),
        )
    }

    /// A version of `id` with sort value `v` (its row `{"v": v}`).
    fn to(s: &mut KeyedState, id: &str, v: i128, shown: bool) -> Vec<String> {
        lines([ev(
            s,
            id,
            Value::Int(1),
            vec![Value::Int(v)],
            !shown,
            true,
            row(v as i64),
        )])
    }

    fn ranked(s: &KeyedState) -> Vec<String> {
        let w = s.ordered_window.as_ref().unwrap();
        w.ranking.iter().map(|e| e.id.clone()).collect()
    }

    /// Ranks 1 to 10: 5 visible, 5 buffered.
    fn ten() -> KeyedState {
        let mut s = top(5, 5);
        let rows = (1..=10)
            .map(|i| {
                (
                    format!("r{i}"),
                    Value::Int(1),
                    vec![Value::Int(i)],
                    row(i as i64),
                )
            })
            .collect();
        assert_eq!(s.history(rows).len(), 5);
        s
    }

    #[test]
    fn the_top_n_and_the_next_ranks_follow_new_sort_values() {
        // a key that wasn't kept (it was 20), now 5: after the 5 it ties with, the buffer's
        // head; nothing visible changes, and 10 is let go
        let mut s = ten();
        assert!(to(&mut s, "x", 5, true).is_empty());
        assert_eq!(
            ranked(&s),
            ["r1", "r2", "r3", "r4", "r5", "x", "r6", "r7", "r8", "r9"]
        );
        // now 4: into the top 5, whose last one (5) goes to the buffer
        let mut s = ten();
        assert_eq!(to(&mut s, "x", 4, true), ["+ @4 x {\"v\":4}", "- @5"]);
        // a kept row past the bound (3 → 20): it leaves, and the buffer's head comes in
        assert_eq!(to(&mut s, "r3", 20, true), ["- r3", "+ @4 r5 {\"v\":5}"]);
        // a key that wasn't kept, past the bound: nothing
        assert!(to(&mut s, "y", 15, true).is_empty());
        assert_eq!(
            ranked(&s),
            ["r1", "r2", "r4", "x", "r5", "r6", "r7", "r8", "r9"]
        );
        // moves: within the top (1 → 4: after r4 and x, which are 4 too), out of it, back in
        assert_eq!(to(&mut s, "r1", 4, true), ["~ r1 @3 {\"v\":4}"]);
        assert_eq!(to(&mut s, "r2", 7, true), ["- r2", "+ @4 r6 {\"v\":6}"]);
        assert_eq!(to(&mut s, "r8", 0, true), ["+ @0 r8 {\"v\":0}", "- @5"]);
        // the same sort value: an update in place if visible, nothing if buffered
        assert_eq!(to(&mut s, "r8", 0, true), ["~ r8 {\"v\":0}"]);
        assert!(to(&mut s, "r9", 9, true).is_empty());
        // a departure from the top: the next one comes in at its end
        assert_eq!(to(&mut s, "r8", 0, false), ["- r8", "+ @4 r6 {\"v\":6}"]);
        // a checkpoint keeps the ranks and rows
        let mut back = top(5, 5);
        let plan = KeyedPlan {
            key: Vec::new(),
            ver: None,
            deleted: None,
            text: "k".into(),
            window: None,
        };
        back.load(&plan, &serde_json::from_str(&s.save().to_string()).unwrap())
            .unwrap();
        assert_eq!(ranked(&back), ranked(&s));
        assert_eq!(back.visible_rows(), s.visible_rows());
    }

    #[test]
    fn with_every_row_kept_there_is_no_bound() {
        let mut s = top(2, 5);
        let rows = (1..=3)
            .map(|i| {
                (
                    format!("r{i}"),
                    Value::Int(1),
                    vec![Value::Int(i)],
                    row(i as i64),
                )
            })
            .collect();
        s.history(rows);
        assert!(s.ordered_window.as_ref().unwrap().exhaustive);
        // far past the last row: still kept, since nothing is past it
        assert!(to(&mut s, "x", 100, true).is_empty());
        assert_eq!(ranked(&s), ["r1", "r2", "r3", "x"]);
        assert!(!s.wants_refill());
        // LIMIT 0: nothing
        let mut none = top(0, 5);
        none.history(Vec::new());
        assert!(to(&mut none, "x", 1, true).is_empty());
    }

    /// Applying `reorder`'s changes to the old rows gives the new ones, whatever they are.
    #[test]
    fn reorder_turns_the_old_rows_into_the_new() {
        use crate::diff::ResultList;
        let mut seed: u64 = 5;
        let mut rand = |n: u64| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 33) % n
        };
        for _ in 0..300 {
            let (n_old, n_new) = (rand(8), rand(8));
            let mut pick = |len: u64| -> Vec<(String, Row)> {
                let mut ids: Vec<u64> = (0..12).collect();
                let mut out = Vec::new();
                for _ in 0..len {
                    let i = rand(ids.len() as u64) as usize;
                    let id = ids.remove(i);
                    out.push((format!("k{id}"), row(id as i64)));
                }
                out
            };
            let (old, new) = (pick(n_old), pick(n_new));
            let mut list = ResultList::default();
            for (id, r) in &old {
                list.push(r.clone(), Some(id.clone()));
            }
            for d in reorder(&old, &new) {
                list.apply(&d).unwrap();
            }
            let got: Vec<&str> = list.ids.iter().map(|i| i.as_deref().unwrap()).collect();
            let want: Vec<&str> = new.iter().map(|(id, _)| id.as_str()).collect();
            assert_eq!(got, want);
            assert_eq!(
                list.rows,
                new.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>()
            );
        }
    }

    /// A rebuild's shadow lets nothing go while it replays, so rows it needs later aren't
    /// lost; settling cuts it to its ranks.
    #[test]
    fn a_shadow_keeps_everything_until_it_settles() {
        let mut s = ten();
        let mut shadow = s.fresh();
        let rows = (1..=10)
            .map(|i| {
                (
                    format!("r{i}"),
                    Value::Int(1),
                    vec![Value::Int(i)],
                    row(i as i64),
                )
            })
            .collect();
        shadow.history(rows);
        for (x, v) in [("x", 0), ("y", 0), ("z", 0)] {
            to(&mut shadow, x, v, true);
        }
        assert_eq!(ranked(&shadow).len(), 13, "nothing let go");
        // departures it can now fill from what it kept
        for i in 1..=5 {
            to(&mut shadow, &format!("r{i}"), 0, false);
        }
        shadow.settle();
        assert_eq!(
            ranked(&shadow),
            ["x", "y", "z", "r6", "r7", "r8", "r9", "r10"]
        );
        // the live state (which let r8–r10 go) takes the shadow's top over
        for (x, v) in [("x", 0), ("y", 0), ("z", 0)] {
            to(&mut s, x, v, true);
        }
        for i in 1..=5 {
            to(&mut s, &format!("r{i}"), 0, false);
        }
        assert_eq!(ranked(&s), ["x", "y", "z", "r6", "r7"]);
        assert_eq!(
            s.last_refill(),
            None,
            "rows were let go: the same snapshot will do"
        );
        let diffs = s.rebuilt(shadow, wm(0));
        assert!(diffs.is_empty(), "the same top 5: {diffs:?}");
        assert_eq!(ranked(&s).len(), 8);
        assert_eq!(s.last_refill(), Some(wm(0)));
    }

    /// Whatever goes in and out of a sequence, anywhere, the client's rows (only ever changed by
    /// the edge rules' diffs) are its positions `[m, m+n)`.
    #[test]
    fn the_edge_rules_keep_the_client_on_the_window() {
        use crate::diff::ResultList;
        let mut seed: u64 = 9;
        let mut rand = |n: u64| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 33) % n
        };
        for _ in 0..200 {
            let (m, n) = (rand(4) as usize, [0, 1, 2, 3, usize::MAX][rand(5) as usize]);
            let mut seq: Vec<String> = Vec::new();
            let mut client = ResultList::default();
            let mut next = 0;
            for _ in 0..60 {
                let row_of = |id: &str| -> Row { json!({ "id": id }).as_object().unwrap().clone() };
                let diffs = if seq.is_empty() || rand(3) > 0 {
                    next += 1;
                    let p = rand(seq.len() as u64 + 1) as usize;
                    seq.insert(p, format!("r{next}"));
                    let seq = &seq;
                    inserted(p, seq.len(), (m, n), |k| (seq[k].clone(), row_of(&seq[k])))
                } else {
                    let o = rand(seq.len() as u64) as usize;
                    let id = seq.remove(o);
                    let seq = &seq;
                    removed(o, id, seq.len(), (m, n), |k| {
                        (seq[k].clone(), row_of(&seq[k]))
                    })
                };
                for d in &diffs {
                    client.apply(d).unwrap();
                }
                let want: Vec<&String> = seq.iter().skip(m).take(n).collect();
                let got: Vec<&String> = client.ids.iter().map(|i| i.as_ref().unwrap()).collect();
                assert_eq!(got, want, "m {m}, n {n}, after {diffs:?}");
            }
        }
    }

    fn plan(limit: Option<u64>, offset: u64, buffer: u64, ordered: bool) -> KeyedWindowPlan {
        KeyedWindowPlan {
            limit,
            offset,
            buffer,
            ordered,
        }
    }

    #[test]
    fn a_window_with_an_offset_holds_the_newest_back() {
        // OFFSET 1 LIMIT 2, 3 spare: a held back, b c shown, d e f spare
        let mut s = KeyedState::with_window(plan(Some(2), 1, 3, false), None);
        assert_eq!(
            history(&mut s, &["a", "b", "c", "d", "e", "f"]),
            ["+ b {\"v\":1}", "+ c {\"v\":1}"]
        );
        let out = lines([
            // a new key, held back: the row it pushes out of the held-back part (a) comes in
            // at the top, and the last one goes; f is let go
            at(&mut s, "x", 1, true),
            // a held-back row leaves: the first shown (a) is held back now, the next comes in
            // at the end
            at(&mut s, "x", 2, false),
            // a shown row leaves; a spare one, and the held-back one's update: nothing
            at(&mut s, "b", 3, false),
            at(&mut s, "e", 4, false),
            at(&mut s, "a", 5, true),
            // a shown one's update
            at(&mut s, "c", 6, true),
        ]);
        assert_eq!(
            out,
            [
                "+ @0 a {\"v\":1}",
                "- @2",
                "- @0",
                "+ @1 c {\"v\":1}",
                "- b",
                "+ @1 d {\"v\":1}",
                "~ c {\"v\":6}",
            ]
        );
        assert_eq!(kept(&s), (vec!["c".into(), "d".into()], vec![]));

        // OFFSET 2, no LIMIT: every row but the newest two, all kept, no refills
        let mut s = KeyedState::with_window(plan(None, 2, 3, false), None);
        assert_eq!(
            history(&mut s, &["a", "b", "c", "d"]),
            ["+ c {\"v\":1}", "+ d {\"v\":1}"]
        );
        assert_eq!(lines([at(&mut s, "x", 1, true)]), ["+ @0 b {\"v\":1}"]);
        assert!(s.window.as_ref().unwrap().exhaustive && !s.wants_refill());
    }

    #[test]
    fn an_ordered_window_with_an_offset_shows_the_ranks_after_it() {
        use crate::binder::{ColumnId, OrderKey};
        let order = KeyOrder(vec![OrderKey {
            column: ColumnId(0),
            desc: false,
            nulls_first: false,
        }]);
        let rows = |n: i128| -> Vec<(String, Value, SortKey, Row)> {
            (1..=n)
                .map(|i| {
                    (
                        format!("r{i}"),
                        Value::Int(1),
                        vec![Value::Int(i)],
                        row(i as i64),
                    )
                })
                .collect()
        };
        // OFFSET 1 LIMIT 2, 3 more: ranks 1 and 2 shown
        let mut s = KeyedState::with_window(plan(Some(2), 1, 3, true), Some(order.clone()));
        assert_eq!(s.history(rows(6)).len(), 2);
        // a new first rank: r1 slides from the held-back part into the top
        assert_eq!(to(&mut s, "x", 0, true), ["+ @0 r1 {\"v\":1}", "- @2"]);
        // from the spare ranks into the window (r4 → 0, after x)
        assert_eq!(to(&mut s, "r4", 0, true), ["+ @0 r4 {\"v\":0}", "- @2"]);
        // within the window, to the same place: an update
        assert_eq!(to(&mut s, "r1", 0, true), ["~ r1 {\"v\":0}"]);
        // the held-back rank leaves: r4 is held back now, r2 comes in at the end
        assert_eq!(to(&mut s, "x", 0, false), ["- @0", "+ @1 r2 {\"v\":2}"]);
        assert_eq!(
            s.visible_rows()
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
            ["r1", "r2"]
        );

        // OFFSET 2, no LIMIT: all ranks from the third on, no bound
        let mut s = KeyedState::with_window(plan(None, 2, 3, true), Some(order));
        assert_eq!(s.history(rows(4)).len(), 2);
        assert_eq!(to(&mut s, "x", 100, true), ["+ @2 x {\"v\":100}"]);
        assert!(!s.wants_refill());
    }
}
