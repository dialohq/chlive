//! Subscription lifecycle: dedupe by `(sql, params)`, snapshot, debounced merges, refcounting.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::{broadcast, mpsc, Notify, OnceCell};
use tracing::{info, info_span, warn, Instrument};

use crate::cache_engine::CacheEngine;
use crate::ch_http::{ChHttp, Column, Params};
use crate::diff::{self, KeyedRows};
use crate::rewriter::{self, params, Catalog, Plan, PlanKind, RewriteError};
use crate::tail::{self, TableAdvanced, TableRuntime};
use crate::watermark::Watermark;
use crate::ws::ServerMsg;

pub type ClientTx = mpsc::UnboundedSender<ServerMsg>;
pub type ConnId = u64;

/// Error sent to a client for a failed subscription.
#[derive(Debug, Clone, Serialize)]
pub struct SubError {
    pub code: &'static str,
    pub message: String,
}

impl From<RewriteError> for SubError {
    fn from(e: RewriteError) -> Self {
        Self {
            code: e.code,
            message: e.message,
        }
    }
}

impl SubError {
    fn query(e: &anyhow::Error) -> Self {
        Self {
            code: "QUERY_FAILED",
            message: format!("{e:#}"),
        }
    }
}

struct State {
    rows: KeyedRows,
    columns: Vec<Column>,
    /// Attached clients: (connection, client-chosen subscription id) → sender.
    clients: HashMap<(ConnId, String), ClientTx>,
}

#[derive(Debug, Default, Clone, Serialize)]
struct Stats {
    merges: u64,
    patches: u64,
    last_merge_ms: f64,
    snapshot_ms: f64,
    last_error: Option<String>,
}

pub struct Sub {
    pub id: u64,
    hash: String,
    sql: String,
    params: Params,
    plan: Plan,
    rt: Arc<TableRuntime>,
    hist_table: String,
    watermark: OnceCell<Watermark>,
    init: OnceCell<Result<(), SubError>>,
    state: tokio::sync::Mutex<State>,
    dirty: Notify,
    stats: Mutex<Stats>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct Entry {
    sub: Arc<Sub>,
    refs: usize,
}

pub struct Manager {
    catalog: Arc<Catalog>,
    tables: HashMap<String, Arc<TableRuntime>>,
    main: ChHttp,
    cache: Arc<dyn CacheEngine>,
    debounce: Duration,
    next_id: AtomicU64,
    subs: Mutex<HashMap<String, Entry>>,
}

impl Manager {
    pub fn new(
        catalog: Arc<Catalog>,
        tables: HashMap<String, Arc<TableRuntime>>,
        main: ChHttp,
        cache: Arc<dyn CacheEngine>,
        debounce: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            catalog,
            tables,
            main,
            cache,
            debounce,
            next_id: AtomicU64::new(1),
            subs: Mutex::new(HashMap::new()),
        })
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Mark subscriptions dirty whenever their table's tail advances.
    pub fn spawn_router(
        self: &Arc<Self>,
        mut rx: broadcast::Receiver<TableAdvanced>,
    ) -> tokio::task::JoinHandle<()> {
        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let table = match rx.recv().await {
                    Ok(ev) => Some(ev.table),
                    Err(broadcast::error::RecvError::Lagged(_)) => None, // notify everything
                    Err(broadcast::error::RecvError::Closed) => return,
                };
                let Some(this) = this.upgrade() else { return };
                let subs = this.subs.lock().expect("subs lock");
                for e in subs.values() {
                    if table.as_deref().is_none_or(|t| t == e.sub.plan.table) {
                        e.sub.dirty.notify_one();
                    }
                }
            }
        })
    }

    /// Attach a client to the subscription for `(sql, params)`, creating it if needed, and
    /// send it a snapshot. Returns the dedupe hash, used to unsubscribe.
    pub async fn subscribe(
        self: &Arc<Self>,
        conn: ConnId,
        client_id: String,
        sql: &str,
        values: &serde_json::Map<String, serde_json::Value>,
        tx: ClientTx,
    ) -> Result<String, SubError> {
        let plan = rewriter::rewrite(sql, &self.catalog)?;
        let bound = params::bind(&plan.param_types, values)?;
        let hash = dedupe_hash(&plan.normalized_sql, &bound);

        let (sub, refs) = {
            let mut subs = self.subs.lock().expect("subs lock");
            let entry = subs.entry(hash.clone()).or_insert_with(|| {
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                let rt = self.tables[&plan.table].clone();
                Entry {
                    sub: Arc::new(Sub {
                        id,
                        hash: hash.clone(),
                        sql: sql.to_string(),
                        params: bound.clone(),
                        plan,
                        rt,
                        hist_table: format!("sub_{id}_hist"),
                        watermark: OnceCell::new(),
                        init: OnceCell::new(),
                        state: tokio::sync::Mutex::new(State {
                            rows: KeyedRows::new(),
                            columns: Vec::new(),
                            clients: HashMap::new(),
                        }),
                        dirty: Notify::new(),
                        stats: Mutex::new(Stats::default()),
                        task: Mutex::new(None),
                    }),
                    refs: 0,
                }
            });
            entry.refs += 1;
            (entry.sub.clone(), entry.refs)
        };
        info!(sub = sub.id, refcount = refs, client = client_id, "attach");

        let init = sub
            .init
            .get_or_init(|| async {
                let r = self.create(&sub).await;
                if r.is_ok() {
                    self.spawn_merger(&sub);
                }
                r
            })
            .await
            .clone();
        if let Err(e) = init {
            self.release(&hash);
            return Err(e);
        }

        let mut st = sub.state.lock().await;
        let _ = tx.send(snapshot_msg(&client_id, &sub.plan, &st));
        st.clients.insert((conn, client_id), tx);
        Ok(hash)
    }

    pub async fn unsubscribe(&self, conn: ConnId, client_id: &str, hash: &str) {
        let sub = {
            let subs = self.subs.lock().expect("subs lock");
            subs.get(hash).map(|e| e.sub.clone())
        };
        if let Some(sub) = sub {
            sub.state
                .lock()
                .await
                .clients
                .remove(&(conn, client_id.to_string()));
            self.release(hash);
        }
    }

    fn release(&self, hash: &str) {
        let removed = {
            let mut subs = self.subs.lock().expect("subs lock");
            let Some(e) = subs.get_mut(hash) else { return };
            e.refs -= 1;
            info!(sub = e.sub.id, refcount = e.refs, "detach");
            if e.refs == 0 {
                subs.remove(hash).map(|e| e.sub)
            } else {
                None
            }
        };
        if let Some(sub) = removed {
            if let Some(t) = sub.task.lock().expect("task lock").take() {
                t.abort();
            }
            let cache = self.cache.clone();
            tokio::spawn(async move {
                // Waits for an in-flight merge (which holds the state lock) before dropping.
                let _guard = sub.state.lock().await;
                let sql = format!("DROP TABLE IF EXISTS {} SYNC", sub.hist_table);
                match cache.exec(&sql, &Params::new()).await {
                    Ok(()) => info!(sub = sub.id, "dropped"),
                    Err(e) => warn!(sub = sub.id, error = %e, "failed to drop history table"),
                }
            });
        }
    }

    /// Snapshot: take `W`, materialize history states from main, run the first merge.
    async fn create(&self, sub: &Arc<Sub>) -> Result<(), SubError> {
        let span = info_span!("snapshot", sub = sub.id, table = sub.plan.table);
        async {
            let started = Instant::now();
            let w = tail::fetch_watermark(&self.main, &sub.plan.table, sub.rt.partitions)
                .await
                .map_err(|e| SubError::query(&e))?;
            let history = sub.plan.history_sql(&w);
            let _ = sub.watermark.set(w);
            let ddl = format!(
                "CREATE TABLE {} ENGINE = Memory AS {history}",
                sub.hist_table
            );
            self.cache
                .exec(&ddl, &sub.params)
                .await
                .map_err(|e| SubError::query(&e))?;
            let hist_ms = started.elapsed().as_secs_f64() * 1e3;
            let mut st = sub.state.lock().await;
            self.merge_into(sub, &mut st).await.inspect_err(|_| {
                // don't leak the history table of a subscription that never became live
                let cache = self.cache.clone();
                let t = sub.hist_table.clone();
                tokio::spawn(async move {
                    let _ = cache
                        .exec(&format!("DROP TABLE IF EXISTS {t} SYNC"), &Params::new())
                        .await;
                });
            })?;
            let total = started.elapsed().as_secs_f64() * 1e3;
            sub.stats.lock().expect("stats").snapshot_ms = total;
            info!(
                sub = sub.id,
                w = ?sub.watermark.get().map(|w| &w.0),
                rows = st.rows.len(),
                history_ms = format!("{hist_ms:.1}"),
                total_ms = format!("{total:.1}"),
                "snapshot ready"
            );
            Ok(())
        }
        .instrument(span)
        .await
    }

    /// Run the merge query and replace the stored result. Returns the diff.
    async fn merge_into(&self, sub: &Sub, st: &mut State) -> Result<diff::Diff, SubError> {
        let started = Instant::now();
        let w = sub.watermark.get().expect("watermark set before merge");
        let sql = sub.plan.merge_sql(w, &sub.hist_table);
        let result = self.cache.query(&sql, &sub.params).await.map_err(|e| {
            sub.stats.lock().expect("stats").last_error = Some(format!("{e:#}"));
            SubError::query(&e)
        })?;
        if let Some(cap) = sub.plan.result_cap {
            if result.data.len() as u64 > cap {
                return Err(SubError {
                    code: "RESULT_TOO_LARGE",
                    message: format!(
                        "the result has more than {cap} rows; add a LIMIT or narrow the query"
                    ),
                });
            }
        }
        let rows = diff::key_rows(&sub.plan, result.data);
        let d = diff::diff(&st.rows, &rows);
        st.rows = rows;
        st.columns = sub
            .plan
            .output_columns
            .iter()
            .map(|c| {
                result
                    .meta
                    .iter()
                    .find(|m| &m.name == c)
                    .cloned()
                    .unwrap_or(Column {
                        name: c.clone(),
                        ty: "".into(),
                    })
            })
            .collect();
        let ms = started.elapsed().as_secs_f64() * 1e3;
        let mut stats = sub.stats.lock().expect("stats");
        stats.merges += 1;
        stats.last_merge_ms = ms;
        stats.last_error = None;
        tracing::debug!(
            sub = sub.id,
            ms = format!("{ms:.1}"),
            upserts = d.upserts.len(),
            deletes = d.deletes.len(),
            "merge"
        );
        Ok(d)
    }

    fn spawn_merger(self: &Arc<Self>, sub: &Arc<Sub>) {
        let this = Arc::downgrade(self);
        // The task holds the subscription; `release` aborts it when the refcount hits zero.
        let sub_ref = sub.clone();
        let debounce = self.debounce;
        let handle = tokio::spawn(async move {
            let sub = sub_ref;
            loop {
                sub.dirty.notified().await;
                tokio::time::sleep(debounce).await;
                let Some(this) = this.upgrade() else { return };
                // The state lock serializes merges for this subscription.
                let mut st = sub.state.lock().await;
                match this.merge_into(&sub, &mut st).await {
                    Ok(d) if !d.is_empty() => {
                        sub.stats.lock().expect("stats").patches += 1;
                        let order = sub.plan.has_order.then(|| diff::order(&st.rows));
                        for ((_, client_id), tx) in &st.clients {
                            let _ = tx.send(ServerMsg::Patch {
                                id: client_id.clone(),
                                upserts: d.upserts.clone(),
                                deletes: d.deletes.clone(),
                                order: order.clone(),
                            });
                        }
                    }
                    Ok(_) => {}
                    Err(e) => warn!(
                        sub = sub.id,
                        code = e.code,
                        error = e.message,
                        "merge failed"
                    ),
                }
            }
        });
        *sub.task.lock().expect("task lock") = Some(handle);
    }

    /// JSON for `/debug/subscriptions`.
    pub async fn debug(&self) -> serde_json::Value {
        let subs: Vec<(Arc<Sub>, usize)> = {
            let subs = self.subs.lock().expect("subs lock");
            subs.values().map(|e| (e.sub.clone(), e.refs)).collect()
        };
        let mut out = Vec::new();
        for (sub, refs) in subs {
            let rows = match sub.state.try_lock() {
                Ok(st) => Some(st.rows.len()),
                Err(_) => None,
            };
            out.push(serde_json::json!({
                "id": sub.id,
                "hash": sub.hash,
                "sql": sub.sql,
                "params": sub.params,
                "refcount": refs,
                "table": sub.plan.table,
                "kind": sub.plan.kind,
                "watermark": sub.watermark.get(),
                "rows": rows,
                "stats": sub.stats.lock().expect("stats").clone(),
            }));
        }
        out.sort_by_key(|v| v["id"].as_u64());
        let tables: serde_json::Map<String, serde_json::Value> = self
            .tables
            .iter()
            .map(|(name, rt)| {
                (
                    name.clone(),
                    serde_json::json!({
                        "partitions": rt.partitions,
                        "w0": rt.w0,
                        "tail_max": *rt.tail_max.read().expect("tail_max"),
                    }),
                )
            })
            .collect();
        serde_json::json!({ "subscriptions": out, "tables": tables })
    }
}

fn snapshot_msg(client_id: &str, plan: &Plan, st: &State) -> ServerMsg {
    let key_columns = match plan.kind {
        PlanKind::Rows => vec!["_partition".to_string(), "_offset".to_string()],
        PlanKind::Aggregate => plan.key_columns.clone(),
    };
    ServerMsg::Snapshot {
        id: client_id.to_string(),
        columns: st.columns.clone(),
        key_columns,
        rows: st.rows.values().cloned().collect(),
        order: plan.has_order.then(|| diff::order(&st.rows)),
    }
}

pub fn dedupe_hash(normalized_sql: &str, params: &Params) -> String {
    let mut h = Sha256::new();
    h.update(normalized_sql.as_bytes());
    h.update([0]);
    h.update(serde_json::to_vec(params).expect("serializable"));
    h.finalize()
        .iter()
        .take(12)
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupe_hash_depends_on_sql_and_params() {
        let p1 = Params::from([("a".into(), "1".into())]);
        let p2 = Params::from([("a".into(), "2".into())]);
        assert_eq!(dedupe_hash("SELECT 1", &p1), dedupe_hash("SELECT 1", &p1));
        assert_ne!(dedupe_hash("SELECT 1", &p1), dedupe_hash("SELECT 1", &p2));
        assert_ne!(dedupe_hash("SELECT 1", &p1), dedupe_hash("SELECT 2", &p1));
    }
}
