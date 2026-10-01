//! End-to-end correctness: live results (snapshot + patches, reconstructed client-side) must
//! equal the original query run directly on main ClickHouse once ingestion has settled.
//!
//! Run with `e2e -- --nocapture` in the dev shell, while `dnvr up` runs with the `stream`
//! process stopped.
//!
//! Tolerances (see NOTES.md): exact for count/sum of integers/min/max/argMin/argMax/uniqExact
//! and strings; 1e-9 relative for float sums/averages/ratios; 2% for `uniq`; 15% for
//! `quantile(s)` or 5 ms absolute (reservoir sampling: merged states and a single pass
//! sample differently).

use std::collections::BTreeMap;
use std::time::Duration;

use chlive_e2e::*;
use chlive_generator::TsMode;
use chlive_server::cache_engine::CacheEngine;
use chlive_server::ch_http::Params;
use chlive_server::rewriter::{self, Options};
use serde_json::{json, Value};

const FLOAT: Tol = Tol::Rel(1e-9);
const UNIQ: Tol = Tol::Rel(0.02);
const QUANTILE: Tol = Tol::Approx {
    rel: 0.15,
    abs: 5.0,
};

struct Case {
    id: &'static str,
    sql: &'static str,
    params: fn() -> Value,
    tol: fn(&str) -> Tol,
}

fn since() -> Value {
    let t = chrono_like_now_minus_hours(1);
    json!({ "since": t })
}

/// `YYYY-MM-DD hh:mm:ss.000` in UTC, one hour ago, without pulling in chrono.
fn chrono_like_now_minus_hours(h: u64) -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - h * 3600;
    let days = secs / 86_400;
    let rem = secs % 86_400;
    // civil-from-days (Howard Hinnant)
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.000",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            id: "latest",
            sql: "SELECT ts, user_id, country, path, latency_ms FROM events ORDER BY ts DESC LIMIT 20",
            params: || json!({}),
            tol: |_| Tol::Exact,
        },
        Case {
            // Rows plan without ORDER BY or LIMIT: every matching row, in Kafka order.
            id: "unordered_rows",
            sql: "SELECT ts, user_id, path, latency_ms FROM events WHERE latency_ms > 5000",
            params: || json!({}),
            tol: |_| Tol::Exact,
        },
        Case {
            id: "counters",
            sql: "SELECT count() AS total, avg(latency_ms) AS avg_latency, sum(amount) AS revenue FROM events",
            params: || json!({}),
            tol: |c| if c == "total" { Tol::Exact } else { FLOAT },
        },
        Case {
            id: "top_countries",
            sql: "SELECT country, count() AS n FROM events WHERE path = {path:String} GROUP BY country ORDER BY n DESC LIMIT 10",
            params: || json!({"path": "/checkout"}),
            tol: |_| Tol::Exact,
        },
        Case {
            id: "per_minute",
            sql: "SELECT toStartOfMinute(ts) AS minute, count() AS requests, quantile(0.95)(latency_ms) AS p95, \
                  quantiles(0.5, 0.99)(latency_ms) AS p50_p99 FROM events WHERE ts >= {since:DateTime64(3)} \
                  GROUP BY minute ORDER BY minute",
            params: since,
            tol: |c| match c {
                "p95" | "p50_p99" => QUANTILE,
                _ => Tol::Exact,
            },
        },
        Case {
            id: "orders",
            sql: "SELECT country, uniq(user_id) AS users, sum(amount) / count() AS avg_order_value, \
                  countIf(amount > 100) AS big_orders, big_orders / count() AS big_order_ratio FROM events \
                  WHERE amount > 0 GROUP BY country HAVING users > 50 ORDER BY users DESC LIMIT 5",
            params: || json!({}),
            tol: |c| match c {
                "users" => UNIQ,
                "avg_order_value" | "big_order_ratio" => FLOAT,
                _ => Tol::Exact,
            },
        },
        Case {
            id: "slowest",
            sql: "SELECT path, max(latency_ms) AS worst_ms, argMax(user_id, latency_ms) AS worst_user \
                  FROM events GROUP BY path ORDER BY worst_ms DESC LIMIT 10",
            params: || json!({}),
            tol: |_| Tol::Exact,
        },
        Case {
            id: "all_functions",
            // Every allowlisted aggregate and its -If form, to validate the State/Merge naming.
            // argMin/argMax use a (latency, user) tuple so ties resolve deterministically.
            sql: "SELECT country, count() AS c, sum(latency_ms) AS s, min(latency_ms) AS mn, max(latency_ms) AS mx, \
                  avg(latency_ms) AS a, any(country) AS an, uniq(user_id) AS u, uniqExact(user_id) AS ue, \
                  argMin(user_id, (latency_ms, user_id)) AS amin, argMax(user_id, (latency_ms, user_id)) AS amax, \
                  quantile(0.5)(latency_ms) AS q, quantiles(0.1, 0.9)(latency_ms) AS qs, \
                  countIf(amount > 100) AS c_if, sumIf(latency_ms, amount > 100) AS s_if, \
                  minIf(latency_ms, amount > 100) AS mn_if, maxIf(latency_ms, amount > 100) AS mx_if, \
                  avgIf(latency_ms, amount > 100) AS a_if, anyIf(country, amount > 100) AS an_if, \
                  uniqIf(user_id, amount > 100) AS u_if, uniqExactIf(user_id, amount > 100) AS ue_if, \
                  argMinIf(user_id, (latency_ms, user_id), amount > 100) AS amin_if, \
                  argMaxIf(user_id, (latency_ms, user_id), amount > 100) AS amax_if, \
                  quantileIf(0.5)(latency_ms, amount > 100) AS q_if, \
                  quantilesIf(0.1, 0.9)(latency_ms, amount > 100) AS qs_if \
                  FROM events GROUP BY country ORDER BY country",
            params: || json!({}),
            tol: |c| match c {
                "a" | "a_if" => FLOAT,
                "u" | "u_if" => UNIQ,
                "q" | "qs" | "q_if" | "qs_if" => QUANTILE,
                _ => Tol::Exact,
            },
        },
    ]
}

/// Rows plans break ORDER BY ties by Kafka position; the original query doesn't, so rows
/// with equal `ts` may legitimately swap, and at the LIMIT boundary a different member of
/// the last tie group may be chosen. Compare the `ts` sequence exactly, and full rows for
/// everything outside the last tie group (sorted within tie groups).
fn normalize_ties(id: &str, mut rows: Vec<Row>) -> Vec<Row> {
    // Without ORDER BY the direct query's order is arbitrary; compare as a set.
    if id == "unordered_rows" {
        rows.sort_by_cached_key(|r| Value::Object(r.clone()).to_string());
    }
    if id == "latest" {
        rows.sort_by(|a, b| {
            b["ts"].as_str().cmp(&a["ts"].as_str()).then_with(|| {
                Value::Object(a.clone())
                    .to_string()
                    .cmp(&Value::Object(b.clone()).to_string())
            })
        });
        if let Some(last_ts) = rows.last().map(|r| r["ts"].clone()) {
            for r in rows.iter_mut().filter(|r| r["ts"] == last_ts) {
                r.retain(|k, _| k == "ts");
            }
        }
    }
    rows
}

async fn check_all(
    conn: &LiveConn,
    main: &chlive_server::ch_http::ChHttp,
    cases: &[(Case, Value)],
) {
    for (case, params) in cases {
        let live = conn.result(case.id).expect("subscribed");
        assert!(live.error.is_none(), "{}: {:?}", case.id, live.error);
        let direct_rows = direct(main, case.sql, params).await.expect("direct query");
        assert!(
            !direct_rows.is_empty(),
            "{}: direct query returned nothing; test data missing?",
            case.id
        );
        assert_results_match(
            case.id,
            &normalize_ties(case.id, live.ordered_rows()),
            &normalize_ties(case.id, direct_rows),
            case.tol,
        );
        eprintln!(
            "{}: ok ({} rows, {} patches)",
            case.id,
            live.rows.len(),
            live.patches
        );
    }
}

fn merge_offsets(a: &mut BTreeMap<i32, i64>, b: BTreeMap<i32, i64>) {
    for (p, o) in b {
        let e = a.entry(p).or_insert(-1);
        *e = (*e).max(o);
    }
}

/// Subscribe after some history exists, keep producing, and compare once settled.
#[tokio::test(flavor = "multi_thread")]
async fn live_results_match_direct_queries() {
    if !enabled() {
        return;
    }
    let _guard = exclusive().await;
    init_tracing();
    let (server, cfg) = start_server().await.unwrap();
    let main = main_client(&cfg);
    let gen = generator(&cfg).unwrap();

    let mut produced = gen.produce(Some(3000), None, TsMode::Now).await.unwrap();
    let conn = LiveConn::connect(&server).await.unwrap();
    let cases: Vec<(Case, Value)> = cases()
        .into_iter()
        .map(|c| {
            let p = (c.params)();
            (c, p)
        })
        .collect();
    for (c, p) in &cases {
        conn.subscribe(c.id, c.sql, p.clone());
    }
    let ids: Vec<&str> = cases.iter().map(|(c, _)| c.id).collect();
    conn.wait_ready(&ids, Duration::from_secs(60))
        .await
        .unwrap();

    merge_offsets(
        &mut produced,
        gen.produce(Some(4000), Some(2000.0), TsMode::Now)
            .await
            .unwrap(),
    );
    settle(&server, &main, &conn, &produced).await.unwrap();
    check_all(&conn, &main, &cases).await;

    // The patch path was actually exercised.
    let counters = conn.result("counters").unwrap();
    assert!(counters.patches > 0, "expected patches for counters");

    // Patch test: the state reconstructed from snapshot + patches equals a fresh snapshot
    // of the latest server result.
    let fresh = LiveConn::connect(&server).await.unwrap();
    for (c, p) in &cases {
        fresh.subscribe(c.id, c.sql, p.clone());
    }
    fresh
        .wait_ready(&ids, Duration::from_secs(30))
        .await
        .unwrap();
    for (c, _) in &cases {
        let a = conn.result(c.id).unwrap();
        let b = fresh.result(c.id).unwrap();
        assert_eq!(
            a.ordered_rows(),
            b.ordered_rows(),
            "{}: patched state != fresh snapshot",
            c.id
        );
        assert_eq!(b.patches, 0);
    }

    // The fresh connection attached to the same subscriptions (dedupe).
    let debug = server.manager.debug().await;
    for s in debug["subscriptions"].as_array().unwrap() {
        assert_eq!(s["refcount"], 2, "{s}");
    }
}

/// Produce events *while* subscriptions are being created to exercise the snapshot/tail
/// boundary (events land between W, the history scan and the first merge).
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_tail_boundary_race() {
    if !enabled() {
        return;
    }
    let _guard = exclusive().await;
    init_tracing();
    let (server, cfg) = start_server().await.unwrap();
    let main = main_client(&cfg);
    let gen = generator(&cfg).unwrap();
    let mut produced = gen.produce(Some(500), None, TsMode::Now).await.unwrap();

    let producer = {
        let gen = generator(&cfg).unwrap();
        tokio::spawn(async move {
            gen.produce(Some(9000), Some(3000.0), TsMode::Now)
                .await
                .unwrap()
        })
    };
    let conn = LiveConn::connect(&server).await.unwrap();
    let cases: Vec<(Case, Value)> = cases()
        .into_iter()
        .map(|c| {
            let p = (c.params)();
            (c, p)
        })
        .collect();
    for (c, p) in &cases {
        conn.subscribe(c.id, c.sql, p.clone());
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    let ids: Vec<&str> = cases.iter().map(|(c, _)| c.id).collect();
    conn.wait_ready(&ids, Duration::from_secs(60))
        .await
        .unwrap();
    merge_offsets(&mut produced, producer.await.unwrap());

    settle(&server, &main, &conn, &produced).await.unwrap();
    check_all(&conn, &main, &cases).await;
}

/// Unsubscribing drops the history table; the refcount follows attached clients.
#[tokio::test(flavor = "multi_thread")]
async fn refcount_and_cleanup() {
    if !enabled() {
        return;
    }
    let _guard = exclusive().await;
    init_tracing();
    let (server, _cfg) = start_server().await.unwrap();
    let cache = server.cache.clone();
    let sql = "SELECT country, count() AS n FROM events GROUP BY country";
    let a = LiveConn::connect(&server).await.unwrap();
    let b = LiveConn::connect(&server).await.unwrap();
    a.subscribe("x", sql, json!({}));
    b.subscribe(
        "y",
        "SELECT country, count() AS n FROM   events GROUP BY country",
        json!({}),
    );
    a.wait_ready(&["x"], Duration::from_secs(30)).await.unwrap();
    b.wait_ready(&["y"], Duration::from_secs(30)).await.unwrap();
    let debug = server.manager.debug().await;
    let subs = debug["subscriptions"].as_array().unwrap();
    assert_eq!(subs.len(), 1, "whitespace-different SQL dedupes: {debug}");
    assert_eq!(subs[0]["refcount"], 2);

    let hist_tables = || async {
        cache
            .query("SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE 'sub\\_%'", &Params::new())
            .await
            .unwrap()
            .data
            .len()
    };
    assert_eq!(hist_tables().await, 1);
    a.unsubscribe("x");
    b.unsubscribe("y");
    for _ in 0..50 {
        if hist_tables().await == 0
            && server.manager.debug().await["subscriptions"]
                .as_array()
                .unwrap()
                .is_empty()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("history table not dropped");
}

/// Rejections come back as protocol errors over the socket.
#[tokio::test(flavor = "multi_thread")]
async fn rejections_over_websocket() {
    if !enabled() {
        return;
    }
    let _guard = exclusive().await;
    init_tracing();
    let (server, _cfg) = start_server().await.unwrap();
    let conn = LiveConn::connect(&server).await.unwrap();
    let cases = [
        (
            "now",
            "SELECT count() FROM events WHERE ts > now() - INTERVAL 1 HOUR",
            "now() is not allowed",
        ),
        (
            "join",
            "SELECT e.country, d.name FROM events e JOIN countries d ON e.country = d.code",
            "joins",
        ),
        ("final", "SELECT count() FROM events FINAL", "FINAL"),
        (
            "window",
            "SELECT count() OVER () FROM events",
            "window functions",
        ),
    ];
    for (id, sql, _) in cases {
        conn.subscribe(id, sql, json!({}));
    }
    conn.subscribe(
        "param",
        "SELECT count() AS n FROM events WHERE path = {path:String}",
        json!({}),
    );
    conn.wait_ready(
        &["now", "join", "final", "window", "param"],
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    for (id, _, needle) in cases {
        let (code, msg) = conn.result(id).unwrap().error.expect("error");
        assert_eq!(code, "UNSUPPORTED_QUERY");
        assert!(msg.contains(needle), "{id}: {msg}");
    }
    let (code, _) = conn.result("param").unwrap().error.expect("error");
    assert_eq!(code, "INVALID_PARAMS");
}

/// The multiIf watermark predicate, evaluated by ClickHouse itself, puts every row in
/// exactly one of history and tail.
#[tokio::test(flavor = "multi_thread")]
async fn watermark_predicate_in_clickhouse() {
    if !enabled() {
        return;
    }
    let cache = chlive_server::cache_engine::ChdbCache::shared(1, false).unwrap();
    let w = chlive_server::watermark::Watermark(BTreeMap::from([(0, 120), (1, 98), (2, -1)]));
    let sql = format!(
        "SELECT countIf(h AND t) AS both, countIf(NOT h AND NOT t) AS neither, countIf(h) AS hist, count() AS total \
         FROM (SELECT {} AS h, {} AS t FROM (SELECT toUInt64(number % 5) AS _partition, toUInt64(intDiv(number, 5)) AS _offset FROM numbers(2000)))",
        w.history_pred(),
        w.tail_pred()
    );
    let r = cache.query(&sql, &Params::new()).await.unwrap();
    let row = &r.data[0];
    assert_eq!(row["both"], 0);
    assert_eq!(row["neither"], 0);
    // partition 0: offsets 0..=120 (121 rows), partition 1: 0..=98 (99 rows), others none
    assert_eq!(row["hist"], 121 + 99);
    assert_eq!(row["total"], 2000);
}

/// Alias references to aggregates (`big_orders / count()`): expanding them in the rewriter
/// and letting ClickHouse resolve them in the merge query give the same result.
#[tokio::test(flavor = "multi_thread")]
async fn alias_expansion_matches_clickhouse_resolution() {
    if !enabled() {
        return;
    }
    let _guard = exclusive().await;
    init_tracing();
    let (server, cfg) = start_server().await.unwrap();
    let cache = server.cache.clone();
    let main = main_client(&cfg);
    let sql = cases().into_iter().find(|c| c.id == "orders").unwrap().sql;
    let catalog = server.manager.catalog();
    let w = Watermark::from_max_offsets(3, &main_max_offsets(&main, "events").await.unwrap());
    let mut results = Vec::new();
    for (i, expand) in [true, false].into_iter().enumerate() {
        let plan = rewriter::rewrite_with(
            sql,
            catalog,
            &Options {
                expand_aliases: expand,
            },
        )
        .unwrap();
        let hist = format!("alias_test_{i}");
        cache
            .exec(&format!("DROP TABLE IF EXISTS {hist}"), &Params::new())
            .await
            .unwrap();
        cache
            .exec(
                &format!(
                    "CREATE TABLE {hist} ENGINE = Memory AS {}",
                    plan.history_sql(&w)
                ),
                &Params::new(),
            )
            .await
            .unwrap();
        results.push(
            cache
                .query(&plan.merge_sql(&w, &hist), &Params::new())
                .await
                .unwrap()
                .data,
        );
        cache
            .exec(&format!("DROP TABLE {hist}"), &Params::new())
            .await
            .unwrap();
    }
    assert!(!results[0].is_empty());
    // Float sums may differ in the last bits between runs (parallel summation order).
    assert_results_match("alias expand vs resolve", &results[0], &results[1], |c| {
        if c.starts_with("avg") || c.ends_with("ratio") {
            FLOAT
        } else {
            Tol::Exact
        }
    });
}

use chlive_server::watermark::Watermark;
