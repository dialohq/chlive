//! End to end over the full stack: what a client sees over the WebSocket equals the same query
//! run directly on ClickHouse once everything is ingested.

use std::time::Duration;

use anyhow::Result;
use chlive_e2e::*;
use chlive_engine::history::HistorySource;
use futures::FutureExt;
use serde_json::json;

/// Dialect queries over `events`, covering numbers, strings, IN, NOT and AND/OR.
/// (id, query, its ORDER BY columns): numbers, strings, IN, NOT, AND/OR, and ORDER BY.
const CASES: &[(&str, &str, &[&str])] = &[
    ("all", "select * from events", &[]),
    ("slow", "select ts, user_id, latency_ms from events where latency_ms > 500", &[]),
    ("pl_checkout", "select user_id, path, amount from events where country = 'PL' and path = '/checkout'", &[]),
    ("paid_eu", "select user_id, country, amount from events where amount > 0 and country in ('DE', 'FR', 'PL')", &[]),
    ("by_latency", "select ts, user_id, latency_ms from events where latency_ms > 200 order by latency_ms desc", &["latency_ms"]),
    ("not_us", "select country from events where not (country = 'US' or country = 'CA')", &[]),
    ("ranges", "select latency_ms, amount from events where latency_ms >= 10 and latency_ms < 50 and amount <= 100.5", &[]),
    ("by_country_ts", "select ts, user_id, country from events where country in ('PL', 'DE', 'FR') order by country, ts desc", &["country", "ts"]),
    ("by_all", "select country, path, amount from events where amount > 0 order by all", &["country", "path", "amount"]),
    ("top_latency", "select ts, user_id, latency_ms from events order by latency_ms desc limit 50", &["latency_ms"]),
    ("window", "select ts, user_id, latency_ms from events where latency_ms > 100 limit 100", &[]),
    ("top_offset", "select ts, user_id, latency_ms from events order by latency_ms desc limit 10 offset 5", &["latency_ms"]),
    ("window_offset", "select ts, user_id, latency_ms from events where latency_ms > 100 limit 20 offset 7", &[]),
    // UUIDs: events spell them in upper case or without dashes; ClickHouse's order isn't the text's
    ("session", "select ts, user_id, session_id from events where session_id = '6F1D3A20-1B2C-4D5E-8F90-A1B2C3D4E5F6'", &[]),
    ("by_session", "select ts, session_id, latency_ms from events where session_id > '0a0a0a0a00004000800000000000000a' order by session_id, latency_ms desc", &["session_id", "latency_ms"]),
];

#[tokio::test]
async fn live_results_match_direct_queries() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    with_server(config(), |server| async move {
    let producer = producer()?;
    let conn = LiveConn::connect(server).await?;

    // Half the queries subscribe before the events, half while they're being produced, so
    // their watermarks land in the middle of the stream.
    let (before, during) = CASES.split_at(CASES.len() / 2);
    for (id, sql, _) in before {
        conn.subscribe(id, sql);
    }
    conn.wait_ready(
        &before.iter().map(|c| c.0).collect::<Vec<_>>(),
        Duration::from_secs(60),
    )
    .await?;
    let producing = tokio::spawn(async move { producer.produce(3000, Some(2000.0)).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    for (id, sql, _) in during {
        conn.subscribe(id, sql);
    }
    let produced = producing.await??;
    conn.wait_ready(
        &CASES.iter().map(|c| c.0).collect::<Vec<_>>(),
        Duration::from_secs(60),
    )
    .await?;
    settle(&conn, &produced).await?;

    for (id, sql, keys) in CASES {
        let live = conn.result(id).unwrap();
        assert!(live.error.is_none(), "{id}: {:?}", live.error);
        assert!(
            live.protocol_violations.is_empty(),
            "{id}: {:?}",
            live.protocol_violations
        );
        let direct = direct(sql).await?;
        eprintln!(
            "{id:>12}: {} rows ({} live inserts in {} diffs)",
            direct.len(),
            live.inserted,
            live.diffs
        );
        if sql.contains(" limit ") {
            check_limited(id, sql, keys, &live, &direct);
            continue;
        }
        assert!(live.inserted > 0, "{id}: no live inserts");
        if !keys.is_empty() {
            // the same keys in the same order as ClickHouse, the same rows in every tie group
            let (l, d) = (tie_groups(&live.rows, keys), tie_groups(&direct, keys));
            if l != d {
                let at = l
                    .iter()
                    .zip(&d)
                    .position(|(a, b)| a != b)
                    .unwrap_or(l.len().min(d.len()));
                panic!(
                    "{id}: order differs from ClickHouse at tie group {at}: live {:?}, ClickHouse {:?}",
                    l.get(at),
                    d.get(at)
                );
            }
        }
        let (live, direct) = (multiset(&live.rows), multiset(&direct));
        if live != direct {
            let missing: Vec<_> = direct
                .iter()
                .filter(|r| !live.contains(r))
                .take(3)
                .collect();
            let extra: Vec<_> = live
                .iter()
                .filter(|r| !direct.contains(r))
                .take(3)
                .collect();
            panic!(
                "{id}: live has {} rows, ClickHouse {}; missing e.g. {missing:?}, extra e.g. {extra:?}",
                live.len(),
                direct.len()
            );
        }
    }

    // a fresh subscription gets the same result: a recent snapshot (shared, up to the refresh
    // interval old), then whatever came after it, replayed from Kafka
    let fresh = LiveConn::connect(server).await?;
    fresh.subscribe("slow", CASES[1].1);
    fresh.wait_ready(&["slow"], Duration::from_secs(60)).await?;
    fresh
        .wait_quiet(Duration::from_millis(500), Duration::from_secs(30))
        .await?;
    let r = fresh.result("slow").unwrap();
    assert_eq!(
        multiset(&r.rows),
        multiset(&conn.result("slow").unwrap().rows)
    );
    Ok(())
}
.boxed())
.await
}

#[tokio::test]
async fn rejections_over_websocket() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    with_server(config(), |server| {
        async move {
            let conn = LiveConn::connect(server).await?;
            let cases = [
                ("syntax", "select * form events", "SYNTAX_ERROR", "at 1:10"),
                (
                    "table",
                    "select * from nope",
                    "INVALID_QUERY",
                    "unknown table nope (not in the server's config) at 1:15",
                ),
                (
                    "column",
                    "select nope from events",
                    "INVALID_QUERY",
                    "unknown column nope in table events",
                ),
                (
                    "type",
                    "select * from events where latency_ms > 'x'",
                    "INVALID_QUERY",
                    "cannot compare UInt32 with a string at 1:41",
                ),
                (
                    "group",
                    "select country, count() from events group by country having count() > 1",
                    "SYNTAX_ERROR",
                    "HAVING is not supported yet",
                ),
                (
                    "final",
                    "select * from products final order by version limit 5 offset 1",
                    "INVALID_QUERY",
                    "FINAL on products (ReplacingMergeTree) with OFFSET isn't supported yet at 1:24",
                ),
            ];
            for (id, sql, _, _) in &cases {
                conn.subscribe(id, sql);
            }
            conn.wait_ready(
                &cases.iter().map(|c| c.0).collect::<Vec<_>>(),
                Duration::from_secs(30),
            )
            .await?;
            for (id, _, code, message) in cases {
                let (got_code, got_message) = conn.result(id).unwrap().error.expect("an error");
                assert_eq!(got_code, code, "{id}: {got_message}");
                assert!(got_message.contains(message), "{id}: {got_message}");
            }
            Ok(())
        }
        .boxed()
    })
    .await
}

#[tokio::test]
async fn unsubscribe_stops_the_stream() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    with_server(config(), |server| {
        async move {
            let producer = producer()?;
            let conn = LiveConn::connect(server).await?;
            conn.subscribe("kept", "select user_id from events");
            conn.subscribe("dropped", "select user_id from events");
            conn.wait_ready(&["kept", "dropped"], Duration::from_secs(60))
                .await?;
            conn.unsubscribe("dropped");
            tokio::time::sleep(Duration::from_millis(200)).await;
            let before = conn.result("dropped").unwrap().rows.len();

            let produced = producer.produce(200, None).await?;
            settle(&conn, &produced).await?;
            assert_eq!(conn.result("kept").unwrap().inserted, 200);
            assert_eq!(conn.result("dropped").unwrap().rows.len(), before);
            Ok(())
        }
        .boxed()
    })
    .await
}

/// LIMIT (and OFFSET): never more rows than the limit (checked after every diff by the harness).
/// - With ORDER BY, the same tie groups as ClickHouse's top N (after the top M), except which
///   rows of the boundary groups made it in, which is arbitrary in ClickHouse too.
/// - Without, a window: full, every row matching, the streamed rows last.
fn check_limited(id: &str, sql: &str, keys: &[&str], live: &LiveResult, direct: &[Row]) {
    let (_, window) = sql.rsplit_once(" limit ").unwrap();
    let limit: usize = window.split_whitespace().next().unwrap().parse().unwrap();
    let offset = window.contains(" offset ");
    assert_eq!(live.limit, Some(limit), "{id}");
    if keys.is_empty() {
        assert_eq!(live.rows.len(), limit, "{id}: window not full");
        assert!(live.inserted > 0, "{id}: no live inserts");
        assert!(
            live.rows
                .iter()
                .all(|r| r["latency_ms"].as_u64().unwrap() > 100),
            "{id}: a row that doesn't match"
        );
        return;
    }
    let boundary = |rows: &[Row]| {
        let mut g = tie_groups(rows, keys);
        if let Some((key, rows)) = g.last_mut() {
            *rows = vec![format!("{key}: {} rows", rows.len())];
        }
        if offset {
            if let Some((key, rows)) = g.first_mut() {
                *rows = vec![format!("{key}: {} rows", rows.len())];
            }
        }
        g
    };
    assert_eq!(live.rows.len(), direct.len(), "{id}: row count");
    assert_eq!(
        boundary(&live.rows),
        boundary(direct),
        "{id}: top {limit} differs from ClickHouse"
    );
}

/// Offline cache, end to end: keep a result and its cursor, go away while events arrive, come
/// back with `since`: the server resumes, sends only what was missed, and the result equals the
/// query on ClickHouse.
#[tokio::test]
async fn resume_after_being_away() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    with_server(config(), |server| {
        async move {
            let producer = producer()?;
            const QUERIES: &[(&str, &str, &[&str])] = &[
                (
                    "pl",
                    "select ts, user_id, latency_ms from events where country = 'PL'",
                    &[],
                ),
                (
                    "top",
                    "select ts, user_id, latency_ms from events order by latency_ms desc limit 30",
                    &["latency_ms"],
                ),
                (
                    "ordered",
                    "select user_id, country from events where latency_ms > 800 order by country",
                    &["country"],
                ),
            ];

            // a first session: subscribe, take in some live events, remember where we are
            let conn = LiveConn::connect(server).await?;
            for (id, sql, _) in QUERIES {
                conn.subscribe(id, sql);
            }
            conn.wait_ready(
                &QUERIES.iter().map(|q| q.0).collect::<Vec<_>>(),
                Duration::from_secs(60),
            )
            .await?;
            let produced = producer.produce(300, None).await?;
            settle(&conn, &produced).await?;
            let kept: Vec<_> = QUERIES.iter().map(|q| conn.result(q.0).unwrap()).collect();
            drop(conn);

            // away: more events arrive
            let produced = producer.produce(600, None).await?;

            // back: resume each query from its cursor
            let conn = LiveConn::connect(server).await?;
            for ((id, sql, _), r) in QUERIES.iter().zip(&kept) {
                conn.resume(id, sql, r.at.as_deref().expect("a cursor"), r.rows.clone());
            }
            conn.wait_ready(
                &QUERIES.iter().map(|q| q.0).collect::<Vec<_>>(),
                Duration::from_secs(60),
            )
            .await?;
            settle(&conn, &produced).await?;
            for ((id, sql, keys), before) in QUERIES.iter().zip(&kept) {
                let live = conn.result(id).unwrap();
                assert!(live.error.is_none(), "{id}: {:?}", live.error);
                assert!(
                    live.protocol_violations.is_empty(),
                    "{id}: {:?}",
                    live.protocol_violations
                );
                assert!(live.resumed, "{id}: got a snapshot instead of a resume");
                let direct = direct(sql).await?;
                eprintln!(
                    "{id:>8}: {} rows kept, {} changes on resume, {} rows now (ClickHouse {})",
                    before.rows.len(),
                    live.inserted,
                    live.rows.len(),
                    direct.len()
                );
                if keys.is_empty() {
                    assert_eq!(multiset(&live.rows), multiset(&direct), "{id}");
                } else if sql.contains(" limit ") {
                    let boundary = |rows: &[Row]| {
                        let mut g = tie_groups(rows, keys);
                        if let Some((k, rows)) = g.last_mut() {
                            *rows = vec![format!("{k}: {}", rows.len())];
                        }
                        g
                    };
                    assert_eq!(boundary(&live.rows), boundary(&direct), "{id}");
                } else {
                    assert_eq!(
                        tie_groups(&live.rows, keys),
                        tie_groups(&direct, keys),
                        "{id}"
                    );
                }
            }
            Ok(())
        }
        .boxed()
    })
    .await
}

/// GROUP BY over the full stack (chDB in the server): after events arrive live, the result
/// built from the snapshot plus inserts and updates equals ClickHouse's GROUP BY.
#[tokio::test]
async fn live_aggregates_match_direct_queries() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    with_server(config(), |server| async move {
    let producer = producer()?;
    const QUERIES: &[(&str, &str)] = &[
        ("total", "select count() as n, sum(amount), avg(latency_ms) from events"),
        ("by_country", "select country, count() as n, avg(latency_ms) as avg_latency, max(latency_ms) from events group by country"),
        ("pl_paths", "select path, count(), min(ts), max(amount) from events where country = 'PL' group by path"),
        // no native uniqExact: runs in chDB
        ("users", "select country, uniqExact(user_id) as users, count() as n from events group by country"),
        // ordered and limited: groups move and come in and out
        ("top_paths", "select path, count() as n, sum(amount) as revenue from events group by path order by n desc limit 5"),
        // UUIDs: grouped, ordered and as min/max, by ClickHouse's order
        ("sessions", "select session_id, count() as n, max(latency_ms) from events group by session_id order by session_id desc"),
        ("session_range", "select min(session_id), max(session_id), count(session_id) from events where country = 'PL'"),
    ];
    let conn = LiveConn::connect(server).await?;
    for (id, sql) in QUERIES {
        conn.subscribe(id, sql);
    }
    conn.wait_ready(
        &QUERIES.iter().map(|q| q.0).collect::<Vec<_>>(),
        Duration::from_secs(60),
    )
    .await?;
    let produced = producer.produce(2000, Some(2000.0)).await?;
    settle(&conn, &produced).await?;
    // floats to 9 significant digits: ClickHouse adds in parallel, in no particular order
    let canon = |rows: &[Row]| -> Vec<String> {
        let mut v: Vec<String> = rows
            .iter()
            .map(|r| {
                r.iter()
                    .map(|(k, v)| match v.as_f64() {
                        Some(f) if !v.is_i64() && !v.is_u64() => format!("{k}={f:.9e}"),
                        _ => format!("{k}={v}"),
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect();
        v.sort();
        v
    };
    for (id, sql) in QUERIES {
        let live = conn.result(id).unwrap();
        assert!(live.error.is_none(), "{id}: {:?}", live.error);
        assert!(
            live.protocol_violations.is_empty(),
            "{id}: {:?}",
            live.protocol_violations
        );
        assert!(live.updated > 0, "{id}: no updates");
        let direct = clickhouse()
            .fetch(&sql.replace("from events", "from default.events"))
            .await?;
        eprintln!(
            "{id:>10}: {} groups, {} updates",
            live.rows.len(),
            live.updated
        );
        if sql.contains(" limit ") {
            check_limited(id, sql, &["n"], &live, &direct);
        } else {
            assert_eq!(canon(&live.rows), canon(&direct), "{id}");
        }
    }
    drop(conn);
    Ok(())
}
.boxed())
.await?;
    // the only test here using chDB: close it, or the test process crashes on exit
    tokio::time::sleep(Duration::from_millis(200)).await;
    chlive_engine::chdb::shutdown();
    Ok(())
}

/// The clones of `default.orders` that exist now.
async fn orders_clones() -> Result<Vec<String>> {
    let rows = clickhouse()
        .fetch(
            // `config()` makes them there, as the dev server does
            "SELECT name FROM system.tables WHERE database = 'chlive_snapshots' \
             AND startsWith(name, '__chlive_snap_orders_') ORDER BY name",
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| r["name"].as_str().unwrap().to_string())
        .collect())
}

/// Until `done` holds of the clones, at most 20 s (a refresh pauses the connector: ~1-2 s).
async fn until_clones(what: &str, done: impl Fn(&[String]) -> bool) -> Result<Vec<String>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let now = orders_clones().await?;
        if done(&now) {
            return Ok(now);
        }
        anyhow::ensure!(std::time::Instant::now() < deadline, "{what}: {now:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Snapshots follow usage: a table in use gets fresh clones, replaced ones are dropped, and
/// after the idle time without subscriptions the last one goes too. (`orders`, which no other
/// test subscribes to, with a short refresh and idle time.)
#[tokio::test]
async fn snapshots_are_refreshed_while_used_and_dropped_when_idle() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    let mut config = config();
    config.snapshots.refresh = Duration::from_secs(1);
    config.snapshots.idle = Duration::from_secs(3);
    // like the server: start by dropping clones left behind (e.g. by an earlier failed run)
    config.snapshots.sweep = true;
    // the shared result is what keeps the table in use: let it go right after the client
    let settings = chlive_server::ServerSettings {
        cache: chlive_server::CacheSettings {
            keep_alive: Duration::from_millis(100),
            keep_warm: Duration::ZERO,
            ..Default::default()
        },
        ..Default::default()
    };
    with_server_settings(config, settings, |server| {
        async move {
            let conn = LiveConn::connect(server).await?;
            conn.subscribe("o", "select order_id, amount from orders");
            conn.wait_ready(&["o"], Duration::from_secs(60)).await?;
            let first = orders_clones().await?;
            assert_eq!(first.len(), 1, "{first:?}");
            // refreshed: a new clone replaces it, and the replaced one is dropped
            until_clones("refreshed", |c| c.len() == 1 && c[0] != first[0]).await?;
            // no subscriptions for longer than the idle time: no clone left
            drop(conn);
            until_clones("dropped when idle", |c| c.is_empty()).await?;
            Ok(())
        }
        .boxed()
    })
    .await
}

/// A semi-join over two topics: events kept back until their user enters the sub-query (as a
/// customer of this run's country), then back-filled from the snapshot and the kept-back tail.
#[tokio::test]
async fn semi_join_matches_clickhouse() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    // a country of this run only: the sub-query starts empty
    let country = format!(
        "X{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            % 1_000_000_000
    );
    let sql = format!(
        "select ts, user_id, path from events \
         where user_id in (select customer_id from customers where country = '{country}')"
    );
    // the same semi-join as a correlated EXISTS, with aliases
    let exists = format!(
        "select e.ts, e.user_id, e.path from events e where exists \
         (select 1 from customers c where c.customer_id = e.user_id and c.country = '{country}')"
    );
    with_server(config(), |server| {
        async move {
            let conn = LiveConn::connect(server).await?;
            conn.subscribe("semi", &sql);
            conn.subscribe("exists", &exists);
            conn.wait_ready(&["semi", "exists"], Duration::from_secs(60)).await?;
            let events = producer()?;
            let customers = producer::Producer::new(&config().kafka_brokers, "customers")?;
            // events of users not in the sub-query yet: kept back
            let mut produced = events.produce(500, Some(2000.0)).await?;
            // 40 of the 5000 users become customers of this country: their events so far come
            let new: Vec<String> = (0..40)
                .map(|i| {
                    format!(
                        r#"{{"ts":"2026-10-09 00:00:00.000","customer_id":"u{}","name":"n","country":"{country}"}}"#,
                        i * 125
                    )
                })
                .collect();
            let joined = customers.produce_json(&new).await?;
            // and their later events pass
            for (p, o) in events.produce(500, Some(2000.0)).await? {
                let e = produced.entry(p).or_insert(o);
                *e = (*e).max(o);
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(120);
            loop {
                let have = ingested_from("customers").await?;
                if joined
                    .iter()
                    .all(|(p, o)| have.get(p).is_some_and(|h| h >= o))
                {
                    break;
                }
                anyhow::ensure!(
                    std::time::Instant::now() < deadline,
                    "customers not ingested: {have:?}"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            settle(&conn, &produced).await?;
            let live = conn.result("semi").unwrap();
            assert!(live.error.is_none(), "{:?}", live.error);
            assert!(live.protocol_violations.is_empty(), "{:?}", live.protocol_violations);
            let direct = clickhouse()
                .fetch(
                    &sql.replace("from events", "from default.events")
                        .replace("from customers", "from default.customers"),
                )
                .await?;
            eprintln!("semi: {} rows (inserted live {})", live.rows.len(), live.inserted);
            assert!(!direct.is_empty());
            assert_eq!(multiset(&live.rows), multiset(&direct));
            let live = conn.result("exists").unwrap();
            assert!(live.error.is_none(), "{:?}", live.error);
            assert_eq!(multiset(&live.rows), multiset(&direct), "EXISTS");
            Ok(())
        }
        .boxed()
    })
    .await
}

/// (product_id, version, deleted) of each row.
fn versions(rows: &[chlive_engine::diff::Row]) -> std::collections::BTreeSet<(String, u64, u64)> {
    rows.iter()
        .map(|r| {
            let n = |k: &str| r[k].as_u64().unwrap_or_else(|| panic!("{k} in {r:?}"));
            (
                r["product_id"].as_str().unwrap().to_string(),
                n("version"),
                n("deleted"),
            )
        })
        .collect()
}

/// A ReplacingMergeTree read without FINAL, like a MergeTree: the snapshot's versions (as many
/// as merges left), then every new version as a new row. Its snapshots keep the engine, so FINAL
/// on them dedups as on the table.
#[tokio::test]
async fn replacing_tables_read_like_merge_tree() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    // products of this run only
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let ids: Vec<String> = ["a", "b", "c"]
        .iter()
        .map(|k| format!("p{run}-{k}"))
        .collect();
    let product = |id: &str, version: u64, deleted: u8| {
        format!(
            r#"{{"product_id":"{id}","name":"n","price":1.5,"version":{version},"deleted":{deleted}}}"#
        )
    };
    let list = ids
        .iter()
        .map(|i| format!("'{i}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql =
        format!("select product_id, version, deleted from products where product_id in ({list})");
    with_server(config(), |server| {
        async move {
            let products = producer::Producer::new(&config().kafka_brokers, "products")?;
            let wait = |produced: std::collections::BTreeMap<i32, i64>| async move {
                let deadline = std::time::Instant::now() + Duration::from_secs(120);
                loop {
                    let have = ingested_from("products").await?;
                    if produced.iter().all(|(p, o)| have.get(p).is_some_and(|h| h >= o)) {
                        return anyhow::Ok(());
                    }
                    anyhow::ensure!(
                        std::time::Instant::now() < deadline,
                        "products not ingested: {have:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            };
            // two versions of each, before subscribing
            let before: Vec<String> = ids
                .iter()
                .flat_map(|id| [product(id, 1, 0), product(id, 2, 0)])
                .collect();
            wait(products.produce_json(&before).await?).await?;

            let conn = LiveConn::connect(server).await?;
            conn.subscribe("p", &sql);
            // FINAL on a MergeTree changes nothing
            conn.subscribe("orders", "select order_id from orders final");
            conn.wait_ready(&["p", "orders"], Duration::from_secs(60)).await?;
            let orders = conn.result("orders").unwrap();
            assert!(orders.error.is_none(), "orders FINAL: {:?}", orders.error);

            // the snapshot is a ReplacingMergeTree too, with the table's keys (others, e.g. an
            // earlier test's not dropped yet, may exist: this server's has this run's products)
            let clones = clickhouse()
                .fetch(
                    "SELECT name, engine_full FROM system.tables WHERE database = 'chlive_snapshots' \
                     AND startsWith(name, '__chlive_snap_products_')",
                )
                .await?;
            assert!(!clones.is_empty(), "a snapshot of products");
            for clone in &clones {
                let engine_full = clone["engine_full"].as_str().unwrap();
                assert!(
                    engine_full.starts_with("ReplacingMergeTree(version, deleted)")
                        && engine_full.contains("ORDER BY product_id"),
                    "{engine_full}"
                );
            }
            let latest = |table: String| {
                let list = list.clone();
                async move {
                    clickhouse()
                        .fetch(&format!(
                            "SELECT product_id, version, deleted FROM {table} FINAL \
                             WHERE product_id IN ({list})"
                        ))
                        .await
                }
            };
            let final_rows = versions(&latest("default.products".into()).await?);
            let mut on_clones = Vec::new();
            for clone in &clones {
                let name = clone["name"].as_str().unwrap();
                on_clones.push(versions(&latest(format!("chlive_snapshots.`{name}`")).await?));
            }
            assert!(
                on_clones.contains(&final_rows),
                "FINAL on the snapshot: {on_clones:?}, want {final_rows:?}"
            );
            assert_eq!(final_rows.len(), 3, "{final_rows:?}");

            // what merges left of the versions: at least the latest, at most all
            let live = conn.result("p").unwrap();
            assert!(live.error.is_none(), "{:?}", live.error);
            let seen = versions(&live.rows);
            let all: std::collections::BTreeSet<_> = ids
                .iter()
                .flat_map(|id| [(id.clone(), 1, 0), (id.clone(), 2, 0)])
                .collect();
            assert!(seen.is_superset(&final_rows), "{seen:?} lacks some of {final_rows:?}");
            assert!(seen.is_subset(&all), "{seen:?} has more than {all:?}");

            // new versions are new rows, a deleted one too (without FINAL)
            let after = [product(&ids[0], 3, 0), product(&ids[1], 4, 1)];
            wait(products.produce_json(&after).await?).await?;
            conn.wait_quiet(Duration::from_millis(1000), Duration::from_secs(60))
                .await?;
            let live = conn.result("p").unwrap();
            assert!(live.protocol_violations.is_empty(), "{:?}", live.protocol_violations);
            let now = versions(&live.rows);
            assert_eq!(live.rows.len(), seen.len() + 2, "{now:?}");
            assert!(now.contains(&(ids[0].clone(), 2, 0)) && now.contains(&(ids[0].clone(), 3, 0)));
            assert!(now.contains(&(ids[1].clone(), 4, 1)), "{now:?}");
            Ok(())
        }
        .boxed()
    })
    .await
}

/// FINAL on a ReplacingMergeTree over the WebSocket: one row per product, its latest version.
/// Versions before and after subscribing (each product's in one partition, as a keyed topic
/// has them), updates, deletes and rows moving in and out of the WHERE end equal to ClickHouse's
/// `SELECT … FINAL`.
#[tokio::test]
async fn final_results_match_clickhouse() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let ids: Vec<String> = (0..8).map(|k| format!("f{run}-{k}")).collect();
    let list = ids
        .iter()
        .map(|i| format!("'{i}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select product_id, price, version from products final \
         where product_id in ({list}) and price > 20"
    );
    // the same, ordered: rows move as their prices change
    let ordered = format!("{sql} order by price desc");
    // and any 3 of them: the newest
    let limited = format!("{sql} limit 3");
    // and the top 3 by price
    let top = format!("{ordered} limit 3");
    // each product: versions that only grow, prices in and out of the WHERE, some deleted
    let versions = move |round: u64| -> Vec<(String, String)> {
        ids.iter()
            .enumerate()
            .map(|(k, id)| {
                let n = round * 8 + k as u64;
                let price = (n * 37) % 50;
                let deleted = u8::from(n % 5 == 4);
                (
                    id.clone(),
                    format!(
                        r#"{{"product_id":"{id}","name":"n","price":{price},"version":{round},"deleted":{deleted}}}"#
                    ),
                )
            })
            .collect()
    };
    with_server(config(), |server| {
        async move {
            let products = producer::Producer::new(&config().kafka_brokers, "products")?;
            let wait = |produced: std::collections::BTreeMap<i32, i64>| async move {
                let deadline = std::time::Instant::now() + Duration::from_secs(120);
                loop {
                    let have = ingested_from("products").await?;
                    if produced
                        .iter()
                        .all(|(p, o)| have.get(p).is_some_and(|h| h >= o))
                    {
                        return anyhow::Ok(());
                    }
                    anyhow::ensure!(
                        std::time::Instant::now() < deadline,
                        "products not ingested: {have:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            };
            for round in 1..=3 {
                wait(products.produce_keyed(&versions(round)).await?).await?;
            }
            let conn = LiveConn::connect(server).await?;
            conn.subscribe("f", &sql);
            conn.subscribe("ordered", &ordered);
            conn.subscribe("limited", &limited);
            conn.subscribe("top", &top);
            conn.wait_ready(&["f", "ordered", "limited", "top"], Duration::from_secs(60))
                .await?;
            for round in 4..=9 {
                wait(products.produce_keyed(&versions(round)).await?).await?;
            }
            conn.wait_quiet(Duration::from_millis(1000), Duration::from_secs(60))
                .await?;
            let live = conn.result("f").unwrap();
            assert!(live.error.is_none(), "{:?}", live.error);
            assert!(
                live.protocol_violations.is_empty(),
                "{:?}",
                live.protocol_violations
            );
            assert!(live.ids.iter().all(Option::is_some), "rows without ids");
            let direct = clickhouse()
                .fetch(&sql.replace("from products", "from default.products"))
                .await?;
            eprintln!(
                "final: {} rows ({} inserts, {} updates, {} deletes)",
                live.rows.len(),
                live.inserted,
                live.updated,
                live.deleted
            );
            assert!(live.updated > 0 && live.deleted > 0);
            assert_eq!(multiset(&live.rows), multiset(&direct));
            let live = conn.result("ordered").unwrap();
            assert!(live.error.is_none(), "{:?}", live.error);
            assert!(
                live.protocol_violations.is_empty(),
                "{:?}",
                live.protocol_violations
            );
            let direct = clickhouse()
                .fetch(&ordered.replace("from products", "from default.products"))
                .await?;
            assert_eq!(
                tie_groups(&live.rows, &["price"]),
                tie_groups(&direct, &["price"]),
                "ordered"
            );
            // any 3 rows of the result (fewer if it has fewer), each as ClickHouse has it
            let live = conn.result("limited").unwrap();
            assert!(live.error.is_none(), "{:?}", live.error);
            assert!(
                live.protocol_violations.is_empty(),
                "{:?}",
                live.protocol_violations
            );
            let all = multiset(&direct);
            assert_eq!(
                live.rows.len(),
                all.len().min(3),
                "limited: {:?}",
                live.rows
            );
            for r in multiset(&live.rows) {
                assert!(all.contains(&r), "limited: {r} isn't a row of the result");
            }
            let mut ids: Vec<_> = live.ids.iter().flatten().collect();
            ids.sort();
            ids.dedup();
            assert_eq!(ids.len(), live.rows.len(), "limited: ids");
            // the top 3 by price, as ClickHouse has them (the last tie group by price and size)
            let live = conn.result("top").unwrap();
            assert!(live.error.is_none(), "{:?}", live.error);
            let want = clickhouse()
                .fetch(&top.replace("from products", "from default.products"))
                .await?;
            check_limited("top", &top, &["price"], &live, &want);
            Ok(())
        }
        .boxed()
    })
    .await
}

/// The Rust client (`chlive-client`) over the real server: a plain, an ordered and limited, and
/// an aggregated query, kept up to date by its diffs, equal ClickHouse's answers.
#[tokio::test]
async fn rust_client_matches_direct_queries() -> Result<()> {
    use chlive_client::{ClientOptions, Kind, LiveClient, Status};
    use futures::StreamExt;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    with_server(config(), |server| async move {
    const QUERIES: &[(&str, &str, Option<usize>)] = &[
        ("slow", "select ts, user_id, latency_ms from events where latency_ms > 500", None),
        ("top_latency", "select ts, user_id, latency_ms from events order by latency_ms desc limit 20", Some(20)),
        ("by_country", "select country, count() as n, avg(latency_ms) as avg_latency from events group by country", None),
    ];
    let client = LiveClient::new(ClientOptions::new(format!("ws://{}/live", server.addr)));
    #[derive(Default)]
    struct Seen {
        live: bool,
        rows: Vec<Row>,
        changes: usize,
        error: Option<String>,
    }
    let seen: Vec<Arc<Mutex<Seen>>> = QUERIES.iter().map(|_| Default::default()).collect();
    let last = Arc::new(Mutex::new(Instant::now()));
    let mut tasks = Vec::new();
    for ((id, sql, limit), seen) in QUERIES.iter().zip(&seen) {
        let mut sub = client.subscribe::<Row>(*sql);
        let (seen, last, id, limit) = (seen.clone(), last.clone(), *id, *limit);
        tasks.push(tokio::spawn(async move {
            while let Some(u) = sub.next().await {
                if let Some(n) = limit {
                    assert!(u.rows.len() <= n, "{id}: {} rows", u.rows.len());
                }
                let mut s = seen.lock().unwrap();
                s.live |= u.kind == Kind::Snapshot;
                s.rows = u.rows.iter().map(|r| (**r).clone()).collect();
                s.changes += u.changes.len();
                if let Status::Error(e) = &u.status {
                    s.error = Some(e.to_string());
                }
                *last.lock().unwrap() = Instant::now();
            }
        }));
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    while !seen.iter().all(|s| s.lock().unwrap().live) {
        anyhow::ensure!(Instant::now() < deadline, "no snapshots");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let producer = producer()?;
    let mut produced = producer.produce(1500, Some(2000.0)).await?;
    // and three just slower than anything so far, so the top 20 changes
    let max = clickhouse()
        .fetch("SELECT max(latency_ms) AS m FROM default.events")
        .await?;
    let base = max[0]["m"].as_u64().unwrap_or(0) + 1;
    let ts = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f");
    let slowest: Vec<String> = (0..3)
        .map(|i| {
            serde_json::json!({"ts": ts.to_string(), "user_id": format!("rust-client-{i}"),
                "country": "PL", "path": "/slow", "latency_ms": base + i, "amount": 0})
            .to_string()
        })
        .collect();
    for (p, o) in producer.produce_json(&slowest).await? {
        let e = produced.entry(p).or_insert(o);
        *e = (*e).max(o);
    }
    wait_ingested(&produced).await?;
    // then until the client has been quiet for a second
    while last.lock().unwrap().elapsed() < Duration::from_secs(1) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // floats to 9 significant digits: ClickHouse adds in parallel, in no particular order
    let canon = |rows: &[Row]| -> Vec<String> {
        let mut v: Vec<String> = rows
            .iter()
            .map(|r| {
                r.iter()
                    .map(|(k, v)| match v.as_f64() {
                        Some(f) if !v.is_i64() && !v.is_u64() => format!("{k}={f:.9e}"),
                        _ => format!("{k}={v}"),
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect();
        v.sort();
        v
    };
    for ((id, sql, limit), seen) in QUERIES.iter().zip(&seen) {
        let direct = if sql.contains(" group by ") {
            clickhouse()
                .fetch(&sql.replace("from events", "from default.events"))
                .await?
        } else {
            direct(sql).await?
        };
        let s = seen.lock().unwrap();
        assert!(s.error.is_none(), "{id}: {:?}", s.error);
        assert!(s.changes > 0, "{id}: no changes");
        eprintln!("{id:>12}: {} rows, {} changes", s.rows.len(), s.changes);
        if limit.is_some() {
            let live = LiveResult {
                rows: s.rows.clone(),
                limit: *limit,
                inserted: s.changes,
                ..Default::default()
            };
            check_limited(id, sql, &["latency_ms"], &live, &direct);
        } else {
            assert_eq!(canon(&s.rows), canon(&direct), "{id}");
        }
    }
    client.close().await;
    for t in tasks {
        t.await?;
    }
    Ok(())
}
.boxed())
.await
}

#[tokio::test]
async fn parameters_match_the_same_query_with_literals() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    let sql = "select ts, user_id, session_id from events \
               where session_id = {s:UUID} and latency_ms > {l:UInt32}";
    let (a, b) = (producer::SESSIONS[0], producer::SESSIONS[2]);
    with_server(config(), |server| {
        async move {
            let producer = producer()?;
            let conn = LiveConn::connect(server).await?;
            // read by their declared types: an upper-case UUID, a number as a string
            conn.subscribe_with(
                "a",
                sql,
                serde_json::json!({"s": a.to_uppercase(), "l": "50"}),
            );
            conn.subscribe_with("b", sql, serde_json::json!({"s": b, "l": 50}));
            conn.wait_ready(&["a", "b"], Duration::from_secs(60))
                .await?;
            let produced = producer.produce(2000, Some(2000.0)).await?;
            settle(&conn, &produced).await?;
            let mut results = Vec::new();
            for (id, session) in [("a", a), ("b", b)] {
                let live = conn.result(id).unwrap();
                assert!(live.error.is_none(), "{id}: {:?}", live.error);
                assert!(
                    live.protocol_violations.is_empty(),
                    "{id}: {:?}",
                    live.protocol_violations
                );
                assert!(live.inserted > 0, "{id}: no live inserts");
                let direct = direct(&format!(
                    "select ts, user_id, session_id from events \
                     where session_id = '{session}' and latency_ms > 50"
                ))
                .await?;
                eprintln!(
                    "{id}: {} rows ({} inserted live)",
                    live.rows.len(),
                    live.inserted
                );
                assert_eq!(multiset(&live.rows), multiset(&direct), "{id}");
                results.push(multiset(&live.rows));
            }
            assert_ne!(results[0], results[1], "other values, other rows");
            Ok(())
        }
        .boxed()
    })
    .await
}

/// Many connections, overlapping queries: one shared entry per distinct query (and parameter
/// values), each joiner getting it as it is; every result still matches ClickHouse.
#[tokio::test]
async fn connections_share_one_entry_per_query() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    with_server(config(), |server| {
        async move {
            let producer = producer()?;
            const QUERIES: &[&str] = &[
                "select ts, user_id, latency_ms from events where country = 'PL'",
                "select ts, user_id, latency_ms from events order by latency_ms desc limit 20",
            ];
            let conns = [
                LiveConn::connect(server).await?,
                LiveConn::connect(server).await?,
                LiveConn::connect(server).await?,
            ];
            // the first connection subscribes before the events, the others while they come
            for (i, sql) in QUERIES.iter().enumerate() {
                conns[0].subscribe(&format!("q{i}"), sql);
            }
            conns[0]
                .wait_ready(&["q0", "q1"], Duration::from_secs(60))
                .await?;
            let producing = tokio::spawn(async move { producer.produce(1000, Some(2000.0)).await });
            tokio::time::sleep(Duration::from_millis(300)).await;
            for conn in &conns[1..] {
                for (i, sql) in QUERIES.iter().enumerate() {
                    conn.subscribe(&format!("q{i}"), sql);
                }
            }
            let produced = producing.await??;
            for conn in &conns {
                conn.wait_ready(&["q0", "q1"], Duration::from_secs(60))
                    .await?;
                settle(conn, &produced).await?;
            }
            let listed = entries(server).await?;
            assert_eq!(
                listed.len(),
                QUERIES.len(),
                "one entry per query: {listed:?}"
            );
            for e in &listed {
                assert_eq!(e["subscribers"].as_array().unwrap().len(), 3, "{e}");
            }
            for (i, sql) in QUERIES.iter().enumerate() {
                let direct = direct(sql).await?;
                for (n, conn) in conns.iter().enumerate() {
                    let live = conn.result(&format!("q{i}")).unwrap();
                    assert!(live.error.is_none(), "conn {n} q{i}: {:?}", live.error);
                    assert!(
                        live.protocol_violations.is_empty(),
                        "conn {n} q{i}: {:?}",
                        live.protocol_violations
                    );
                    if sql.contains(" limit ") {
                        let keys = |rows: &[Row]| -> Vec<String> {
                            rows.iter().map(|r| r["latency_ms"].to_string()).collect()
                        };
                        assert_eq!(
                            keys(&live.rows),
                            keys(&direct),
                            "conn {n} q{i}: the top 20's latencies"
                        );
                    } else {
                        assert_eq!(multiset(&live.rows), multiset(&direct), "conn {n} q{i}");
                    }
                }
            }
            Ok(())
        }
        .boxed()
    })
    .await
}

/// With no warm results, a client returning after its entry is gone gets a snapshot (a new
/// entry, another lineage), which matches ClickHouse.
#[tokio::test]
async fn returning_after_the_entry_is_gone_gets_a_snapshot() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    let settings = chlive_server::ServerSettings {
        cache: chlive_server::CacheSettings {
            keep_alive: Duration::from_millis(200),
            keep_warm: Duration::ZERO,
            ..Default::default()
        },
        ..Default::default()
    };
    with_server_settings(config(), settings, |server| {
        async move {
            let producer = producer()?;
            let sql = "select ts, user_id, latency_ms from events where country = 'PL'";
            let conn = LiveConn::connect(server).await?;
            conn.subscribe("pl", sql);
            conn.wait_ready(&["pl"], Duration::from_secs(60)).await?;
            let kept = conn.result("pl").unwrap();
            drop(conn);
            // away until the entry is gone; events meanwhile
            let produced = producer.produce(300, None).await?;
            tokio::time::sleep(Duration::from_millis(600)).await;
            assert!(entries(server).await?.is_empty(), "the entry is gone");

            let conn = LiveConn::connect(server).await?;
            conn.resume(
                "pl",
                sql,
                kept.at.as_deref().expect("a cursor"),
                kept.rows.clone(),
            );
            conn.wait_ready(&["pl"], Duration::from_secs(60)).await?;
            settle(&conn, &produced).await?;
            let live = conn.result("pl").unwrap();
            assert!(!live.resumed, "a snapshot, not a resume");
            assert!(live.snapshot);
            assert_eq!(multiset(&live.rows), multiset(&direct(sql).await?));
            Ok(())
        }
        .boxed()
    })
    .await
}

/// A directory for a disk store, removed when dropped.
struct CacheDir(std::path::PathBuf);

impl CacheDir {
    fn new(name: &str) -> Self {
        let d = std::env::temp_dir().join(format!("chlive-e2e-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        Self(d)
    }
}

impl Drop for CacheDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A warm entry follows Kafka with nobody connected, its rows on disk; a returning client
/// resumes from it, and the result matches ClickHouse.
#[tokio::test]
async fn a_warm_entry_follows_kafka_on_disk_and_serves_the_next_client() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    let dir = CacheDir::new("warm");
    let settings = chlive_server::ServerSettings {
        cache: chlive_server::CacheSettings {
            dir: Some(dir.0.clone()),
            keep_alive: Duration::from_millis(200),
            keep_warm: Duration::from_secs(60),
            ..Default::default()
        },
        ..Default::default()
    };
    with_server_settings(config(), settings, |server| {
        async move {
            let producer = producer()?;
            let sql = "select ts, user_id, latency_ms from events where country = 'PL'";
            let conn = LiveConn::connect(server).await?;
            conn.subscribe("pl", sql);
            conn.wait_ready(&["pl"], Duration::from_secs(60)).await?;
            let kept = conn.result("pl").unwrap();
            drop(conn);
            tokio::time::sleep(Duration::from_millis(600)).await;
            let e = &entries(server).await?[0];
            assert_eq!(
                (&e["stage"], &e["inMemory"]),
                (&json!("warm"), &json!(false)),
                "{e}"
            );
            let produced = producer.produce(300, None).await?;
            wait_ingested(&produced).await?;

            let conn = LiveConn::connect(server).await?;
            conn.resume(
                "pl",
                sql,
                kept.at.as_deref().expect("a cursor"),
                kept.rows.clone(),
            );
            conn.wait_ready(&["pl"], Duration::from_secs(60)).await?;
            settle(&conn, &produced).await?;
            let live = conn.result("pl").unwrap();
            assert!(live.resumed, "a resume from the disk log");
            assert_eq!(multiset(&live.rows), multiset(&direct(sql).await?));
            let e = &entries(server).await?[0];
            assert_eq!(
                (&e["stage"], &e["inMemory"]),
                (&json!("live"), &json!(true)),
                "{e}"
            );
            Ok(())
        }
        .boxed()
    })
    .await
}

/// A FINAL result on disk: updated and deleted by id while live, then (the server down) more
/// versions; after the restart it's restored from disk (no ClickHouse) with its keys, versions
/// and order, caught up from Kafka, and a client from before resumes through the fork. Both it
/// and a new client end equal to ClickHouse's `SELECT … FINAL … ORDER BY`.
#[tokio::test]
async fn a_restarted_server_restores_final_results_from_disk() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    let dir = CacheDir::new("restart-final");
    let settings = chlive_server::ServerSettings {
        cache: chlive_server::CacheSettings {
            dir: Some(dir.0.clone()),
            ..Default::default()
        },
        ..Default::default()
    };
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let ids: Vec<String> = (0..8).map(|k| format!("r{run}-{k}")).collect();
    let list = ids
        .iter()
        .map(|i| format!("'{i}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql: &'static str = Box::leak(
        format!(
            "select product_id, price, version from products final \
             where product_id in ({list}) and price > 20 order by price desc"
        )
        .into_boxed_str(),
    );
    // each product: versions that only grow, prices in and out of the WHERE, some deleted
    let versions = move |round: u64| -> Vec<(String, String)> {
        ids.iter()
            .enumerate()
            .map(|(k, id)| {
                let n = round * 8 + k as u64;
                let price = (n * 37) % 50;
                let deleted = u8::from(n % 5 == 4);
                (
                    id.clone(),
                    format!(
                        r#"{{"product_id":"{id}","name":"n","price":{price},"version":{round},"deleted":{deleted}}}"#
                    ),
                )
            })
            .collect()
    };
    let products = producer::Producer::new(&config().kafka_brokers, "products")?;
    let ingested = |produced: std::collections::BTreeMap<i32, i64>| async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        loop {
            let have = ingested_from("products").await?;
            if produced
                .iter()
                .all(|(p, o)| have.get(p).is_some_and(|h| h >= o))
            {
                return anyhow::Ok(());
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "products not ingested: {have:?}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    for round in 1..=2 {
        ingested(products.produce_keyed(&versions(round)).await?).await?;
    }
    let kept = std::sync::Arc::new(std::sync::Mutex::new(None));
    let k = kept.clone();
    let live_versions: Vec<_> = (3..=4).map(&versions).collect();
    with_server_settings(config(), settings.clone(), move |server| {
        async move {
            let conn = LiveConn::connect(server).await?;
            conn.subscribe("f", sql);
            conn.wait_ready(&["f"], Duration::from_secs(60)).await?;
            // live: updates, moves and deletes by id, into the stored log
            let products = producer::Producer::new(&config().kafka_brokers, "products")?;
            for v in &live_versions {
                products.produce_keyed(v).await?;
            }
            conn.wait_quiet(Duration::from_millis(1000), Duration::from_secs(60))
                .await?;
            let live = conn.result("f").unwrap();
            assert!(live.updated > 0 && live.deleted > 0, "{live:?}");
            *k.lock().unwrap() = Some(live);
            Ok(())
        }
        .boxed()
    })
    .await?;
    // while the server is down
    let mut produced = std::collections::BTreeMap::new();
    for round in 5..=7 {
        for (p, o) in products.produce_keyed(&versions(round)).await? {
            let e = produced.entry(p).or_insert(o);
            *e = (*e).max(o);
        }
    }
    ingested(produced).await?;
    let kept = kept.lock().unwrap().clone().unwrap();
    with_server_settings(config(), settings, move |server| {
        async move {
            let e = &entries(server).await?;
            assert_eq!(e.len(), 1, "{e:?}");
            assert_eq!(e[0]["stage"], "cold");
            let conn = LiveConn::connect(server).await?;
            conn.resume_kept("f", sql, &kept);
            conn.wait_ready(&["f"], Duration::from_secs(60)).await?;
            conn.subscribe("fresh", sql);
            conn.wait_ready(&["fresh"], Duration::from_secs(60)).await?;
            conn.wait_quiet(Duration::from_millis(1000), Duration::from_secs(60))
                .await?;
            let direct = clickhouse()
                .fetch(&sql.replace("from products", "from default.products"))
                .await?;
            for id in ["f", "fresh"] {
                let live = conn.result(id).unwrap();
                assert!(live.error.is_none(), "{id}: {:?}", live.error);
                assert!(
                    live.protocol_violations.is_empty(),
                    "{id}: {:?}",
                    live.protocol_violations
                );
                assert!(
                    live.ids.iter().all(Option::is_some),
                    "{id}: rows without ids"
                );
                assert_eq!(
                    tie_groups(&live.rows, &["price"]),
                    tie_groups(&direct, &["price"]),
                    "{id}"
                );
            }
            assert!(
                conn.result("f").unwrap().resumed,
                "resumed through the fork"
            );
            let e = &entries(server).await?[0];
            assert_eq!(e["restored"], true, "no ClickHouse: {e}");
            Ok(())
        }
        .boxed()
    })
    .await
}

/// After a restart, a result is restored from disk (no ClickHouse), caught up from Kafka, and
/// a client from before the restart resumes through the fork; it matches ClickHouse.
#[tokio::test]
async fn a_restarted_server_restores_results_from_disk() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    let dir = CacheDir::new("restart");
    let settings = chlive_server::ServerSettings {
        cache: chlive_server::CacheSettings {
            dir: Some(dir.0.clone()),
            ..Default::default()
        },
        ..Default::default()
    };
    let sql = "select user_id, latency_ms from events where country = 'DE' order by latency_ms desc, user_id limit 20";
    let kept = std::sync::Arc::new(std::sync::Mutex::new(None));
    let k = kept.clone();
    with_server_settings(config(), settings.clone(), move |server| {
        async move {
            let conn = LiveConn::connect(server).await?;
            conn.subscribe("de", sql);
            conn.wait_ready(&["de"], Duration::from_secs(60)).await?;
            *k.lock().unwrap() = conn.result("de");
            Ok(())
        }
        .boxed()
    })
    .await?;
    // while the server is down
    let produced = producer()?.produce(300, None).await?;
    wait_ingested(&produced).await?;
    let kept = kept.lock().unwrap().clone().unwrap();
    with_server_settings(config(), settings, move |server| {
        async move {
            let e = &entries(server).await?;
            assert_eq!(e.len(), 1, "{e:?}");
            assert_eq!(e[0]["stage"], "cold");
            let conn = LiveConn::connect(server).await?;
            conn.resume(
                "de",
                sql,
                kept.at.as_deref().expect("a cursor"),
                kept.rows.clone(),
            );
            conn.wait_ready(&["de"], Duration::from_secs(60)).await?;
            settle(&conn, &produced).await?;
            let live = conn.result("de").unwrap();
            assert!(live.resumed, "resumed through the fork");
            assert!(
                live.protocol_violations.is_empty(),
                "{:?}",
                live.protocol_violations
            );
            let latencies = |rows: &[Row]| -> Vec<String> {
                rows.iter().map(|r| r["latency_ms"].to_string()).collect()
            };
            assert_eq!(latencies(&live.rows), latencies(&direct(sql).await?));
            let e = &entries(server).await?[0];
            assert_eq!(e["restored"], true, "no ClickHouse: {e}");
            assert!(e.get("historyMs").is_some());
            Ok(())
        }
        .boxed()
    })
    .await
}
