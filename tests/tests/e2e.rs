//! End to end over the full stack: what a client sees over the WebSocket equals the same query
//! run directly on ClickHouse once everything is ingested.

use std::time::Duration;

use anyhow::Result;
use chlive_e2e::*;
use chlive_engine::history::HistorySource;
use futures::FutureExt;

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
                    "unknown table nope in ClickHouse (default.nope) at 1:15",
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
            "SELECT name FROM system.tables WHERE database = 'default' \
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
    with_server(config, |server| {
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
