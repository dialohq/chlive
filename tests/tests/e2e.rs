//! End to end over the full stack: what a client sees over the WebSocket equals the same query
//! run directly on ClickHouse once everything is ingested.

use std::time::Duration;

use anyhow::Result;
use chlive_e2e::*;
use chlive_engine::history::HistorySource;

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
];

#[tokio::test]
async fn live_results_match_direct_queries() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    let server = start_server().await?;
    let producer = producer()?;
    let conn = LiveConn::connect(&server).await?;

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

    // a fresh subscription's snapshot is the whole result, with nothing left to stream
    let fresh = LiveConn::connect(&server).await?;
    fresh.subscribe("slow", CASES[1].1);
    fresh.wait_ready(&["slow"], Duration::from_secs(60)).await?;
    fresh
        .wait_quiet(Duration::from_millis(500), Duration::from_secs(30))
        .await?;
    let r = fresh.result("slow").unwrap();
    assert_eq!(r.inserted, 0);
    assert_eq!(
        multiset(&r.rows),
        multiset(&conn.result("slow").unwrap().rows)
    );
    Ok(())
}

#[tokio::test]
async fn rejections_over_websocket() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    let server = start_server().await?;
    let conn = LiveConn::connect(&server).await?;
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

#[tokio::test]
async fn unsubscribe_stops_the_stream() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    let server = start_server().await?;
    let producer = producer()?;
    let conn = LiveConn::connect(&server).await?;
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

/// LIMIT: never more rows than the limit (checked after every diff by the harness).
/// - With ORDER BY, the same tie groups as ClickHouse's top N, except which rows of the last
///   (boundary) group made it in, which is arbitrary in ClickHouse too.
/// - Without, a window: full, every row matching, the streamed rows last.
fn check_limited(id: &str, sql: &str, keys: &[&str], live: &LiveResult, direct: &[Row]) {
    let limit: usize = sql
        .rsplit(" limit ")
        .next()
        .unwrap()
        .trim()
        .parse()
        .unwrap();
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
    let server = start_server().await?;
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
    let conn = LiveConn::connect(&server).await?;
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
    let conn = LiveConn::connect(&server).await?;
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

/// GROUP BY over the full stack (chDB in the server): after events arrive live, the result
/// built from the snapshot plus inserts and updates equals ClickHouse's GROUP BY.
#[tokio::test]
async fn live_aggregates_match_direct_queries() -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    let _guard = exclusive().await;
    init_tracing();
    let server = start_server().await?;
    let producer = producer()?;
    const QUERIES: &[(&str, &str)] = &[
        ("total", "select count() as n, sum(amount), avg(latency_ms) from events"),
        ("by_country", "select country, count() as n, avg(latency_ms) as avg_latency, max(latency_ms) from events group by country"),
        ("pl_paths", "select path, count(), min(ts), max(amount) from events where country = 'PL' group by path"),
    ];
    let conn = LiveConn::connect(&server).await?;
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
        assert_eq!(canon(&live.rows), canon(&direct), "{id}");
    }
    drop(conn);
    server.stop();
    // the only test here using chDB: close it, or the test process crashes on exit
    tokio::time::sleep(Duration::from_millis(200)).await;
    chlive_engine::chdb::shutdown();
    Ok(())
}
