use std::collections::{BTreeMap, HashMap};

use super::*;
use crate::watermark::Watermark;

pub fn catalog() -> Catalog {
    let cols = [
        ("ts", "DateTime64(3)"),
        ("user_id", "String"),
        ("country", "LowCardinality(String)"),
        ("path", "String"),
        ("latency_ms", "UInt32"),
        ("amount", "Float64"),
        ("_partition", "UInt64"),
        ("_offset", "UInt64"),
    ];
    Catalog {
        database: "default".into(),
        tables: HashMap::from([(
            "events".to_string(),
            TableSchema {
                name: "events".into(),
                columns: cols
                    .iter()
                    .map(|(a, b)| (a.to_string(), b.to_string()))
                    .collect(),
            },
        )]),
        functions: FunctionCatalog::builtin(),
        rows_limit_cap: 1000,
        source_template: "remote(main_ro, database = '{database}', table = '{table}')".into(),
    }
}

fn w() -> Watermark {
    Watermark(BTreeMap::from([(0, 120), (1, 98), (2, -1)]))
}

/// Golden files live in `src/rewriter/golden/`. Regenerate with `UPDATE_GOLDEN=1 cargo test`.
fn golden(name: &str, actual: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/rewriter/golden")
        .join(name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "missing golden file {}; run with UPDATE_GOLDEN=1",
            path.display()
        )
    });
    assert_eq!(actual, expected, "golden mismatch for {name}");
}

fn render(sql: &str, opts: &Options) -> String {
    let p = rewrite_with(sql, &catalog(), opts).unwrap();
    format!(
        "-- input\n{sql}\n-- kind: {:?}\n-- key_columns: {:?}\n-- output_columns: {:?}\n-- params: {:?}\n\
         -- history_sql\n{}\n-- tail_state_sql\n{}\n-- merge_sql\n{}\n",
        p.kind,
        p.key_columns,
        p.output_columns,
        p.param_types,
        p.history_sql(&w()),
        p.tail_sql(&w()),
        p.merge_sql(&w(), "sub_1_hist"),
    )
}

#[test]
fn golden_examples() {
    for (i, sql) in EXAMPLES.iter().enumerate() {
        golden(
            &format!("example_{}.sql", i + 1),
            &render(sql, &Options::default()),
        );
    }
}

#[test]
fn golden_example_5_without_alias_expansion() {
    let opts = Options {
        expand_aliases: false,
    };
    golden("example_5_no_expand.sql", &render(EXAMPLES[4], &opts));
}

#[test]
fn plan_kinds_and_keys() {
    let c = catalog();
    let kinds: Vec<_> = EXAMPLES
        .iter()
        .map(|s| rewrite(s, &c).unwrap().kind)
        .collect();
    assert_eq!(
        kinds,
        [
            PlanKind::Rows,
            PlanKind::Aggregate,
            PlanKind::Aggregate,
            PlanKind::Aggregate,
            PlanKind::Aggregate,
            PlanKind::Aggregate
        ]
    );
    let p = rewrite(EXAMPLES[1], &c).unwrap();
    assert!(p.key_columns.is_empty());
    assert!(!p.has_order);
    assert_eq!(p.result_cap, Some(1000));
    let p = rewrite(EXAMPLES[3], &c).unwrap();
    assert_eq!(p.param_types["since"], "DateTime64(3)");
}

#[test]
fn deduplicates_identical_aggregates() {
    let p = rewrite(
        "SELECT count() AS a, count() * 2 AS b, COUNT(*) AS c FROM events",
        &catalog(),
    )
    .unwrap();
    let h = p.history_sql(&w());
    assert_eq!(h.matches("countState()").count(), 1, "{h}");
}

#[test]
fn unaliased_group_key_is_hidden() {
    let p = rewrite(
        "SELECT toStartOfHour(ts), count() FROM events GROUP BY toStartOfHour(ts) ORDER BY toStartOfHour(ts)",
        &catalog(),
    )
    .unwrap();
    assert_eq!(p.key_columns, ["__key_0"]);
    assert_eq!(p.output_columns, ["toStartOfHour(ts)", "count()"]);
    let m = p.merge_sql(&w(), "h");
    assert!(
        m.starts_with(
            "SELECT __key_0 AS `toStartOfHour(ts)`, countMerge(__agg_0) AS `count()`, __key_0 FROM"
        ),
        "{m}"
    );
    assert!(p.merge_sql(&w(), "h").contains("ORDER BY __key_0"));
}

#[test]
fn group_by_without_aggregates_and_hidden_key() {
    let p = rewrite(
        "SELECT count() AS n FROM events GROUP BY country",
        &catalog(),
    )
    .unwrap();
    assert_eq!(p.key_columns, ["country"]);
    assert_eq!(p.output_columns, ["n"]);
    assert!(p
        .merge_sql(&w(), "h")
        .starts_with("SELECT countMerge(__agg_0) AS n, country FROM"));
}

#[test]
fn rows_select_star_and_params() {
    let p = rewrite(
        "SELECT * FROM events WHERE path = {p:String} ORDER BY latency_ms DESC LIMIT 5",
        &catalog(),
    )
    .unwrap();
    assert_eq!(p.output_columns.len(), 8);
    assert!(p
        .history_sql(&w())
        .contains("WHERE (path = {p:String}) AND"));
}

fn reject(sql: &str) -> RewriteError {
    match rewrite(sql, &catalog()) {
        Ok(p) => panic!("expected rejection for {sql}, got {p:?}"),
        Err(e) => e,
    }
}

#[test]
fn rejections_from_spec() {
    let e = reject("SELECT count() FROM events WHERE ts > now() - INTERVAL 1 HOUR");
    assert_eq!(e.code, "UNSUPPORTED_QUERY");
    assert!(
        e.message.contains("now() is not allowed") && e.message.contains("{since:DateTime64(3)}"),
        "{e}"
    );

    let e = reject("SELECT e.country, d.name FROM events e JOIN countries d ON e.country = d.code");
    assert!(e.message.contains("joins"), "{e}");

    let e = reject("SELECT * FROM events");
    assert!(e.message.contains("ORDER BY and LIMIT"), "{e}");

    let e = reject("SELECT count() OVER () FROM events");
    assert!(e.message.contains("window functions"), "{e}");
}

#[test]
fn other_rejections() {
    let cases = [
        ("SELECT count() FROM events FINAL", "FINAL"),
        ("SELECT count() FROM (SELECT * FROM events)", "subqueries"),
        (
            "SELECT count() FROM events WHERE user_id IN (SELECT user_id FROM events)",
            "subqueries",
        ),
        (
            "SELECT count() FROM events UNION ALL SELECT count() FROM events",
            "UNION",
        ),
        ("WITH t AS (SELECT 1) SELECT count() FROM events", "WITH"),
        (
            "SELECT count() FROM events SETTINGS max_threads = 1",
            "SETTINGS",
        ),
        ("SELECT count() FROM events FORMAT JSON", "FORMAT"),
        ("SELECT * FROM events ARRAY JOIN arr", "ARRAY JOIN"),
        (
            "SELECT count() FROM events WHERE rand() > 5",
            "deterministic",
        ),
        (
            "SELECT generateUUIDv4() AS u, ts FROM events ORDER BY ts LIMIT 1",
            "deterministic",
        ),
        (
            "SELECT count() FROM events WHERE ts > today()",
            "today() is not allowed",
        ),
        (
            "SELECT anyLast(path) FROM events",
            "'anyLast' is not supported",
        ),
        (
            "SELECT uniqState(user_id) FROM events",
            "'uniqState' is not supported",
        ),
        ("SELECT count() FROM other", "not allowlisted"),
        (
            "SELECT count() FROM remote('x', default, events)",
            "table functions",
        ),
        (
            "SELECT ts FROM events ORDER BY ts LIMIT 5000",
            "exceeds the maximum",
        ),
        ("SELECT ts FROM events LIMIT 5", "ORDER BY and LIMIT"),
        (
            "SELECT country, count() FROM events GROUP BY country LIMIT 2000",
            "exceeds",
        ),
        ("SELECT sum(count()) FROM events", "nested"),
        ("SELECT count(DISTINCT user_id) FROM events", "uniqExact"),
        ("SELECT count() FROM events WHERE sum(amount) > 1", "HAVING"),
        (
            "SELECT max(latency_ms) AS latency_ms FROM events",
            "shadows",
        ),
        ("SELECT DISTINCT country FROM events", "DISTINCT"),
        (
            "SELECT ts FROM events ORDER BY ts LIMIT 5 OFFSET 5",
            "OFFSET",
        ),
        ("SELECT __agg_0 FROM events", "reserved"),
        (
            "SELECT country, count() FROM events GROUP BY 1",
            "positional",
        ),
        (
            "SELECT country, count() FROM events GROUP BY ALL",
            "GROUP BY ALL",
        ),
        (
            "SELECT country, count() FROM events GROUP BY country WITH TOTALS",
            "modifiers",
        ),
        ("SELECT count() FROM events PREWHERE amount > 0", "PREWHERE"),
        (
            "SELECT events.country FROM events ORDER BY ts LIMIT 1",
            "qualified",
        ),
        (
            "SELECT {a:String} AS x, {a:UInt8} AS y FROM events ORDER BY ts LIMIT 1",
            "two types",
        ),
        ("INSERT INTO events VALUES (1)", "only SELECT"),
        ("SELECT 1; SELECT 2", "exactly one"),
        ("SELEC 1", "Expected"),
    ];
    for (sql, needle) in cases {
        let e = reject(sql);
        assert!(
            e.message.contains(needle),
            "{sql}\n  -> {e}\n  expected to contain {needle:?}"
        );
    }
}

pub const EXAMPLES: &[&str] = &[
    "SELECT ts, user_id, country, path, latency_ms FROM events ORDER BY ts DESC LIMIT 20",
    "SELECT count() AS total, avg(latency_ms) AS avg_latency, sum(amount) AS revenue FROM events",
    "SELECT country, count() AS n FROM events WHERE path = {path:String} GROUP BY country ORDER BY n DESC LIMIT 10",
    "SELECT toStartOfMinute(ts) AS minute, count() AS requests, quantile(0.95)(latency_ms) AS p95, quantiles(0.5, 0.99)(latency_ms) AS p50_p99 FROM events WHERE ts >= {since:DateTime64(3)} GROUP BY minute ORDER BY minute",
    "SELECT country, uniq(user_id) AS users, sum(amount) / count() AS avg_order_value, countIf(amount > 100) AS big_orders, big_orders / count() AS big_order_ratio FROM events WHERE amount > 0 GROUP BY country HAVING users > 50 ORDER BY users DESC LIMIT 5",
    "SELECT path, max(latency_ms) AS worst_ms, argMax(user_id, latency_ms) AS worst_user FROM events GROUP BY path ORDER BY worst_ms DESC LIMIT 10",
];
