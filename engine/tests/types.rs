//! Every column type against a real ClickHouse (CHLIVE_CLICKHOUSE_URL, which the `e2e` script
//! sets); runs only with CHLIVE_E2E=1.
//!
//! For each type, sample event values go through ClickHouse's `JSONEachRow` input and the
//! engine's `decode`: both must accept the same ones and render them the same way. The
//! accepted ones must sort the same under every ORDER BY direction and NULLS placement, and
//! compare the same with sample literals (rendered by `to_clickhouse_sql`). Literals the engine
//! finds invalid must fail in ClickHouse too.

use std::cmp::Ordering;

use chlive_engine::ast::Literal;
use chlive_engine::types::{decode, quote_string, Kind, LiteralError, Type, Value};
use serde_json::{json, Value as Json};

fn url() -> String {
    std::env::var("CHLIVE_CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:18123".into())
}

/// The rows of a query, read with the history side's settings; `Err` with ClickHouse's message.
async fn rows(sql: &str) -> Result<Vec<Json>, String> {
    let resp = reqwest::Client::new()
        .post(url())
        .header("X-ClickHouse-User", "default")
        .query(&[
            ("default_format", "JSONEachRow"),
            ("output_format_json_quote_64bit_integers", "0"),
            ("output_format_json_quote_denormals", "1"),
        ])
        .body(sql.to_string())
        .send()
        .await
        .expect("ClickHouse reachable (is `dnvr up` running?)");
    let ok = resp.status().is_success();
    let text = resp.text().await.unwrap();
    if !ok {
        return Err(text.lines().next().unwrap_or_default().to_string());
    }
    Ok(text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect())
}

/// `format(JSONEachRow, …)` over rows `{"i": n, "v": value}`.
fn source(ty: &str, values: &[&Json]) -> String {
    let data: Vec<String> = values
        .iter()
        .enumerate()
        .map(|(i, v)| json!({"i": i, "v": v}).to_string())
        .collect();
    format!(
        "format(JSONEachRow, 'i UInt32, v {ty}', {})",
        quote_string(&data.join("\n"))
    )
}

struct Cases {
    /// ClickHouse type names of this kind, each checked on its own.
    types: &'static [&'static str],
    /// Event values; `null` is added for Nullable types.
    inputs: Vec<Json>,
    literals: Vec<Literal>,
}

/// One arm per kind, so a new type doesn't compile without cases.
fn cases(kind: &Kind) -> Cases {
    let s = |v: &str| Literal::String(v.into());
    let n = |v: &str| Literal::Number(v.into());
    match kind {
        Kind::Int(_) => Cases {
            types: &[
                "UInt8",
                "Int8",
                "UInt32",
                "Int64",
                "UInt64",
                "Nullable(Int32)",
            ],
            inputs: vec![
                json!(0),
                json!(1),
                json!(-1),
                json!(127),
                json!(128),
                json!(255),
                json!(256),
                json!(-129),
                json!(4294967296u64),
                json!(u64::MAX),
                json!("42"),
                json!(" 42"),
                json!("false"),
                json!("256"),
                json!("-1"),
                json!("-129"),
                json!("abc"),
                json!(true),
                json!(1.5),
            ],
            literals: vec![n("5"), n("-1"), n("127"), n("2.5")],
        },
        Kind::Float(_) => Cases {
            types: &["Float64", "Nullable(Float64)"],
            inputs: vec![
                json!(0),
                json!(1.5),
                json!(-2.25),
                json!(1e300),
                json!("2.5"),
                json!("abc"),
                json!(true),
                json!(false),
                json!("true"),
                json!("nan"),
                json!("inf"),
                json!("-inf"),
            ],
            literals: vec![n("1.5"), n("0"), n("-3")],
        },
        Kind::String(_) => Cases {
            types: &["String", "Nullable(String)"],
            inputs: vec![
                json!(""),
                json!("a"),
                json!("B"),
                json!("aa"),
                json!("é"),
                json!("it's"),
                json!(7),
                json!(true),
            ],
            literals: vec![s("a"), s(""), s("é")],
        },
        Kind::DateTime(_) => Cases {
            types: &["DateTime", "DateTime64(3)", "Nullable(DateTime64(6))"],
            inputs: vec![
                json!("2026-10-01 12:00:00"),
                json!("2026-10-01 12:00:01.5"),
                json!("2026-10-01 12:00:01.123456789"),
                json!("1970-01-01 00:00:00"),
                json!("2026-10-01"),
                json!(1_700_000_000),
                json!(-5),
                json!(4_294_967_296u64),
                json!(1.5),
                // Known differences, which the engine rejects (skipping the event) while
                // ClickHouse reads them: digit strings, `"1700000000"` as Unix seconds
                // (`"1700000000.5"` too for DateTime64) and `"2026"` as 2026-01-01, and other
                // partial dates such as `" 7"` (the 7th of January this year).
                json!("2026-13-01 00:00:00"),
                json!("yesterday"),
            ],
            literals: vec![s("2026-10-01 12:00:00"), s("2026-10-01"), s("soon")],
        },
        Kind::Date(_) => Cases {
            types: &["Date", "Nullable(Date)"],
            inputs: vec![
                json!("2026-10-01"),
                json!("2024-02-29"),
                json!("1970-01-01"),
                json!("2100-01-01"),
                json!("x"),
            ],
            literals: vec![s("2026-10-01"), s("bad")],
        },
        Kind::Bool(_) => Cases {
            types: &["Bool", "Nullable(Bool)"],
            inputs: vec![json!(true), json!(false), json!(0), json!(1), json!(2)],
            literals: vec![Literal::Bool(true), Literal::Bool(false)],
        },
        Kind::Uuid(_) => Cases {
            types: &["UUID", "Nullable(UUID)"],
            inputs: vec![
                json!("abcdef00-0000-0000-0000-000000000001"),
                json!("ABCDEF00-0000-0000-0000-000000000002"),
                json!("abcdef00000000000000000000000003"),
                json!("ffffffff-ffff-ffff-0000-000000000000"),
                json!("00000000-0000-0001-0000-000000000001"),
                json!("00000000-0000-0002-0000-000000000001"),
                json!("00000000-0000-0000-0000-000000000000"),
                json!("{abcdef00-0000-0000-0000-000000000001}"),
                json!(" abcdef00-0000-0000-0000-000000000001"),
                json!("abcdef00-0000-0000-0000-00000000000"),
                json!(""),
                json!(123),
            ],
            literals: vec![
                s("ABCDEF00-0000-0000-0000-000000000001"),
                s("00000000-0000-0001-0000-000000000001"),
                s("nope"),
            ],
        },
        Kind::Other(_) => Cases {
            types: &["Array(String)"],
            inputs: vec![json!(["a", "b"]), json!([])],
            literals: vec![],
        },
    }
}

/// One type of each kind, to find every kind's cases.
const KINDS: &[&str] = &[
    "Int32",
    "Float64",
    "String",
    "DateTime",
    "Date",
    "Bool",
    "UUID",
    "Array(String)",
];

/// A literal as ClickHouse SQL text, for checking that ClickHouse rejects it too.
fn literal_sql(lit: &Literal) -> String {
    match lit {
        Literal::Number(n) => n.clone(),
        Literal::String(s) => quote_string(s),
        Literal::Bool(b) => b.to_string(),
        Literal::Null => "NULL".into(),
        Literal::Param { .. } | Literal::Bound { .. } => unreachable!("no parameters here"),
    }
}

async fn check(name: &str, inputs: &[Json], literals: &[Literal], problems: &mut Vec<String>) {
    let ty = Type::parse(name);
    let mut inputs = inputs.to_vec();
    if ty.nullable {
        inputs.push(Json::Null);
    }

    // decoding: accepted alike, rendered alike
    let mut accepted: Vec<(Json, Value)> = Vec::new();
    for input in &inputs {
        let theirs = rows(&format!("SELECT v FROM {}", source(name, &[input])))
            .await
            .map(|r| r[0]["v"].clone());
        let ours = decode(Some(input), &ty);
        match (&theirs, &ours) {
            (Ok(t), Ok(o)) if *t == o.to_json() => accepted.push((input.clone(), o.clone())),
            (Err(_), Err(_)) => {}
            _ => problems.push(format!(
                "{name} {input}: ClickHouse {theirs:?}, engine {:?}",
                ours.as_ref().map(Value::to_json)
            )),
        }
    }
    let values: Vec<&Json> = accepted.iter().map(|(i, _)| i).collect();

    // ordering
    if ty.ops().order && !values.is_empty() {
        for (desc, nulls_first) in [(false, false), (true, false), (false, true), (true, true)] {
            let dir = if desc { "DESC" } else { "ASC" };
            let nulls = if nulls_first { "FIRST" } else { "LAST" };
            let theirs: Vec<Json> = rows(&format!(
                "SELECT v FROM {} ORDER BY v {dir} NULLS {nulls}",
                source(name, &values)
            ))
            .await
            .unwrap()
            .into_iter()
            .map(|r| r["v"].clone())
            .collect();
            let mut ours: Vec<&Value> = accepted.iter().map(|(_, v)| v).collect();
            ours.sort_by(|a, b| a.sort_cmp(b, desc, nulls_first));
            let ours: Vec<Json> = ours.iter().map(|v| v.to_json()).collect();
            if theirs != ours {
                problems.push(format!(
                    "{name} ORDER BY v {dir} NULLS {nulls}: ClickHouse {theirs:?}, engine {ours:?}"
                ));
            }
        }
    }

    // literals: the same truth for = and <, and the same rejections
    for lit in literals {
        match ty.literal(lit) {
            Ok(l) => {
                if values.is_empty() {
                    continue;
                }
                let sql = l.to_clickhouse_sql();
                let theirs = match rows(&format!(
                    "SELECT i, v = {sql} AS eq, v < {sql} AS lt FROM {} ORDER BY i",
                    source(name, &values)
                ))
                .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        problems.push(format!("{name} compared with {sql}: ClickHouse {e}"));
                        continue;
                    }
                };
                for (row, (input, v)) in theirs.iter().zip(&accepted) {
                    let truth = |b: bool| json!(u8::from(b));
                    let ours = match v.compare(&l) {
                        _ if *v == Value::Null => (Json::Null, Json::Null),
                        c => (
                            truth(c == Some(Ordering::Equal)),
                            truth(c == Some(Ordering::Less)),
                        ),
                    };
                    if (row["eq"].clone(), row["lt"].clone()) != ours {
                        problems.push(format!(
                            "{name} {input} vs {sql}: ClickHouse (=, <) {}, {}; engine {}, {}",
                            row["eq"], row["lt"], ours.0, ours.1
                        ));
                    }
                }
            }
            Err(LiteralError::Invalid(message)) => {
                // to the bare type: a failed cast to Nullable(T) is NULL, not an error
                let bare = name
                    .strip_prefix("Nullable(")
                    .and_then(|t| t.strip_suffix(')'))
                    .unwrap_or(name);
                let sql = format!("SELECT CAST({} AS {bare}) AS v", literal_sql(lit));
                if let Ok(r) = rows(&sql).await {
                    problems.push(format!(
                        "{name} literal {}: engine rejects it ({message}), ClickHouse reads {}",
                        literal_sql(lit),
                        r[0]["v"]
                    ));
                }
            }
            // the dialect's own rule (e.g. no strings for numbers), stricter than ClickHouse
            Err(LiteralError::Mismatch) => {}
        }
    }
}

#[tokio::test]
async fn types_match_clickhouse() {
    if std::env::var_os("CHLIVE_E2E").is_none() {
        eprintln!("skipped: set CHLIVE_E2E=1 with `dnvr up` running");
        return;
    }
    let mut problems = Vec::new();
    for kind in KINDS {
        let Cases {
            types,
            inputs,
            literals,
        } = cases(&Type::parse(kind).kind);
        for name in types {
            check(name, &inputs, &literals, &mut problems).await;
        }
    }
    assert!(
        problems.is_empty(),
        "{} differences from ClickHouse:\n{}",
        problems.len(),
        problems.join("\n")
    );
}
