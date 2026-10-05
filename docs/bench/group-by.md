# GROUP BY: native states vs chDB, what it costs

Measured on 2026-10-05 on the dev machine (Apple Silicon, release build), ClickHouse 26.7.2 and chDB 26.7.3 (`engine/examples/agg_bench.rs`; reproduce with `dnvr up`, then `cargo run --release -p chlive-engine --example agg_bench`).

The engine runs a query's aggregates in one of two ways:

- **Native** (when every aggregate has a native implementation: `count`, `sum`, `avg`, `min`, `max`): ClickHouse computes the parts of each group's state over HTTP (e.g. `sum(x)` and `count(x)` for `avg(x)`), and the engine keeps the states in Rust and updates them per event (`engine/src/aggregate/`).
- **chDB** (the fallback, e.g. for `uniqExact` or `median`): main ClickHouse computes the history's aggregate states (`countState()`, …) with `remote()` into a chDB table in the engine process; each batch of events adds its states (`INSERT … SELECT …State() FROM format(JSONEachRow, …)`), and the touched groups are read back finalized (`…Merge`). `Aggregates::ChdbOnly` forces it for any query, which is how sections 3 and 4 measure both.

Section 1 compares chDB's raw per-batch cost with a hand-rolled `HashMap` of (count, sum) per group: the floor for any native implementation. Section 3 is the real thing, through the engine, both ways (native over 1M events; chDB over 5k one at a time, 200k batched).

## 1. chDB per batch vs hand-rolled native (aggregation only)

| query | groups | batch | chDB insert | chDB read | chDB per event | chDB events/s | native per event | native events/s | slowdown |
|---|---|---|---|---|---|---|---|---|---|
| count() | 1 | 1 | 1.26 ms | 570.7 µs | 1.83 ms | 545 /s | 166 ns | 6.0 M/s | 11037× |
| count() | 1 | 10 | 1.28 ms | 564.8 µs | 184.9 µs | 5.4 k/s | 34 ns | 29.4 M/s | 5436× |
| count() | 1 | 100 | 1.24 ms | 567.5 µs | 18.0 µs | 55.4 k/s | 21 ns | 47.0 M/s | 848× |
| count() | 1 | 1000 | 1.37 ms | 569.4 µs | 1.9 µs | 516.0 k/s | 21 ns | 48.6 M/s | 94× |
| avg(x) | 1 | 1 | 1.21 ms | 539.0 µs | 1.75 ms | 572 /s | 204 ns | 4.9 M/s | 8579× |
| avg(x) | 1 | 10 | 1.19 ms | 530.0 µs | 172.1 µs | 5.8 k/s | 38 ns | 26.2 M/s | 4513× |
| avg(x) | 1 | 100 | 1.18 ms | 541.8 µs | 17.2 µs | 58.0 k/s | 22 ns | 45.8 M/s | 790× |
| avg(x) | 1 | 1000 | 1.55 ms | 553.7 µs | 2.1 µs | 476.4 k/s | 21 ns | 48.0 M/s | 101× |
| k, count(), avg(x) | 10 | 1 | 1.59 ms | 1.16 ms | 2.75 ms | 363 /s | 212 ns | 4.7 M/s | 12995× |
| k, count(), avg(x) | 10 | 10 | 1.64 ms | 1.34 ms | 298.3 µs | 3.4 k/s | 208 ns | 4.8 M/s | 1435× |
| k, count(), avg(x) | 10 | 100 | 2.07 ms | 1.42 ms | 34.9 µs | 28.7 k/s | 40 ns | 25.0 M/s | 873× |
| k, count(), avg(x) | 10 | 1000 | 1.82 ms | 1.66 ms | 3.5 µs | 287.4 k/s | 23 ns | 43.8 M/s | 152× |
| k, count(), avg(x) | 10000 | 1 | 1.70 ms | 1.35 ms | 3.05 ms | 328 /s | 209 ns | 4.8 M/s | 14567× |
| k, count(), avg(x) | 10000 | 10 | 1.72 ms | 1.67 ms | 339.0 µs | 2.9 k/s | 199 ns | 5.0 M/s | 1699× |
| k, count(), avg(x) | 10000 | 100 | 1.66 ms | 1.64 ms | 33.0 µs | 30.3 k/s | 235 ns | 4.3 M/s | 141× |
| k, count(), avg(x) | 10000 | 1000 | 2.23 ms | 2.68 ms | 4.9 µs | 203.5 k/s | 284 ns | 3.5 M/s | 17× |

## 3. End to end through the engine (decode, filter, aggregate, diff)

| query | max batch | native events/s | native per event | chDB events/s | chDB per event | native vs chDB | diffs |
|---|---|---|---|---|---|---|---|
| `select k10, x from events where x > 500` | 1000 | 3.0 M/s | 339 ns | 3.2 M/s | 313 ns | 1× | 499000 / 99800 |
| `select count() from events` | 1 | 1.7 M/s | 582 ns | 526 /s | 1.90 ms | 3264× | 1000000 / 5000 |
| `select count() from events` | 1000 | 7.6 M/s | 132 ns | 458.0 k/s | 2.2 µs | 17× | 1000 / 200 |
| `select k10, count(), avg(x) from events group by k10` | 1 | 871.6 k/s | 1.1 µs | 354 /s | 2.83 ms | 2465× | 1000000 / 5000 |
| `select k10, count(), avg(x) from events group by k10` | 1000 | 3.0 M/s | 335 ns | 237.8 k/s | 4.2 µs | 13× | 10000 / 2000 |
| `select k10k, count(), avg(x) from events group by k10k` | 1 | 882.8 k/s | 1.1 µs | 326 /s | 3.07 ms | 2709× | 1000000 / 5000 |
| `select k10k, count(), avg(x) from events group by k10k` | 1000 | 991.0 k/s | 1.0 µs | 163.9 k/s | 6.1 µs | 6× | 1000000 / 200000 |
| `select k10k, count(), sum(x), min(x), max(x) from events group by k10k` | 1000 | 722.3 k/s | 1.4 µs | 139.4 k/s | 7.2 µs | 5× | 1000000 / 200000 |

## 4. Subscribing over 1000000 history rows

| query | native: states over HTTP + snapshot | chDB: states via remote() + snapshot | ClickHouse GROUP BY directly |
|---|---|---|---|
| `select count() from events` | 6 ms | 11 ms | 1 ms |
| `select k10, count(), avg(x) from events group by k10` | 7 ms | 13 ms | 3 ms |
| `select k10k, count(), avg(x) from events group by k10k` | 23 ms | 28 ms | 8 ms |

## Takeaways

- **chDB costs about 2–5 ms per batch, whatever its size** (~1.2–2 ms to insert the batch's states, ~0.5–2.7 ms to read the touched groups back). One event at a time that's ~330–550 events/s per subscription.
- **Native, through the whole engine** (decode, filter, aggregate, finalize, diff): **0.9–1.7 M events/s one at a time** (~0.6–1.1 µs per event), **2,500–3,300× faster than chDB**; 0.7–7.6 M/s in batches of 1000, **5–17× faster** than chDB batched.
- **Native one at a time is about as fast as chDB batched by 1000**, without the batching's latency.
- **With many groups (10k) the cost is per touched group, not per event**: a batch of 1000 touches ~1000 groups, each finalized and diffed (~1 µs per event, near the plain WHERE query's ~0.3 µs plus a diff per event).
- **Subscribing** over 1M history rows: native 6–23 ms, chDB 11–28 ms, ClickHouse answering the GROUP BY directly 1–8 ms. The native path skips the chDB table; what's left is the HTTP transfer and building the states.
- **So**: native for the common algebraic aggregates; chDB for the rest is fine at low event rates or with batching.
