# GROUP BY via chDB: what it costs

Measured on 2026-10-05 on the dev machine (Apple Silicon, release build), ClickHouse 26.7.2 and chDB 26.7.3 (`engine/examples/agg_bench.rs`; reproduce with `dnvr up`, then `cargo run --release -p chlive-engine --example agg_bench`).

How the chDB path works: main ClickHouse computes the history's aggregate states (`countState()`, `avgState(x)`, …) with `remote()` into a chDB table in the engine process; each batch of events adds its states (`INSERT … SELECT …State() FROM format(JSONEachRow, …)`), and the touched groups are read back finalized (`…Merge`). "Native" is a plain Rust `HashMap` of (count, sum) per group doing the same updates, finalization and diffing.

## 1. chDB per batch vs native (aggregation only)

| query | groups | batch | chDB insert | chDB read | chDB per event | chDB events/s | native per event | native events/s | slowdown |
|---|---|---|---|---|---|---|---|---|---|
| count() | 1 | 1 | 1.35 ms | 579.7 µs | 1.93 ms | 519 /s | 167 ns | 6.0 M/s | 11549× |
| count() | 1 | 10 | 1.39 ms | 591.2 µs | 198.3 µs | 5.0 k/s | 33 ns | 30.2 M/s | 5988× |
| count() | 1 | 100 | 1.39 ms | 646.0 µs | 20.4 µs | 49.0 k/s | 21 ns | 48.4 M/s | 986× |
| count() | 1 | 1000 | 1.46 ms | 597.4 µs | 2.1 µs | 485.8 k/s | 20 ns | 50.4 M/s | 104× |
| avg(x) | 1 | 1 | 1.33 ms | 558.8 µs | 1.89 ms | 530 /s | 204 ns | 4.9 M/s | 9228× |
| avg(x) | 1 | 10 | 1.48 ms | 568.3 µs | 204.6 µs | 4.9 k/s | 38 ns | 26.1 M/s | 5333× |
| avg(x) | 1 | 100 | 1.22 ms | 541.2 µs | 17.6 µs | 56.8 k/s | 21 ns | 47.9 M/s | 844× |
| avg(x) | 1 | 1000 | 1.44 ms | 565.4 µs | 2.0 µs | 498.8 k/s | 21 ns | 48.0 M/s | 96× |
| k, count(), avg(x) | 10 | 1 | 1.62 ms | 1.15 ms | 2.77 ms | 361 /s | 212 ns | 4.7 M/s | 13036× |
| k, count(), avg(x) | 10 | 10 | 1.59 ms | 1.29 ms | 287.5 µs | 3.5 k/s | 214 ns | 4.7 M/s | 1343× |
| k, count(), avg(x) | 10 | 100 | 1.61 ms | 1.32 ms | 29.3 µs | 34.2 k/s | 40 ns | 25.1 M/s | 734× |
| k, count(), avg(x) | 10 | 1000 | 2.00 ms | 1.62 ms | 3.6 µs | 276.4 k/s | 22 ns | 44.5 M/s | 161× |
| k, count(), avg(x) | 10000 | 1 | 1.73 ms | 1.27 ms | 3.01 ms | 332 /s | 208 ns | 4.8 M/s | 14469× |
| k, count(), avg(x) | 10000 | 10 | 1.73 ms | 1.61 ms | 334.8 µs | 3.0 k/s | 195 ns | 5.1 M/s | 1715× |
| k, count(), avg(x) | 10000 | 100 | 1.74 ms | 1.58 ms | 33.2 µs | 30.1 k/s | 229 ns | 4.4 M/s | 145× |
| k, count(), avg(x) | 10000 | 1000 | 2.36 ms | 2.63 ms | 5.0 µs | 200.6 k/s | 278 ns | 3.6 M/s | 18× |

## 3. End to end through the engine (decode, filter, aggregate, diff)

| query | max batch | events | time | events/s | per event | diffs |
|---|---|---|---|---|---|---|
| `select k10, x from events where x > 500` | 1000 | 200000 | 0.07 s | 2.9 M/s | 348 ns | 99800 |
| `select count() from events` | 1 | 5000 | 9.26 s | 540 /s | 1.85 ms | 5000 |
| `select count() from events` | 1000 | 200000 | 0.45 s | 446.2 k/s | 2.2 µs | 200 |
| `select k10, count(), avg(x) from events group by k10` | 1 | 5000 | 13.81 s | 362 /s | 2.76 ms | 5000 |
| `select k10, count(), avg(x) from events group by k10` | 1000 | 200000 | 0.77 s | 258.6 k/s | 3.9 µs | 2000 |
| `select k10k, count(), avg(x) from events group by k10k` | 1000 | 200000 | 1.17 s | 170.4 k/s | 5.9 µs | 200000 |

## 4. Subscribing over 1000000 history rows

| query | chDB: states via remote() + snapshot | ClickHouse GROUP BY directly |
|---|---|---|
| `select count() from events` | 11 ms | 1 ms |
| `select k10, count(), avg(x) from events group by k10` | 13 ms | 2 ms |
| `select k10k, count(), avg(x) from events group by k10k` | 27 ms | 8 ms |

## Takeaways

- **Every batch costs about 2–5 ms in chDB, whatever its size**: ~1.4 ms to insert the batch's states, ~0.6–2.6 ms to read the touched groups back. That fixed cost is what matters.
- **One event at a time (latency mode), chDB does ~350–550 events/s per subscription**, while the native path does ~5 M/s: **10 000× slower**. This is the "count() on frequent events" case, and chDB can't keep up with any real event rate per subscription.
- **Batching hides most of it:** with 1000-event batches chDB reaches 200–500 k events/s (2–5 µs per event), still 18–160× slower than native, and each batch adds 2–5 ms of latency.
- **End to end**, the engine without aggregates does ~2.9 M events/s; with chDB aggregates 170–450 k/s when batched, 360–540/s when not.
- **Subscribing is cheap either way**: building the states from 1M history rows takes 11–27 ms (vs 1–8 ms for ClickHouse answering the GROUP BY directly): the `remote()` transfer of states and the chDB table cost ~10–20 ms more.
- **So**: native implementations for the common algebraic aggregates (count, sum, avg, min, max) are worth it; chDB is reasonable as the fallback for aggregates we don't implement, accepting batching (and its latency) or low event rates.
