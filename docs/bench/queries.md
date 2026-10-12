# Queries through the TS client, as they get more complex

Measured on 2026-10-12 on the dev machine (Apple M3 Pro, 11 cores, 18 GB), with the dev stack (`dnvr up`: ClickHouse 26.7.2, Redpanda 25.2.1 in Docker, the release-built server) and `bench/queries.ts` through `@chlive/client` on Bun 1.4.2. Reproduce with `dnvr up`, then `bench` (= `bun bench/queries.ts`; its options are in the file's header). The `rust-alerts` and playground clients were idle or stopped.

## The queries

From one column with an equality filter to the most complex queries the dialect has; `events` is `scripts/schema.sql`'s, filled as `scripts/seed.sql` fills it (15 countries, 8 paths, ~5000 users, long-tailed latency), `customers` has 5000 rows (385 in PL).

| | what | query |
|---|---|---|
| q01 | one column, equality filter (nearly empty result) | `select user_id from events where user_id = 'bench-…'` |
| q02 | all columns, range filter (~1.5% of rows) | `select * from events where latency_ms > 2000` |
| q03 | compound filter: IN, OR, !=, AND (~6% of rows) | `select ts, user_id, path, latency_ms from events where (country in ('PL', 'DE', 'FR') or latency_ms >= 1000) and path != '/' and amount > 250` |
| q04 | LIMIT without ORDER BY: a sliding window of 1000 | `select * from events limit 1000` |
| q05 | ORDER BY one key, LIMIT 100 (top-N) | `select ts, user_id, latency_ms from events order by latency_ms desc limit 100` |
| q06 | filter + ORDER BY three keys + LIMIT/OFFSET | `select ts, user_id, country, latency_ms from events where country != 'US' order by country asc, latency_ms desc, ts desc limit 50 offset 100` |
| q07 | global aggregate | `select count() from events` |
| q08 | GROUP BY 15 groups, native aggregates, ORDER BY + LIMIT | `select country, count() as n, avg(latency_ms) as avg_latency, max(latency_ms) as max_latency from events group by country order by n desc limit 5` |
| q09 | GROUP BY two keys (120 groups), ORDER BY two terms | `select country, path, count() as n, avg(latency_ms) as avg_latency, max(amount) as max_amount from events group by country, path order by n desc, country asc limit 20` |
| q10 | GROUP BY ~5000 users, filter, ORDER BY sum, LIMIT/OFFSET | `select user_id, count() as n, sum(amount) as spent from events where amount > 0 group by user_id order by spent desc limit 10 offset 5` |
| q11 | GROUP BY with chDB aggregates next to a native one | `select country, uniqExact(user_id) as users, median(latency_ms) as p50, count() as n from events group by country order by users desc` |
| q12 | the most complex aggregate: OR filter, 2 keys, mixed chDB/native, ORDER BY an unselected aggregate, LIMIT/OFFSET | `select country, path, count() as n, uniq(user_id) as users, avg(latency_ms) as avg_latency from events where latency_ms > 50 and (amount > 0 or path = '/checkout') group by country, path order by max(latency_ms) desc, n desc limit 10 offset 2` |
| q13 | IN sub-query on a second table, filter, ORDER BY, LIMIT | `select ts, user_id, path, latency_ms from events where user_id in (select customer_id from customers where country = 'PL') and latency_ms > 100 order by ts desc limit 100` |
| q14 | the most complex plain query: correlated EXISTS + compound filter + 2-key ORDER BY + LIMIT/OFFSET | `select e.ts, e.user_id, e.country, e.amount from events e where exists (select 1 from customers c where c.customer_id = e.user_id and c.country in ('PL', 'DE') and c.name != '') and (e.amount > 50 or e.latency_ms > 1000) and e.path != '/login' order by e.amount desc, e.ts desc limit 25 offset 10` |

Each query comes with an event that's sure to change its result (a row above the top-N, a new user spending the most, …): the latency runs produce those one at a time.

## Summary

- **Subscribing costs ~220–300 ms whatever the query** (two tables: ~450 ms), while ClickHouse answers the same SQL in 2–28 ms. About 210 ms of it is the engine reading the topic's partition list twice per subscribe (once binding, once starting), each time through a new rdkafka client (~105 ms each, `KafkaSource::partitions` in `engine/src/source.rs`). Caching the partitions (or keeping one metadata client) would take ~200 ms off every subscribe.
- **Latency doesn't grow with complexity:** from producing an event to the client applying its diff, the median is 0.2–0.6 ms for every native query, plain or aggregated, with sub-queries or not; p99 ≤ 4 ms. Only chDB aggregates cost more (7–10 ms median, its per-batch cost). Unbounded results pay on the client: each diff copies the rows (`ResultStore.applyDiff`), so q03's 57k-row result has a 1.6 ms median, and a 500k-row result ~11 ms.
- **Throughput:** a backlog of ~50k events is absorbed while subscribing for every native query (≈1–2 µs of server CPU per event); unbounded results are limited by the client copying the result for each diff; chDB aggregates go through ~37–75k events/s at 17–31 µs/event. Larger bursts sometimes stalled ~1 s, likely librdkafka's fetch backoff (section 3).
- **A scaling problem with ordered GROUP BY over many groups:** `GroupResult::history` (`engine/src/aggregate/result.rs`) ranks the history's groups one at a time with `Vec::insert`, which is O(groups²). With ~113k users, q10 takes 7–15 s to subscribe (ClickHouse: 21 ms); the same GROUP BY without ORDER BY takes 0.3–0.6 s. Sorting them once (O(n log n)) should fix it.
- **The ClickHouse sink is the slowest part of the stack:** ~2.4k events/s (exactly-once, batches of ~500 with a KeeperMap write each), against ~150k/s produced through Redpanda's HTTP proxy and well over 100k/s consumed per subscription. Bursts take a while to reach snapshots; until they do, every new subscription replays them from Kafka.

## 1. Subscribing: subscribe → snapshot applied

On 1.0M events. Cold is the query's first subscription (the table already had a snapshot), warm the median of the next four; ClickHouse alone is the same SQL over HTTP (`FORMAT JSONEachRow`), median of five; client apply is `ResultStore.applySnapshot`.

| query | rows | size | cold | warm | ClickHouse alone | client apply |
|---|---|---|---|---|---|---|
| q01 | 13 | 0.5 kB | 265 ms | 233 ms | 3 ms | 0.04 ms |
| q02 | 14551 | 1.7 MB | 290 ms | 281 ms | 11 ms | 0.09 ms |
| q03 | 56982 | 4.9 MB | 308 ms | 304 ms | 17 ms | 0.06 ms |
| q04 | 1000 | 113.8 kB | 227 ms | 249 ms | 3 ms | 0.01 ms |
| q05 | 100 | 7.5 kB | 254 ms | 253 ms | 6 ms | 0.01 ms |
| q06 | 50 | 4.7 kB | 253 ms | 253 ms | 8 ms | 0.02 ms |
| q07 | 1 | 0.2 kB | 218 ms | 227 ms | 2 ms | 0.10 ms |
| q08 | 5 | 0.8 kB | 227 ms | 243 ms | 6 ms | 0.01 ms |
| q09 | 20 | 2.9 kB | 243 ms | 258 ms | 20 ms | 0.04 ms |
| q10 | 10 | 1.0 kB | 264 ms | 273 ms | 7 ms | 0.01 ms |
| q11 | 18 | 1.4 kB | 288 ms | 288 ms | 18 ms | 0.02 ms |
| q12 | 10 | 1.6 kB | 282 ms | 297 ms | 28 ms | 0.05 ms |
| q13 | 100 | 8.9 kB | 451 ms | 456 ms | 7 ms | 0.04 ms |
| q14 | 25 | 2.5 kB | 459 ms | 474 ms | 20 ms | 0.02 ms |

- Complexity barely shows: ~210 ms is the partition lookups above, the rest is the history query, binding (a `DESCRIBE` per table) and the transfer. The two-table queries (q13, q14) look up two topics and read two snapshots.
- Size shows a little: q03's 57k rows (4.9 MB) take ~70 ms more than q01's 13.
- The first subscription to a table without a snapshot also waits for one (pause, clone, resume): 2.5 s for `customers` in a first run here, 4.3 s for `events` in an engine started fresh.
- With many groups, ordered GROUP BY gets slow (see the summary): q10 at ~5k users is 273 ms; later, at ~113k users (the latency runs' hits are new users), 7–15 s.

## 2. Latency: produce → diff applied

On ~5.0M events. 200 events per query, one at a time: the time from sending the produce request (Redpanda's HTTP proxy, which acks in ~0.3 ms) to the client applying the diff it causes. Before measuring, each subscription is caught up with everything produced.

| query | p50 | p95 | p99 | max | missed |
|---|---|---|---|---|---|
| q01 | 0.3 ms | 0.8 ms | 1.3 ms | 1.7 ms | 0 |
| q02 | 0.4 ms | 1.1 ms | 1.9 ms | 2.0 ms | 0 |
| q03 | 1.6 ms | 2.7 ms | 4.1 ms | 5.5 ms | 0 |
| q04 | 0.4 ms | 0.9 ms | 1.8 ms | 1.9 ms | 0 |
| q05 | 0.3 ms | 0.9 ms | 1.7 ms | 1.8 ms | 0 |
| q06 | 0.4 ms | 0.8 ms | 1.4 ms | 2.0 ms | 0 |
| q07 | 0.4 ms | 0.8 ms | 1.7 ms | 1.9 ms | 0 |
| q08 | 0.2 ms | 0.7 ms | 1.1 ms | 1.2 ms | 0 |
| q09 | 0.3 ms | 0.8 ms | 1.4 ms | 2.0 ms | 0 |
| q10 | 0.4 ms | 0.9 ms | 1.4 ms | 2.1 ms | 0 |
| q11 | 9.5 ms | 15.1 ms | 29.8 ms | 30.2 ms | 0 |
| q12 | 7.4 ms | 9.1 ms | 13.8 ms | 14.3 ms | 0 |
| q13 | 0.4 ms | 1.2 ms | 2.4 ms | 6.7 ms | 0 |
| q14 | 0.3 ms | 1.1 ms | 2.8 ms | 3.0 ms | 0 |

- An event goes Redpanda → the subscription's consumer (a fetch returns as soon as there's data) → the engine → a WebSocket message → the client within a millisecond. Filters, ORDER BY with OFFSET, native aggregates and the semi-join's key lookup are all in the noise.
- chDB (q11, q12) adds an `INSERT … SELECT …State() FROM format(…)` and a `…Merge` read per batch: 7–10 ms.
- q03 is the only unbounded result here with many rows: the client copies its 57k rows for each diff.

## 3. Throughput: a backlog of events

On ~8.8M events, bursts of 50k, three runs per query: median (min–max). Each run waits for the sink to ingest everything, then for the table's snapshot to be refreshed, then produces a burst of events (seed.sql's distribution), and subscribes: the snapshot is from before the burst, so the subscription replays it from Kafka. *Caught up* is from subscribing until the client's cursor covers the burst's last offsets (the events counted from the snapshot's cursor); *server CPU/event* is the server process's CPU time over the run divided by those events (so it includes the subscribe and history).

| query | events | snapshot | caught up | after the snapshot | server CPU/event | diffs | changes | bytes | client apply |
|---|---|---|---|---|---|---|---|---|---|
| q01 | 47k | 296 ms | 298 ms (296–298) | 1 ms (1–2) | 1.3 µs | 2 | 2 | 0.5 kB | 0 ms |
| q02 | 47k | 481 ms | 764 ms (750–774) | 275 ms (269–284) | 5.3 µs | 193 | 749 | 124.2 kB | 282 ms |
| q03 | 48k | 819 ms | 1054 ms (1019–1952) | 255 ms (201–1129) | 10.6 µs | 201 | 3131 | 353.3 kB | 258 ms |
| q04 | 48k | 320 ms | 381 ms (362–408) | 88 ms (54–131) | 3.2 µs | 199 | 105004 | 8.2 MB | 21 ms |
| q05 | 53k | 343 ms | 345 ms (327–371) | 2 ms (2–2) | 2.1 µs | 1 | 2 | 0.4 kB | 0 ms |
| q06 | 51k | 353 ms | 355 ms (352–361) | 2 ms (2–2) | 2.2 µs | 1 | 2 | 0.4 kB | 0 ms |
| q07 | 48k | 222 ms | 283 ms (279–283) | 58 ms (57–61) | 1.5 µs | 1 | 54 | 2.9 kB | 0 ms |
| q08 | 68k | 313 ms | 313 ms (293–331) | 0 ms (0–1) | 1.4 µs | 1 | 367 | 30.8 kB | 0 ms |
| q09 | 52k | 400 ms | 401 ms (400–436) | 1 ms (1–1) | 2.1 µs | 1 | 1141 | 109.8 kB | 0 ms |
| q10 | 50k | 2518 ms | 2518 ms (2433–2537) | 0 ms (0–0) | 45.4 µs | 1 | 43 | 2.3 kB | 0 ms |
| q11 | 68k | 363 ms | 1252 ms (987–1406) | 899 ms (624–902) | 17.5 µs | 72 | 1096 | 68.0 kB | 1 ms |
| q12 | 49k | 427 ms | 1746 ms (1685–2191) | 1314 ms (1262–1764) | 31.3 µs | 53 | 462 | 50.7 kB | 0 ms |
| q13 | 51k | 554 ms | 554 ms (546–591) | 0 ms (0–0) | 2.3 µs | 0 | 2 | 0.1 kB | 0 ms |
| q14 | 48k | 624 ms | 624 ms (615–625) | 0 ms (0–0) | 2.3 µs | 0 | 2 | 0.1 kB | 0 ms |

- **A backlog of ~50k events costs nothing extra for most queries:** plain queries with a bounded result and every native aggregate are caught up when their snapshot arrives (*after the snapshot* ≈ 0), because the engine goes through the backlog while the subscribe is still in flight. With ~1.3–2.3 µs of server CPU per event, that's well over 100k events/s per subscription.
- **Unbounded results are bound by the client:** q02 and q03 take 255–275 ms more, almost all of it the client applying ~200 diffs (282 and 258 ms), each copying the whole result (15k and 57k rows). They also cost the most server CPU among the plain queries (5–11 µs/event): every matching row is sent.
- **The sliding window (q04)** changes on every event: 105k changes (8.2 MB) to the client in ~90 ms after the snapshot.
- **chDB is the expensive part:** q11 catches up at ~75k events/s, q12 at ~37k/s, at 17–31 µs of server CPU per event (5–15× the native queries).
- **q10's subscribe** (2.5 s) is the ordered-GROUP-BY build over ~110k users by then (see section 1); its CPU per event is mostly that.
- **Stalls of ~1 s** showed up in some earlier runs with 100k-event bursts: plain queries then took ~1.2–1.3 s to catch up, and another time ~0.3 s with the same setup; 300k events took ~2 s. A likely cause (not verified) is librdkafka's `fetch.queue.backoff.ms` (1000 ms by default in librdkafka 2.12): it pauses fetching once the prefetch queue passes its thresholds, and librdkafka's docs warn it can cause "long (~1s) delays between messages". The engine sets neither it nor the queue sizes.

## How it's measured, and caveats

- One machine runs everything: Redpanda (in Docker), ClickHouse, Kafka Connect, the server, the client. CPU contention shows in throughput (the sink ingests a burst while it's replayed).
- Timestamps are taken in the client's listener, when a message is applied: Bun can deliver several WebSocket messages before an `await` resumes, so timing after the `await` would credit later messages to earlier ones.
- A cursor's offsets are the last offset the result includes, per partition (`Watermark`).
- The three sections ran on growing data: the latency and throughput runs add events (each section's numbers are on the size given).
- The bench adds its events to the dev tables (the hits, with very high latencies and amounts, and ~100k `bench-*` users): `reset-data` (with `dnvr up` stopped) puts the dev data back to a fresh seed.
