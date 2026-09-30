# chlive: notes, decisions, verified behaviors

All behaviors below were checked against `clickhouse/clickhouse-server:25.8` (25.8.33.6), Redpanda v25.2.1 and `sqlparser` 0.63.

## Data assumptions (enforced by the demo setup)

1. **One Kafka message is one row**, encoded as `JSONEachRow`. The main ingest MV (`infra/main-init.sql`) is a pure passthrough that only adds `_partition` and `_offset`.
2. Sources are plain, append-only `MergeTree` tables: no updates, no deletes, no Replacing/Collapsing engines. `FINAL` is rejected.
3. Every source table has `_partition UInt64` and `_offset UInt64`. The server checks this at startup (`tail::describe`).
4. Per partition, rows land in main in offset order, so `max(_offset)` per partition is a valid watermark. Verified: after seeding, offsets are contiguous from 0 per partition.
5. **Duplicates from at-least-once ingestion are ignored.** A Kafka-engine redelivery on main would be double-counted in history. The tail is not affected, because the server's consumer never commits and reads each offset exactly once per process.

## Spikes

### (a) `CREATE TABLE … ENGINE = Memory AS SELECT …State() FROM remote(main)`: works

`AggregateFunction(...)` columns round-trip through `remote()` into a `Memory` table. The Native-format fallback was not needed. The column types come out as, for example, `AggregateFunction(quantile(0.95), UInt32)`. Query parameters (`{path:String}`) are substituted by the cache before the query is forwarded to main. The aggregation runs on main: its `query_log` shows the forwarded `…State()` query with its `GROUP BY` reading all 1.13M rows and returning only 15 state rows. The history scan therefore happens on main, and the cache only receives states.

Setup gotchas that were verified along the way:

- The cache reaches main through a **named collection** `main_ro` (`infra/cache-config.xml`), so no credentials appear in SQL text or query logs.
- Using a named collection needs the `NAMED COLLECTION` grant. That is `named_collection_control` on the user (`infra/cache-users.xml`).
- The users.d override must sort *after* the image's `default-user.xml`, because that file does `remove="remove"` on `default`. Hence the name `zz-chlive.xml`.
- The main user `chlive_ro` is `readonly=2`, with `max_execution_time=60` and `max_memory_usage=4GB`. `readonly=1` would reject the settings that `remote()` and the HTTP interface send, such as `output_format_json_quote_64bit_integers`.

### (b) `fState`/`fMerge` naming for every allowlisted function: works

The naming holds for all of them: `f` → `fState` / `fMerge`, and `fIf` → `fIfState` / `fIfMerge`. That covers count, sum, min, max, avg, any, uniq, uniqExact, argMin, argMax, `quantile(p)`, `quantiles(…)`, and the `-If` form of each. The `all_functions` case in `tests/tests/e2e.rs` uses every one of them through the full pipeline and compares against main.

Observed exactness:

- count, sum over integers, min, max, argMin, argMax and uniqExact are **exact**. `any` is only compared where it is deterministic (`any(country)` grouped by country).
- `uniq` matched exactly in the spike, but it is documented as approximate. The tests allow 2%.
- `quantile(s)` **differs**, because it uses reservoir sampling (8192 samples): merging states samples differently from a single pass once a group has more than about 8k rows. The tests allow 15% relative or 5 ms absolute. The absolute slack matters for small percentiles like p10 = 5 vs 6.
- Float `sum`/`avg` can differ in the last bits between runs, because parallel summation order varies. The tests allow a relative 1e-9.

### (c) `sqlparser` (`ClickHouseDialect`) round-trip

- `quantile(0.95)(x)` parses into `Function.parameters` and prints back identically.
- `{name:Type}` *does* parse, but as `Expr::Dictionary` (`{name: Type}`), and a type like `Map(String, UInt8)` would not survive that. **Workaround** (`rewriter/params.rs`): before parsing, a small scanner that is aware of string literals, quoted identifiers and comments replaces each `{name:Type}` with the identifier `__p_name` and records its type. The placeholders are restored in the generated SQL. The recorded types are also used to validate client params: missing names and unknown names are both errors.
- `DateTime64(3)` literals, `INTERVAL`, `FORMAT`, `SETTINGS`, `ARRAY JOIN` and `OVER ()` all parse. The validator then rejects the unsupported ones.
- `FROM events FINAL` parses **as a table alias named FINAL**. All table aliases are rejected, and `FINAL` gets its own error message.
- ClickHouse's `WITH 1 AS x SELECT …` (expression aliases) does not parse, so it surfaces as a parse error. Standard `WITH t AS (…)` parses and is rejected as a CTE.
- `to_string()` output is accepted by ClickHouse for every example; the e2e tests run all of them.

### (d) `multiIf` watermark predicate: works

```sql
toInt64(_offset) <= multiIf(_partition = 0, 120, _partition = 1, 98, -1)   -- history
toInt64(_offset) >  multiIf(_partition = 0, 120, _partition = 1, 98, -1)   -- tail
```

Partitions at `-1` or missing from `W` are omitted, so they fall through to the `-1` default and all their rows go to the tail. With no rows at all, the predicate collapses to `toInt64(_offset) <= -1`, because `multiIf` needs at least 3 arguments. Two tests cover it. A unit test uses random watermarks with a Rust mirror of the predicate (`watermark.rs`). A live test has ClickHouse evaluate both predicates over synthetic `(partition, offset)` rows and asserts that history and tail are disjoint and complete (`watermark_predicate_in_clickhouse`).

### UNION ALL types

`sub_<id>_hist` is created from main's result types, and the tail state query runs in the cache over `tail_<table>`, which is created from main's `DESCRIBE`. The types match, including `LowCardinality(String)` keys and `AggregateFunction` columns, so no CASTs are needed. Both servers must run the same ClickHouse version, and the compose file pins it.

## Rewriter decisions

- **Aggregate aliases are expanded (the default).** `big_orders / count()` becomes `(countIf(amount > 100)) / count()` before the aggregates are split. That keeps the state query free of aggregate aliases and makes the merge SQL self-contained. The alternative, letting ClickHouse resolve `big_orders` inside the merge query, is kept behind `Options { expand_aliases: false }`. Both are tested: the golden file `example_5_no_expand.sql`, and the e2e test `alias_expansion_matches_clickhouse_resolution`, which runs both merges and gets identical results. An aggregate alias that shadows a source column is rejected, for example `max(latency_ms) AS latency_ms`.
- **Identical aggregate calls are computed once.** Calls are deduped by their printed form, so `count()`, `COUNT(*)` and a repeated `count()` share one slot.
- **Group keys.** A key that is a select alias uses that alias. A bare column uses its name. Otherwise the key gets a hidden `__key_i`. Key expressions inside merge-side expressions are replaced by the key alias. Keys that are not selected are still returned, because they are needed to build `__key`, but they are not client-visible columns.
- **Unaliased output columns** are named after their expression text, as ClickHouse would name them, for example `` countMerge(__agg_0) AS `count()` ``.
- **Rows plans** add hidden `__ord_i` columns for the ORDER BY expressions, so ordering by non-selected columns works. They also add `__partition`/`__offset` and break ties on them, which makes the result and its diffs deterministic. The key sent to clients is `[_partition, _offset]`.
- **Result size.** `LIMIT` must be at most `rows_limit_cap` (1000). An Aggregate query without LIMIT gets `LIMIT cap+1`, and if it returns more than `cap` rows, the subscription fails with `RESULT_TOO_LARGE`.
- **Aggregate detection** uses main's `system.functions` (`is_aggregate`) and `system.aggregate_function_combinators`, loaded at startup, with a builtin fallback list. Any aggregate outside the allowlist is rejected, including other combinators like `uniqState` and `sumArray`, and aggregates like `anyLast`.
- **Rejected functions:** time-relative ones (`now`, `now64`, `today`, `yesterday`, `current_timestamp`, …), whose error suggests passing the time as a parameter, and non-deterministic ones (`rand*`, `random*`, `generateUUID*`, `runningDifference`, `dictGet*`, …).
- Identifiers starting with `__` are reserved.

## Runtime decisions

- **Dedupe:** subscriptions are keyed by sha256 of the parser-normalized SQL plus the bound params. Whitespace differences therefore share a subscription. Concurrent first subscribers wait on one `OnceCell` initialization, so a history table is only ever built once. The refcount lives in the manager map, and when it reaches 0 the merge task is aborted and `sub_<id>_hist` is dropped.
- **Snapshot/patch ordering:** a client is attached, and gets its snapshot, while holding the subscription's state lock. Patches are broadcast under the same lock, so a client never sees a patch before its snapshot. The same lock serializes merges, so there are never two concurrent merges for one subscription.
- **Debounce (leading-edge throttle):** one `Notify` per subscription coalesces `TableAdvanced` events. The first change after a quiet period merges immediately, and changes arriving during a merge or within `debounce_ms` (100 ms) of the last one coalesce into one merge per interval. This used to be a trailing 100 ms sleep, which added 100 ms to every single event.
- **Tail batching is adaptive:** a batch is flushed as soon as no further message is already buffered, so a lone event is inserted immediately. Under load, messages pile up during the previous insert and batches grow by themselves. `batch_rows` and `batch_ms` only cap a batch's size and age.
- **Redpanda HTTP proxy:** `pandaproxy_client.produce_batch_delay_ms` is set to 0 in `docker-compose.yml`. The default of 100 ms added 100 ms to every write from the simple demo.

### Measured per-event latency (simple demo path: HTTP proxy write, then a patch at the client)

| Stage | Before | After |
|---|---|---|
| Produce via the HTTP proxy until acknowledged | 105 ms | 3 ms |
| Acknowledged → tail insert | 59 ms (the 50 ms batch window) | 6 ms |
| Tail insert → merge start | 102 ms (trailing debounce) | 0.1 ms |
| Merge query | 15 ms | 13 ms |
| Merge → client | 0.5 ms | 0.5 ms |
| **End to end (median of 20)** | **281 ms** | **24 ms** |

The merge query now dominates. It is mostly fixed ClickHouse per-query overhead (HTTP, parsing, planning) plus Docker Desktop networking on macOS, not data volume. Embedded chDB behind `CacheEngine` would remove the HTTP hop.
- **Tail consumer:** `assign()` with explicit offsets `W0[p] + 1`, `enable.auto.commit=false`, and a throwaway group id that rdkafka requires. It batches 1000 rows or 50 ms, and failed inserts are retried with backoff indefinitely. An undecodable message is logged and skipped. Main's Kafka engine would stall on the same message (`kafka_skip_broken_messages = 0`), so the upstream contract is that such messages don't exist.
- **The cache holds no durable state:** the `chlive` database in the cache is dropped and recreated on startup. The e2e tests use their own cache databases.
- The live result is always correct up to the tail's progress. If the tail has not yet reached `W`, the rows it is missing are all `> W`, and those only show up later.
- The demo rounds `since` down to the minute, so tabs opened in the same minute share the per-minute subscription.

## Known limitations

- **Duplicates** from at-least-once ingestion into main are not deduplicated (assumption 5).
- **The tail grows forever** (see compaction below).
- **No sliding windows:** `now()`-relative filters are rejected. The per-minute example passes a fixed `since` computed at subscribe time.
- **ORDER BY ties** in Rows plans are broken by Kafka position, while the original query breaks them arbitrarily. At a LIMIT boundary, the row picked among equal sort keys can therefore differ from a direct query. The e2e test compares the sort-key sequence exactly and full rows outside the last tie group. For the same reason, `argMax(user_id, latency_ms)` with tied maxima is only as deterministic as ClickHouse itself. The all-functions test uses a `(latency, user)` tuple so that it is deterministic.
- The patch `order` is the full key list. That is fine given the 1000-row cap.
- No auth, multi-tenancy or row-level security. There is a table allowlist (`[[tables]]` in `chlive.toml`), and main is accessed with a readonly user that has execution-time and memory limits.
- A single process holds all state. There is no horizontal scaling, and a server restart means clients resubscribe and get fresh snapshots. The client reconnects automatically.

## Next steps

### Tail compaction

Periodically, per subscription, and later per table:

1. Pick a new watermark `W' ≤ tail_max` for which the tail is complete.
2. Build the new history as `SELECT keys, fMergeState(__agg_i) … FROM (hist UNION ALL tail_state(W < offset ≤ W')) GROUP BY keys`, inserted into `sub_<id>_hist_next`. For Rows plans, use `topN(hist ∪ tail(W, W'])`.
3. Atomically swap it in (`EXCHANGE TABLES`), set `W := W'`, and do both under the subscription's state lock so no merge sees a mix.
4. When no subscription's `W` is below some offset, `ALTER TABLE tail_<t> DELETE WHERE _offset <= min_W[p]` can drop it. Better: partition `tail_<t>` by an offset bucket so old parts can be dropped cheaply.

### Other work

- **Sliding windows:** time-bucketed states (for example per minute) with windows assembled from buckets at merge time, and expiry driven by a server-side clock rather than `now()` in SQL.
- **Dedup:** track `(partition, offset)` in history too, or require an idempotent upstream key.
- **chDB:** implement `CacheEngine` over embedded chDB. The trait already isolates `exec`/`query`/`insert_json_each_row`. `remote()` works the same way from chDB.
- Share tail state queries between subscriptions that have the same WHERE and GROUP BY, and send incremental `order` diffs.
