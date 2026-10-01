# chlive: notes, decisions, verified behaviors

The behaviors below were first verified against `clickhouse/clickhouse-server:25.8` with a separate cache ClickHouse server. Since the move to chDB (see *Embedded chDB cache* below), main runs **26.7.2** and the cache is chDB 26.7.3 (ClickHouse 26.7.2.1). The e2e suite passes on that setup. `sqlparser` 0.63. Kafka was originally Redpanda v25.2.1 in Docker; it is now Apache Kafka 4.3.1 from nixpkgs, run by dnvr (see *Local stack (dnvr)*).

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

- The cache reaches main through a **named collection** `main_ro`, so no credentials appear in per-query SQL or query logs. With chDB it is created in SQL at startup (`CREATE NAMED COLLECTION`, from `[main]` in `chlive.toml`). The earlier cache server needed an XML config, the `named_collection_control` grant, and a users.d file sorted after the image's `default-user.xml`; none of that exists any more.
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
- **Result size is the query author's responsibility.** `ORDER BY` and `LIMIT` are optional for every query, and there is no cap. (The spec required both, with `LIMIT ≤ 1000`, for row queries; that was dropped because results go to a frontend and the author knows how much they need.)
  - A row query without `LIMIT` copies every matching history row into chDB and re-reads it on every merge, so it should be selective (e.g. a `WHERE` on an id).
  - Without `ORDER BY`, row queries are ordered by `(_partition, _offset)`, which is Kafka order within each partition. Across partitions it is simply partition 0 first, then 1, and so on: Kafka has no global order.
  - `LIMIT n` without `ORDER BY` therefore means "the first n rows by Kafka position". That is deterministic, but once filled it never changes, since new rows always sort after them. To follow new rows, use `ORDER BY ts DESC LIMIT n`.
  - History and tail are sorted only when a `LIMIT` cuts them. Without one, only the merge sorts.
- **Aggregate detection** uses main's `system.functions` (`is_aggregate`) and `system.aggregate_function_combinators`, loaded at startup, with a builtin fallback list. Any aggregate outside the allowlist is rejected, including other combinators like `uniqState` and `sumArray`, and aggregates like `anyLast`.
- **Rejected functions:** time-relative ones (`now`, `now64`, `today`, `yesterday`, `current_timestamp`, …), whose error suggests passing the time as a parameter, and non-deterministic ones (`rand*`, `random*`, `generateUUID*`, `runningDifference`, `dictGet*`, …).
- Identifiers starting with `__` are reserved.

## Runtime decisions

- **Dedupe:** subscriptions are keyed by sha256 of the parser-normalized SQL plus the bound params. Whitespace differences therefore share a subscription. Concurrent first subscribers wait on one `OnceCell` initialization, so a history table is only ever built once. The refcount lives in the manager map, and when it reaches 0 the merge task is aborted and `sub_<id>_hist` is dropped.
- **Snapshot/patch ordering:** a client is attached, and gets its snapshot, while holding the subscription's state lock. Patches are broadcast under the same lock, so a client never sees a patch before its snapshot. The same lock serializes merges, so there are never two concurrent merges for one subscription.
- **Debounce (leading-edge throttle):** one `Notify` per subscription coalesces `TableAdvanced` events. The first change after a quiet period merges immediately, and changes arriving during a merge or within `debounce_ms` (100 ms) of the last one coalesce into one merge per interval. This used to be a trailing 100 ms sleep, which added 100 ms to every single event.
- **Tail batching is adaptive:** a batch is flushed as soon as no further message is already buffered, so a lone event is inserted immediately. Under load, messages pile up during the previous insert and batches grow by themselves. `batch_rows` and `batch_ms` only cap a batch's size and age.
- **HTTP produce proxy:** the simple demo's writes originally went through Redpanda's HTTP proxy, whose default `produce_batch_delay_ms` of 100 ms added 100 ms to every write; it was set to 0. With Kafka there is no built-in proxy; `chlive-generator http-proxy` sends each record immediately (`linger.ms=0`).

### Measured per-event latency (simple demo path: HTTP proxy write, then a patch at the client)

| Stage | Before | After |
|---|---|---|
| Produce via the HTTP proxy until acknowledged | 105 ms | 3 ms |
| Acknowledged → tail insert | 59 ms (the 50 ms batch window) | 6 ms |
| Tail insert → merge start | 102 ms (trailing debounce) | 0.1 ms |
| Merge query | 15 ms | 13 ms |
| Merge → client | 0.5 ms | 0.5 ms |
| **End to end (median of 20)** | **281 ms** | **24 ms** |

The merge query now dominates. With the HTTP cache it was mostly per-query overhead plus Docker networking on macOS; the chDB cache (below) removed about 4 ms of that.
- **Tail consumer:** `assign()` with explicit offsets `W0[p] + 1`, `enable.auto.commit=false`, and a throwaway group id that rdkafka requires. It batches 1000 rows or 50 ms, and failed inserts are retried with backoff indefinitely. An undecodable message is logged and skipped. Main's Kafka engine would stall on the same message (`kafka_skip_broken_messages = 0`), so the upstream contract is that such messages don't exist.
- **The cache holds no durable state:** chDB runs in-memory, and a server start drops any `tail_*` / `sub_*` tables left over from an earlier server in the same process (the e2e tests start several).
- The live result is always correct up to the tail's progress. If the tail has not yet reached `W`, the rows it is missing are all `> W`, and those only show up later.
- The demo rounds `since` down to the minute, so tabs opened in the same minute share the per-minute subscription.

## Embedded chDB cache

The cache is chDB (`chdb-rust` 2.0 over libchdb, chdb-core v26.7.3) inside `chlive-server`. It implements `CacheEngine` in `server/src/cache_engine.rs`; nothing else changed.

- **Version lock:** libchdb is ClickHouse 26.7.2.1, and main is pinned to ClickHouse 26.7.2.59 (packaged in `flake.nix` from the official release binary). AggregateFunction states cross between them in binary form, so **upgrade both together**: bump `libchdbVersion`, `clickhouseVersion`, and their hashes in `flake.nix`. The chdb-rust version pins the engine it expects, and 2.0 requires chdb-core ≥ 26.7.3.
- **Moving main from 25.8 to 26.7** (in the Docker days): the upgrade recreated main with empty tables, and main's consumer-group offsets in Redpanda had to be deleted so it re-ingested every topic from offset 0. That kept offsets and watermarks consistent.
- **Nix packaging:** `flake.nix` fetches the release tarball (hashes from its `SHA256SUMS`). It contains `libchdb.so` (also on macOS) and `chdb.h`. The dev shell sets `CHDB_LIB_DIR` / `CHDB_INCLUDE_DIR`, so chdb-rust's `build.rs` links the store copy and never downloads; bindgen gets `LIBCLANG_PATH`. The library's install name is `@rpath/libchdb.so`, so `server/build.rs` and `tests/build.rs` add an rpath to the store path, and binaries run outside cargo. Caveat: a build with a stale environment, such as an editor still holding the pre-chDB direnv shell, has no `CHDB_LIB_DIR`, and chdb-rust then silently downloads an engine into `~/Library/Caches/chdb-rust`. That happened once here. Reload direnv after flake changes.
- **Verified in a spike** (26.7.2 main, chDB 26.7.3):
  - `remote('localhost:19000', …)` works. Main's native port is 19000.
  - SQL-defined named collections work.
  - `CREATE TABLE … ENGINE = Memory AS SELECT …State() FROM remote(…)` round-trips states.
  - Typed `{name:Type}` params via `chdb_query_with_params` work, including params inside queries forwarded through `remote()`.
  - Streaming JSONEachRow inserts work.
  - Several in-memory connections share one engine and its tables, and run queries concurrently: two `sleep(1)` on two connections took 1.01 s.
- **Pool:** a single process-wide pool (`OnceLock`) of `pool_size` connections (default 4), opened once and never closed. chDB allows one engine per process, and closing and reopening repeatedly is known to corrupt the allocator on macOS. Each connection runs one statement at a time; calls run in `spawn_blocking` on a free connection.
- **Session settings, applied to every connection:**
  - `output_format_json_quote_64bit_integers = 0` and `output_format_json_quote_denormals = 1`, the same as the HTTP client used.
  - **`session_timezone` = main's `timezone()`.** chDB defaults to the host's local zone, which shifted every `DateTime` by the UTC offset (UTC+2 here). The e2e suite caught this.
- **chDB console log is off** (`--logger.console=0`; `cache.console_log` in `chlive.toml` turns it back on). chDB prints its own ClickHouse server log to stderr, but only when stderr is a terminal, such as a dnvr pane. In the dnvr pane, every tail insert made it log `<Fatal> ThreadStatus: current_thread contains invalid address`: about 85/s with the stream at 200 events/s, and about 110/s during catch-up.
  - **Bisection** (real server under a pty, repeated runs): 0 lines with tail inserts skipped, and still present with a fixed one-row body, so any insert triggers it and the data doesn't matter. Pool size, table count and parallel parsing make no difference.
  - **Not reproduced standalone:** isolated chdb-rust programs (direct or through `ChdbCache`, sustained inserts, an active Kafka consumer) never emit it, so the root cause is still unknown. It is a chDB-internal `ThreadStatus` bookkeeping diagnostic.
  - **No effect on results:** query results are unaffected, and the e2e suite passes with it present. A candidate for an upstream chDB report once a minimal repro exists.
  - **Caveat for debugging:** with the console log off, chDB's own diagnostics don't show up; query errors still come back through the API.
- **Signal handlers:** chDB's are disabled at startup (`runtime::signal_handlers(false)`), so tokio keeps SIGINT/SIGTERM.
- **Footprint:** libchdb is 342 MB on disk, and the server's RSS was 132 MB with the demo subscriptions live. The separate cache container is gone.
- **Latency, measured on the simple-demo path with 20 single events:**

  | Stage | ClickHouse cache over HTTP | chDB in-process |
  |---|---|---|
  | Produce through the HTTP proxy until acknowledged | 3.3 ms | 5.0 ms |
  | Write → tail insert done | 9.3 ms | 13.2 ms |
  | Merge query | 13.4 ms | **9.0 ms** |
  | Merge → client | 0.5 ms | 0.3 ms |
  | **End to end (median)** | **23.6 ms** | **22.9 ms** |

  The merge got ~4 ms faster: no HTTP, JSON over the wire, or Docker network hop. The remaining ~9 ms is ClickHouse's own parsing, planning and execution, which chDB doesn't remove. End to end is about the same within noise, so the win is operational (one service fewer, no cache container config) rather than latency.

## Local stack (dnvr)

The local stack is a [dnvr](https://github.com/dialohq/dnvr) shell (`dnvr.shells.default` in `flake.nix`). `nix develop` gives the toolchain, and `dnvr up` runs the processes in a tmux dashboard. Docker is gone: there is no `docker-compose.yml`, and `scripts/dev.sh` was replaced by `dnvr up`.

- **ClickHouse 26.7.2 is packaged from the official release binary.** nixpkgs has 26.5 and 26.3 LTS, and main must match chDB's 26.7.2. The binaries are self-extracting: the first run decompresses the file in place, which fails in the read-only Nix store ("mkstemp: Permission denied"). The derivation therefore runs `clickhouse --version` once at build time. The unpacked binary is 815 MB.
- **Main runs on dnvr's `clickhouse` preset,** with `access_control_path` added in `extraConfigXml`. Without it the server has no writable user storage, and `CREATE USER chlive_ro` fails with `ACCESS_STORAGE_FOR_INSERTION_NOT_FOUND`. The `chlive_ro` user (`readonly=2`, `max_execution_time`, `max_memory_usage`) is now created in SQL in `infra/main-init.sql`, not in a users.d XML file.
- **Ports 18123/19000/19092** instead of the defaults, because this machine already runs another ClickHouse on 8123.
- **The `ch` client uses its own config file** (`--config-file`). Otherwise `~/.clickhouse-client/config.xml` applied, and here it set a different user (`readonly`), which broke the schema step.
- **Kafka** is Apache Kafka 4.3.1 from nixpkgs as a single-node KRaft broker and controller, with data in `.dnvr/kafka`. The `kafka` process formats storage on first start, creates the three topics, and only then publishes `kafka.topics`, which dependents wait on. Restarting main's ClickHouse while Kafka keeps running leaves the old Kafka-engine consumers in their groups until the session timeout (~45 s), and ingestion pauses until the group rebalances.
- **Start-up order comes from `dnvr://` refs:** `schema` waits for `clickhouse.database` and `kafka.topics`, `seed` and `server` wait for `schema.ready`, `stream` waits for `seed.seeded`, `proxy` waits for `kafka.topics`, and `simple` waits for `demo.deps` (the `npm install`). Refs only resolve while the producer is alive, so `schema` and `seed` idle after finishing.
- **Vite binds `localhost` to `[::1]` only,** so open the demos at `localhost`, not `127.0.0.1`.

## Known limitations

- **Duplicates** from at-least-once ingestion into main are not deduplicated (assumption 5).
- **The tail grows forever** (see compaction below).
- **No sliding windows:** `now()`-relative filters are rejected. The per-minute example passes a fixed `since` computed at subscribe time.
- **ORDER BY ties** in Rows plans are broken by Kafka position, while the original query breaks them arbitrarily. At a LIMIT boundary, the row picked among equal sort keys can therefore differ from a direct query. The e2e test compares the sort-key sequence exactly and full rows outside the last tie group. For the same reason, `argMax(user_id, latency_ms)` with tied maxima is only as deterministic as ClickHouse itself. The all-functions test uses a `(latency, user)` tuple so that it is deterministic.
- The patch `order` is the full key list, so it grows with the result. That's fine for frontend-sized results, but large unbounded results would want incremental order diffs.
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
- Share tail state queries between subscriptions that have the same WHERE and GROUP BY, and send incremental `order` diffs.
