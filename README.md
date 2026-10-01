# chlive

A minimal **query-driven live sync layer** for a Kafka → ClickHouse analytics stack, in the spirit of Zero and ElectricSQL. A client subscribes with a ClickHouse SQL query. It gets the current result right away, and then incremental patches over WebSocket as new Kafka events arrive. The server does **not** re-scan history on every update.

## Quickstart

Everything except Redpanda comes from the nix flake: the Rust and Node toolchains, **libchdb**, and **ClickHouse 26.7.2** (both packaged in `flake.nix`, because nixpkgs lacks them or has other versions). Redpanda runs in **Docker**, so Docker must be running. Local services run through [dnvr](https://github.com/dialohq/dnvr). There are two dev shells: `default`, the full stack described here, and `engine`, just ClickHouse for working on the engine (see [Engine](#engine-custom-dialect-step-1)).

```sh
nix develop      # or `direnv allow` once (run `direnv reload` after flake changes)
dnvr up          # starts everything in a tmux dashboard
```

`dnvr up` starts these processes; the arrows are the start-up order dnvr derives from `dnvr://` dependencies:

| process | what | where |
|---|---|---|
| `redpanda` | Redpanda in Docker (container `chlive-redpanda`, volume `chlive-redpanda`); creates topics `events`, `customers`, `orders` (3 partitions each) | Kafka `127.0.0.1:19092`, HTTP proxy `127.0.0.1:18082` |
| `clickhouse` | main ClickHouse 26.7.2 (dnvr's clickhouse preset) | HTTP `127.0.0.1:18123`, native `127.0.0.1:19000` |
| `schema` → clickhouse, redpanda | applies `scripts/schema.sql` (Kafka-engine tables, MVs, the `chlive_ro` user) | |
| `seed` → schema | runs `scripts/seed.sql` if main has fewer than `$SEED_COUNT` (1M) events: generated, backdated events produced to Kafka from ClickHouse | |
| `server` → schema | chlive-server with the embedded chDB cache | `:8080` (`/live`, `/debug/subscriptions`) |
| `simple` | the simple example app | <http://localhost:5174> |

Dashboard keys: `j`/`k` selects, `Enter` shows a process (a second `Enter` focuses it), `r` restarts, `x` interrupts, `Q` stops everything, and `Ctrl-G` detaches. Running `dnvr up` again reattaches. `dnvr ps` and `dnvr logs <process>` work from any terminal. There is no live traffic generator; new events come from the simple example's buttons (or anything else producing to Kafka). The shell also has these scripts: `ch` (clickhouse client on main), `e2e` (end-to-end tests), and `reset-data` (wipe Redpanda and ClickHouse data together, since offsets and watermarks must match; run it with the group stopped).

State lives in `.dnvr/` (gitignored) and in the `chlive-redpanda` Docker volume. The ports avoid the defaults (8123/9000/9092), so the stack can run next to another local ClickHouse or Kafka.

### Examples

`examples/` holds example apps (Bun workspaces). `examples/simple` is a nearly empty plain React (JSX) page, served by the `simple` process at <http://localhost:5174>. It has:

- a live list of the latest `events` rows, with a button that writes a new event;
- a live list of the latest `orders`, joined in the browser with the latest `customers` rows (two live queries; joins are not supported server-side). Buttons write a customer, write an order, or rename a customer. A rename is a new row, and `argMax(name, ts)` picks the latest one.

The buttons write **straight to Kafka** through Redpanda's HTTP proxy (`127.0.0.1:18082`, proxied as `/kafka`), so you can watch each write travel Kafka → tail → patch → UI.

## How it works: history states + tail

ClickHouse aggregates have mergeable intermediate **states** (`countState`, `uniqState`, `quantileState(0.95)` …). Merging them is associative and commutative:

```
agg(all rows) == aggMerge( aggState(history rows) ∪ aggState(tail rows) )
```

For each distinct subscription `(sql, params)`, the server does the following:

1. **Snapshot, once.** It takes a per-partition Kafka offset watermark `W` from main (`max(_offset)` per `_partition`). The cache (embedded **chDB**, i.e. ClickHouse running inside the server process) then materializes the states of every row with `_offset <= W[_partition]` by reading main through `remote()`. The result goes into `sub_<id>_hist`, an in-memory table.
2. **Tail.** The server consumes Kafka itself, using `assign()` with explicit offsets starting after the startup watermark. It appends the raw rows to `tail_<table>` in the cache.
3. **Merge.** The live result is `merge(history states, states over tail rows with _offset > W[_partition])`. After each tail insert, affected subscriptions re-run this cheap merge right away, throttled to at most one merge per 100 ms under load. The server diffs the result by key and pushes a `patch`.

Queries without aggregates use the same trick with rows. With a `LIMIT`, `topN(history ∪ tail) == topN(topN(history) ∪ tail)`, so only the top N history rows are copied. Without one, every matching history row is copied, so keep such queries selective.

The rewriter (`server/src/rewriter/`) turns the client's SQL into the history, tail-state, and merge queries. See the golden files in `server/src/rewriter/golden/` for exactly what gets generated from each example query.

```
Kafka ──▶ ClickHouse main (Kafka engine + MV adds _partition/_offset)
   │                ▲ remote(): history states, once per subscription
   ▼                │
 chlive-server [chDB in-process: tail_events, sub_<id>_hist] ── WebSocket ──▶ @chlive/client
```

## Layout

| path | what |
|---|---|
| `flake.nix` | the dnvr dev shell: toolchain, env, the local stack's processes (Redpanda in Docker, main ClickHouse 26.7.2, schema, seed, server, examples) and scripts |
| `nix/packages/` | custom derivations: `libchdb.nix`, `clickhouse.nix` (26.7.2, matching chDB), `redpanda.nix` (the Docker-run Redpanda process) |
| `scripts/` | `schema.sql` (main's tables, Kafka ingestion, readonly user) and `seed.sql` (historical events via Kafka); `engine/` has the same pair for the engine shell |
| `engine/` | `chlive-engine` (Rust, new architecture, in progress): a custom SQL dialect with its own parser; plans each query into a ClickHouse history query plus the topics to consume, then runs the event loop over Kafka |
| `server/` | `chlive-server` (Rust): rewriter, watermark, tail consumer, subscription manager, diff, WebSocket |
| `client/` | `@chlive/client` (zero dependencies) plus the `@chlive/client/react` hook `useLiveQuery` |
| `examples/` | example apps; `examples/simple` for now |
| `tests/` | end-to-end correctness tests (`chlive-e2e`), with a small Kafka producer for test events |

## Engine (custom dialect)

The engine has its own dev shell: its own ClickHouse and Redpanda, with ClickHouse ingesting the `events` topic like the full stack, and 10 seed events (`scripts/engine/`) produced through Kafka:

```sh
nix develop .#engine
dnvr up                                     # engine-redpanda (Kafka 29092), engine-clickhouse (HTTP 28123, native 29000), engine-schema, engine-seed
engine --explain "select * from events"     # plan: ClickHouse SQL, topics, keys, filter
engine "select * from events"               # history (10 rows), then live events until Ctrl-C
produce events '{"user_id": "u7", "latency_ms": 120}'   # one event per JSON argument (or JSON lines on stdin)
engine-test                                 # all engine tests, including the ClickHouse and Kafka ones
reset-data                                  # with `dnvr up` stopped: wipe the engine shell's Redpanda and ClickHouse together
```

Its process names, ports, Redpanda container and volume differ from the default shell's, so the two never share data and can run at the same time. In either shell, the engine uses that shell's ClickHouse and Kafka via `CHLIVE_CLICKHOUSE_URL` and `CHLIVE_KAFKA`; the default shell has `produce` too.

**History and stream meet at a watermark.** At subscribe time the engine reads `max(_offset)` per `_partition` from the ClickHouse table. History is every row up to it (the history query gets `toInt64(_offset) <= multiIf(_partition = 0, W0, …)`), and each Kafka partition is consumed from the offset right after it. So every event is in exactly one of the two, wherever ClickHouse's ingestion is at. The engine assigns partitions directly and never commits offsets, so subscriptions don't affect each other. The table must have `_partition` and `_offset` columns; the binder says so if it doesn't.

So far the dialect accepts `SELECT * | col, … FROM <table> [WHERE <condition>]`. A condition can use:
- comparisons (`=`, `!=`/`<>`, `<`, `<=`, `>`, `>=`), with `AND`, `OR`, `NOT` and parentheses;
- `IS [NOT] NULL` and `[NOT] IN (…)`;
- operands that are columns or literals: numbers, strings, `TRUE`/`FALSE`.

Functions, arithmetic, `LIKE` and `BETWEEN` aren't supported yet.

**WHERE runs in two places, which must agree on every row.** ClickHouse evaluates it for history; the engine evaluates it for each event.
- **Typed decoding.** Each event's needed JSON fields are decoded by their ClickHouse column types, following ClickHouse's input rules: a quoted number is read as a number, and a DateTime string is parsed.
- **Missing keys.** A missing key is NULL for a `Nullable` column, and the type's zero value otherwise, just as ClickHouse's ingestion stores it.
- **Conditions.** They're evaluated with SQL's three-valued logic.
- **Literals.** They're typed and checked at bind time; for example, comparing `latency_ms` with a string is a positioned error.
- **Output.** Event rows are rendered the way ClickHouse renders rows.
- **Tested.** The agreement test in `engine/tests/clickhouse.rs` checks, for 22 predicates, that three setups return the same rows: everything in history; everything from the stream; and half from each, split at the watermark. `engine/tests/kafka.rs` produces events before, around and after subscribing, against the real Redpanda and ClickHouse, and checks that each arrives exactly once.

The query goes through parser → binder → planner. The binder resolves FROM, then WHERE, then SELECT, in SQL's evaluation order:
- **FROM:** the table must exist in ClickHouse, have a Kafka topic, and have the `_partition` and `_offset` columns.
- **SELECT:** listed columns must exist, and none may be selected twice.

The planner selects only the needed columns from ClickHouse, and the stream decodes only the needed JSON keys of each event; the other keys are skipped unparsed. Hard-coded naming rules map table `t` to ClickHouse `default.t` and to topic `t`, and keep column names the same on both sides; columns starting with `_` are hidden.

Each iteration prints its diff: first the history (`+ {row}` per ClickHouse row, then `history: N rows`), then one line per relevant event of the subscribed topics, as `<topic>/<partition>:<offset> + {…}`, e.g. `events/0:21 + {…}`. Only the query's topics are consumed.

## Client

```ts
import { LiveClient } from "@chlive/client";
const live = new LiveClient({ url: "ws://localhost:8080/live" });
const sub = live.subscribe(sql, { path: "/checkout" }, (s) => render(s.status, s.rows, s.error));
sub.unsubscribe();

// React
import { useLiveQuery } from "@chlive/client/react";
const { rows, status, error } = useLiveQuery(sql, params);
```

Params are typed by their placeholders in the SQL (`{path:String}`, `{since:DateTime64(3)}`) and passed to ClickHouse as query parameters. They are never interpolated into the SQL text.

## Tests

```sh
cargo test --workspace                     # rewriter goldens + rejections, watermark, diff, params
bun run test                               # client state + reconnect/resubscribe (bun test)
e2e                                        # = CHLIVE_E2E=1 cargo test -p chlive-e2e, against `dnvr up`
```

The e2e tests produce their own events and compare against the whole `events` table, so don't produce other events (e.g. with the simple example's buttons) while they run. Regenerate golden files with `UPDATE_GOLDEN=1 cargo test -p chlive-server`.

## Supported queries

```
SELECT <items> FROM <allowlisted table> [WHERE …] [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT n]
```

- Aggregates: `count sum min max avg any uniq uniqExact argMin argMax quantile(p) quantiles(…)`, each also in its `-If` form.
- Expressions over aggregates work, and so do references to aggregate aliases.
- `ORDER BY` and `LIMIT` are optional everywhere, and there is no result-size cap. How much a query selects is up to its author; these results go to a frontend.
- Without `ORDER BY`, rows come in **Kafka order**: by `(partition, offset)`. That is Kafka's order within each partition; there is no global order across partitions.
- These are rejected with a clear error: `now()`/`today()`/`rand()`, joins, subqueries, CTEs, UNION, window functions, ARRAY JOIN, FINAL, SETTINGS, FORMAT, and table functions.
