# chlive

A minimal **query-driven live sync layer** for a Kafka → ClickHouse analytics stack, in the spirit of Zero and ElectricSQL. A client subscribes with a ClickHouse SQL query. It gets the current result right away, and then incremental patches over WebSocket as new Kafka events arrive. The server does **not** re-scan history on every update.

## Quickstart

Everything except Redpanda comes from the nix flake: the Rust and Node toolchains, **libchdb**, and **ClickHouse 26.7.2** (both packaged in `flake.nix`, because nixpkgs lacks them or has other versions). Redpanda runs in **Docker**, so Docker must be running. Local services run through [dnvr](https://github.com/dialohq/dnvr):

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

`examples/` holds example apps (npm workspaces). `examples/simple` is a nearly empty plain React (JSX) page, served by the `simple` process at <http://localhost:5174>. It has:

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
| `scripts/` | `schema.sql` (main's tables, Kafka ingestion, readonly user) and `seed.sql` (historical events via Kafka) |
| `engine/` | `chlive-engine` (Rust, new architecture, in progress): a custom SQL dialect with its own parser; plans each query into a ClickHouse history query plus the topics to consume, then runs the event loop. Kafka is simulated by `engine/topics/*.jsonl` for now |
| `server/` | `chlive-server` (Rust): rewriter, watermark, tail consumer, subscription manager, diff, WebSocket |
| `client/` | `@chlive/client` (zero dependencies) plus the `@chlive/client/react` hook `useLiveQuery` |
| `examples/` | example apps; `examples/simple` for now |
| `tests/` | end-to-end correctness tests (`chlive-e2e`), with a small Kafka producer for test events |

## Engine (custom dialect, step 1)

```sh
cargo run -p chlive-engine -- --explain "select * from events"     # plan: ClickHouse SQL + topics
cargo run -p chlive-engine -- --follow "select * from events"      # history, then live events
```

So far the dialect accepts only `SELECT * FROM <table>`. Hard-coded naming rules map table `t` to ClickHouse `default.t` and to topic `t` (`engine/topics/t.jsonl`), and keep column names the same on both sides; columns starting with `_` are hidden.

Each iteration prints its diff: first the history (`+ {row}` per ClickHouse row, then `history: N rows`), then one line per event of the subscribed topics, e.g. `events:21 + {…}`. Only the query's topics are opened: lines appended to `events.jsonl` show up with `--follow`, and lines appended to `orders.jsonl` don't.

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
npm test -w client                         # client state + reconnect/resubscribe
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
