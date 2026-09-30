# chlive

A minimal **query-driven live sync layer** for a Kafka (Redpanda) → ClickHouse analytics stack, in the spirit of Zero and ElectricSQL. A client subscribes with a ClickHouse SQL query. It gets the current result right away, and then incremental patches over WebSocket as new Kafka events arrive. The server does **not** re-scan history on every update.

## Quickstart

Everything except Docker comes from the nix flake (cargo, node, librdkafka, …):

```sh
nix develop -c ./scripts/dev.sh      # or `direnv allow` once, then ./scripts/dev.sh
```

The script runs `docker compose up`, builds the server and generator, and seeds about 1M backdated events through Kafka. It then starts the server on `:8080`, streams 200 events/s, and serves the demo at <http://localhost:5173>. You can tune it with `SEED_COUNT=… RATE=…`.

Individual pieces:

```sh
docker compose up -d                                     # redpanda, clickhouse-main, clickhouse-cache
cargo run -p chlive-generator -- seed --count 1000000    # history, via Kafka
cargo run -p chlive-server                               # reads chlive.toml
cargo run -p chlive-generator -- stream --rate 200       # live events
npm install && npm run dev -w demo                       # demo UI
curl localhost:8080/debug/subscriptions                  # shared subscriptions + refcounts
```

## How it works: history states + tail

ClickHouse aggregates have mergeable intermediate **states** (`countState`, `uniqState`, `quantileState(0.95)` …). Merging them is associative and commutative:

```
agg(all rows) == aggMerge( aggState(history rows) ∪ aggState(tail rows) )
```

For each distinct subscription `(sql, params)`, the server does the following:

1. **Snapshot, once.** It takes a per-partition Kafka offset watermark `W` from main (`max(_offset)` per `_partition`). The cache ClickHouse then materializes the states of every row with `_offset <= W[_partition]` by reading main through `remote()`. The result goes into `sub_<id>_hist`, an in-memory table.
2. **Tail.** The server consumes Kafka itself, using `assign()` with explicit offsets starting after the startup watermark. It appends the raw rows to `tail_<table>` in the cache.
3. **Merge.** The live result is `merge(history states, states over tail rows with _offset > W[_partition])`. After each tail insert, affected subscriptions re-run this cheap merge (debounced, 100 ms). The server diffs the result by key and pushes a `patch`.

Queries without aggregates (`SELECT … ORDER BY … LIMIT n`) use the same trick with rows: `topN(history ∪ tail) == topN(topN(history) ∪ tail)`.

The rewriter (`server/src/rewriter/`) turns the client's SQL into the history, tail-state, and merge queries. See the golden files in `server/src/rewriter/golden/` for exactly what gets generated from each demo query.

```
Redpanda ──▶ ClickHouse main (Kafka engine + MV adds _partition/_offset)
   │                ▲ remote(): history states, once per subscription
   ▼                │
 chlive-server ── ClickHouse cache (tail_events, sub_<id>_hist) ── WebSocket ──▶ @chlive/client
```

## Layout

| path | what |
|---|---|
| `flake.nix` | dev shell: rust toolchain, librdkafka, node |
| `docker-compose.yml`, `infra/` | Redpanda (topic `events`, 3 partitions), main and cache ClickHouse, schema, readonly user, `remote()` named collection |
| `server/` | `chlive-server` (Rust): rewriter, watermark, tail consumer, subscription manager, diff, WebSocket |
| `generator/` | `chlive-generator`: seeds history and streams live events through Kafka |
| `client/` | `@chlive/client` (zero dependencies) plus the `@chlive/client/react` hook `useLiveQuery` |
| `demo/` | Vite + React page with examples 1–8 |
| `tests/` | end-to-end correctness tests (`chlive-e2e`) |
| `NOTES.md` | decisions, verified ClickHouse behaviors, limitations, next steps |

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
docker compose up -d
CHLIVE_E2E=1 cargo test -p chlive-e2e      # live vs. direct-on-main correctness, boundary race, …
```

The e2e tests produce their own events and compare against the whole `events` table, so **stop any other producer**, such as `dev.sh`'s stream, while they run. Regenerate golden files with `UPDATE_GOLDEN=1 cargo test -p chlive-server`.

## Supported queries

```
SELECT <items> FROM <allowlisted table> [WHERE …] [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT n]
```

- Aggregates: `count sum min max avg any uniq uniqExact argMin argMax quantile(p) quantiles(…)`, each also in its `-If` form.
- Expressions over aggregates work, and so do references to aggregate aliases.
- Queries without aggregates need `ORDER BY` and `LIMIT n ≤ 1000`.
- These are rejected with a clear error: `now()`/`today()`/`rand()`, joins, subqueries, CTEs, UNION, window functions, ARRAY JOIN, FINAL, SETTINGS, FORMAT, and table functions.
