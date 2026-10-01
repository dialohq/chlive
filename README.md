# chlive

A minimal **query-driven live sync layer** for a Kafka → ClickHouse analytics stack, in the spirit of Zero and ElectricSQL. A client subscribes with a query in chlive's SQL dialect. It gets the current result right away, then the changes over WebSocket as new Kafka events arrive. History is read from ClickHouse once per subscription; after that only new events are processed.

## Quickstart

Everything except Redpanda comes from the nix flake: the Rust and Bun toolchains, and **ClickHouse 26.7.2** (packaged in `nix/packages/clickhouse.nix`, because nixpkgs has another version). Redpanda runs in **Docker**, so Docker must be running. Local services run through [dnvr](https://github.com/dialohq/dnvr). There are two dev shells: `default`, the full stack described here, and `engine`, a smaller stack for working on the engine (see [Engine](#engine-custom-dialect)).

```sh
nix develop      # or `direnv allow` once (run `direnv reload` after flake changes)
dnvr up          # starts everything in a tmux dashboard
```

`dnvr up` starts these processes; the arrows are the start-up order dnvr derives from `dnvr://` dependencies:

| process | what | where |
|---|---|---|
| `redpanda` | Redpanda in Docker (container `chlive-redpanda`, volume `chlive-redpanda`); creates topics `events`, `customers`, `orders` (3 partitions each) | Kafka `127.0.0.1:19092`, HTTP proxy `127.0.0.1:18082` |
| `clickhouse` | ClickHouse 26.7.2 (dnvr's clickhouse preset) | HTTP `127.0.0.1:18123`, native `127.0.0.1:19000` |
| `schema` → clickhouse, redpanda | applies `scripts/schema.sql` (Kafka-engine tables, MVs, the `chlive_ro` user) | |
| `seed` → schema | runs `scripts/seed.sql` if ClickHouse has fewer than `$SEED_COUNT` events: generated, backdated events produced to Kafka from ClickHouse | |
| `server` → schema | chlive-server: WebSocket over chlive-engine, as `chlive_ro` | `:8080` (`/live`, `/debug/subscriptions`, `/healthz`) |
| `simple` | the example query playground | <http://localhost:5174> |

Dashboard keys: `j`/`k` selects, `Enter` shows a process (a second `Enter` focuses it), `r` restarts, `x` interrupts, `Q` stops everything, and `Ctrl-G` detaches. Running `dnvr up` again reattaches. `dnvr ps` and `dnvr logs <process>` work from any terminal. There is no live traffic generator; new events come from the playground's buttons, `produce`, or anything else producing to Kafka. The shell also has these scripts: `ch` (clickhouse client), `produce` (JSON events to a topic), `e2e` (end-to-end tests), and `reset-data` (wipe Redpanda and ClickHouse data together, since Kafka offsets and ClickHouse's `_offset`s must match; run it with the group stopped).

State lives in `.dnvr/` (gitignored) and in the `chlive-redpanda` Docker volume. The ports avoid the defaults (8123/9000/9092), so the stack can run next to another local ClickHouse or Kafka.

### Example: query playground

`examples/simple` (plain React, Bun workspace) is served by the `simple` process at <http://localhost:5174>:

- a query box with presets over `events`, `orders` and `customers`; ⌘/Ctrl+Enter runs it;
- the live result, newest rows first, with its status, row count and the engine's positioned error (e.g. `unknown column latency in table events (…) at 1:17`);
- buttons that write 1 or 50 random rows to each topic **straight to Kafka** through Redpanda's HTTP proxy (`127.0.0.1:18082`, proxied as `/kafka`), so you can watch each write travel Kafka → server → UI.

## How it works

The **engine** (`engine/`, see [Engine](#engine-custom-dialect)) does the work: it parses and binds the query against ClickHouse's schema, reads a per-partition offset watermark, runs the history query on ClickHouse up to it, and consumes the table's Kafka topic from right after it, decoding, filtering and projecting each event itself. Every event is in exactly one of history and stream.

The **server** (`server/`) is a thin WebSocket layer: one engine subscription per client subscription, its history sent as a `snapshot` and its events as `diff`s. There is no sharing between subscribers yet: each subscription reads its own watermark and has its own Kafka consumer.

```
Kafka ──▶ ClickHouse (Kafka engine + MV adds _partition/_offset)
  │             ▲ watermark + history query, once per subscription
  │             │
  └──────▶ chlive-server = WebSocket + chlive-engine ── snapshot, diffs ──▶ @chlive/client
   events after the watermark
```

## Layout

| path | what |
|---|---|
| `flake.nix` | the dnvr dev shells: toolchain, env, the local stacks' processes (Redpanda in Docker, ClickHouse 26.7.2, schema, seed, server, examples) and scripts |
| `nix/packages/` | custom derivations: `clickhouse.nix` (26.7.2), `redpanda.nix` (the Docker-run Redpanda process) |
| `scripts/` | `schema.sql` (tables, Kafka ingestion, read-only user) and `seed.sql` (historical events via Kafka); `engine/` has the same pair for the engine shell |
| `engine/` | `chlive-engine` (Rust): the SQL dialect (parser, binder, planner) and the event loop over ClickHouse and Kafka; also a CLI that prints each iteration's diffs |
| `server/` | `chlive-server` (Rust): the WebSocket protocol over the engine |
| `client/` | `@chlive/client` (zero dependencies) plus the `@chlive/client/react` hook `useLiveQuery` |
| `examples/` | example apps; `examples/simple`, the query playground |
| `tests/` | end-to-end tests (`chlive-e2e`): the server over the full stack, with a small Kafka producer |

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
const sub = live.subscribe("select * from events where latency_ms > 500", (s) => render(s.status, s.rows, s.columns, s.error));
sub.unsubscribe();

// React
import { useLiveQuery } from "@chlive/client/react";
const { rows, columns, status, error, errorCode } = useLiveQuery(sql);
```

`status` is `loading`, `live` or `error`. One socket carries all of a client's subscriptions; it reconnects with backoff and resubscribes, and the new snapshot replaces the old rows.

### Protocol

JSON text frames on `/live` (`server/src/protocol.rs`, mirrored by `client/src/protocol.ts`). Ids are the client's, per connection:

```jsonc
// client → server
{"type": "subscribe", "id": "s1", "sql": "select user_id, latency_ms from events where latency_ms > 100"}
{"type": "unsubscribe", "id": "s1"}
// server → client
{"type": "snapshot", "id": "s1", "columns": [{"name": "user_id", "type": "String"}, …], "rows": [{"user_id": "u1", "latency_ms": 340}, …]}
{"type": "diff", "id": "s1", "changes": [{"op": "insert", "row": {…}}, …]}
{"type": "error", "id": "s1", "code": "INVALID_QUERY", "message": "unknown column nope in table events (…) at 1:8"}
```

- `snapshot` comes first and once; `diff`s follow, each with the changes of the events that were ready (never empty). Only `insert` exists so far; clients reject ops they don't know.
- Error codes: `SYNTAX_ERROR` and `INVALID_QUERY` (the query; positioned), `HISTORY_FAILED` (ClickHouse), `STREAM_FAILED` (Kafka), `BAD_MESSAGE` (unparseable, no id), `DUPLICATE_ID`. After an error, that subscription is over.
- Rows are exactly the selected columns, rendered like ClickHouse's JSON output.

## Tests

```sh
cargo test --workspace     # engine (lexer, parser, binder, planner, types, watermark), server protocol over a real WebSocket
bun run test               # client state + reconnect/resubscribe (bun test)
e2e                        # = CHLIVE_E2E=1 cargo test -p chlive-e2e, against `dnvr up`
engine-test                # in the engine shell: the engine against its ClickHouse and Redpanda
```

The e2e tests subscribe over WebSocket while producing events, and check that each live result (snapshot plus inserts) equals the same query run directly on ClickHouse once everything is ingested. They count rows across the whole `events` table, so don't produce other events (e.g. with the playground's buttons) while they run.

## Supported queries

The engine's dialect, for now:

```
SELECT * | col, … FROM <table> [WHERE <condition>]
```

- A table needs a ClickHouse table (with `_partition` and `_offset`) and a Kafka topic of the same name.
- Conditions: comparisons, `AND`/`OR`/`NOT`, `IS [NOT] NULL`, `[NOT] IN (…)` over columns and literals; see [Engine](#engine-custom-dialect).
- Results only grow (inserts); there's no `ORDER BY`, `LIMIT`, `GROUP BY`/aggregates, joins, functions or parameters yet, and no result-size cap. Rows arrive in Kafka order per partition after the snapshot.
