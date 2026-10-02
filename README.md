# chlive

A minimal **query-driven live sync layer** for a Kafka → ClickHouse analytics stack, in the spirit of Zero and ElectricSQL. A client subscribes with a query in chlive's SQL dialect. It gets the current result right away, then the changes over WebSocket as new Kafka events arrive. History is read from ClickHouse once per subscription; after that only new events are processed.

## Quickstart

Everything except Redpanda comes from the nix flake: the Rust and Bun toolchains, and **ClickHouse 26.7.2** (packaged in `nix/packages/clickhouse.nix`, because nixpkgs has another version). Redpanda runs in **Docker**, so Docker must be running. Local services run through [dnvr](https://github.com/dialohq/dnvr). One dev shell runs the whole stack, for the server, the engine and the examples alike.

```sh
nix develop      # or `direnv allow` once (run `direnv reload` after flake changes)
dnvr up          # starts everything in a tmux dashboard
```

`dnvr up` starts these processes; the arrows are the start-up order dnvr derives from `dnvr://` dependencies:

| process | what | where |
|---|---|---|
| `redpanda` | Redpanda in Docker (container `chlive-redpanda`, volume `chlive-redpanda`); creates topics `events`, `customers`, `orders` (3 partitions each) | Kafka `127.0.0.1:9092`, HTTP proxy `127.0.0.1:8082` |
| `clickhouse` | ClickHouse 26.7.2 (dnvr's clickhouse preset) | HTTP `127.0.0.1:18123`, native `127.0.0.1:19000` |
| `schema` → clickhouse, redpanda | applies `scripts/schema.sql` (Kafka-engine tables, MVs, the `chlive_ro` user) | |
| `seed` → schema | runs `scripts/seed.sql` if ClickHouse has fewer than `$SEED_COUNT` events: generated, backdated events produced to Kafka from ClickHouse | |
| `server` → schema | chlive-server: WebSocket over chlive-engine, as `chlive_ro`, on its default ports | `:8080` (`/live`, `/debug/subscriptions`, `/healthz`) |
| `playground` | the example query playground | <http://localhost:5174> |

Dashboard keys: `j`/`k` selects, `Enter` shows a process (a second `Enter` focuses it), `r` restarts, `x` interrupts, `Q` stops everything, and `Ctrl-G` detaches. Running `dnvr up` again reattaches. `dnvr ps` and `dnvr logs <process>` work from any terminal. There is no live traffic generator; new events come from the playground's buttons, `produce`, or anything else producing to Kafka. The shell also has these scripts: `ch` (clickhouse client), `produce` (JSON events to a topic, e.g. `produce events '{"user_id": "u7", "latency_ms": 120}'`; or JSON lines on stdin), `engine` (the engine CLI), `e2e` (the tests that need the stack), and `reset-data` (wipe Redpanda and ClickHouse data together, since Kafka offsets and ClickHouse's `_offset`s must match; run it with the group stopped).

State lives in `.dnvr/` (gitignored) and in the `chlive-redpanda` Docker volume. Redpanda uses its standard ports (Kafka 9092, HTTP proxy 8082) and the server listens on 8080; ClickHouse uses 18123/19000 rather than 8123/9000, so it can run next to another local ClickHouse. These are the built-in defaults of the server, the engine CLI, the tests and the example, so nothing needs configuring; `CHLIVE_CLICKHOUSE_URL`, `CHLIVE_KAFKA` and `CHLIVE_LISTEN` override them.

### Example: query playground

`examples/playground` (plain React, Bun workspace) is served by the `playground` process at <http://localhost:5174>. Two panes:

- **Live query:** a query box with presets over `events`, `orders` and `customers` (⌘/Ctrl+Enter runs it), and the live result: newest rows first, with its status, row count and the engine's positioned error (e.g. `unknown column latency in table events (…) at 1:17`).
- **Write to Kafka:** pick a topic, write JSON by hand (one object, an array, or JSON lines) or start from a preset: a slow checkout, numbers as strings, missing keys (zero values), an unreadable value that both ClickHouse and the engine skip, a batch, a big order, a customer rename… Each write reports the partition and offset it landed at; `Random` and `50 random` write generated rows.

Writes go **straight to Kafka** through Redpanda's HTTP proxy (`127.0.0.1:8082`, proxied as `/kafka`), so you can watch each one travel Kafka → server → UI.

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
| `scripts/` | `schema.sql` (tables, Kafka ingestion, read-only user) and `seed.sql` (historical events via Kafka); |
| `engine/` | `chlive-engine` (Rust): the SQL dialect (parser, binder, planner) and the event loop over ClickHouse and Kafka; also a CLI that prints each iteration's diffs |
| `server/` | `chlive-server` (Rust): the WebSocket protocol over the engine |
| `client/` | `@chlive/client` (zero dependencies) plus the `@chlive/client/react` hook `useLiveQuery` |
| `examples/` | example apps; `examples/playground`, the query playground |
| `tests/` | end-to-end tests (`chlive-e2e`): the server over the full stack, with a small Kafka producer |

## Engine (custom dialect)

The engine is a library (used by the server) with a CLI that prints each iteration's diffs. With `dnvr up` running:

```sh
engine --explain "select * from events"     # plan: ClickHouse SQL, topics, keys, filter
engine "select * from events where latency_ms > 500"   # history, then live events until Ctrl-C
produce events '{"user_id": "u7", "latency_ms": 900}'  # one event per JSON argument
```

**History and stream meet at a watermark.** At subscribe time the engine reads `max(_offset)` per `_partition` from the ClickHouse table. History is every row up to it (the history query gets `toInt64(_offset) <= multiIf(_partition = 0, W0, …)`), and each Kafka partition is consumed from the offset right after it. So every event is in exactly one of the two, wherever ClickHouse's ingestion is at. The engine assigns partitions directly and never commits offsets, so subscriptions don't affect each other. The table must have `_partition` and `_offset` columns; the binder says so if it doesn't.

So far the dialect accepts `SELECT * | col, … FROM <table> [WHERE <condition>] [ORDER BY <order>] [LIMIT n]`. A condition can use:
- comparisons (`=`, `!=`/`<>`, `<`, `<=`, `>`, `>=`), with `AND`, `OR`, `NOT` and parentheses;
- `IS [NOT] NULL` and `[NOT] IN (…)`;
- operands that are columns or literals: numbers, strings, `TRUE`/`FALSE`.

Functions, arithmetic, `LIKE` and `BETWEEN` aren't supported yet.

**ORDER BY** takes columns (selected or not), each with `ASC`/`DESC` and `NULLS FIRST`/`NULLS LAST`, or `ALL`, which sorts by the SELECT list in SELECT order, as in ClickHouse. Positions, expressions, `COLLATE` and `WITH FILL` aren't supported yet. The order is **managed**:
- **Semantics match ClickHouse.** NULLs default to last in both directions; NaN sits between the values and the NULLs; strings compare by bytes; `-0` ties with `0`. The history query always spells out each key's direction and NULL placement.
- **State.** Per subscription, the engine keeps only the ORDER BY values of the result, with equal keys merged into tie groups, never the rows. Order-only columns are fetched from ClickHouse under hidden aliases (`__order_0`, …) and stripped from the output.
- **Placement.** The history arrives in ClickHouse's order. Each new matching row goes after the last row it ties with, and its insert carries that `index`. So the live list has the same sequence of keys as a fresh query; only the order of rows within a tie group can differ.
- **Tested.** `engine/tests/clickhouse.rs` checks 20 orderings (6 of them with LIMIT) (NULL, NaN, ±inf, `-0`, byte-order strings, DateTime precision, Bool, duplicates, DESC, NULLS FIRST/LAST, several keys, `ALL`, an unselected column, with WHERE) in the same three setups as WHERE.

**LIMIT n** (a whole number; OFFSET, `WITH TIES` and `LIMIT … BY` aren't supported yet) goes into the history query and bounds the result:
- **With ORDER BY: the top N.** The state holds at most N sort keys. A new row placed inside the top N is inserted at its index, and the row pushed past the end is deleted (`delete @N`). A row placed at or past the end, including one tying with the last row, changes nothing and sends nothing. Which rows of the boundary tie group make it in is arbitrary, as it is in ClickHouse.
- **Without ORDER BY: a window of the latest N rows.** History is whatever N rows ClickHouse returns. Each new matching row is appended, and once the window is full, the first row is deleted (`delete @0`). Only a row count is kept.
- **Atomic diffs.** An insert and the delete it causes are in the same iteration, so always in the same `diff`. The result has at most N rows after every diff, and the client only ever exposes whole diffs.

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
// with ORDER BY: the snapshot is in order and says so, and inserts carry their index
{"type": "snapshot", "id": "s2", "columns": […], "rows": […], "order": [{"column": "latency_ms", "direction": "desc", "nulls": "last"}]}
{"type": "diff", "id": "s2", "changes": [{"op": "insert", "row": {…}, "index": 2}, …]}
{"type": "error", "id": "s1", "code": "INVALID_QUERY", "message": "unknown column nope in table events (…) at 1:8"}
```

- `snapshot` comes first and once; `diff`s follow, each with the changes of the events that were ready (never empty). Only `insert` exists so far; clients reject ops they don't know.
- With an `index`, the row is inserted there: the row at that index and every row after it move one down. Without one, it's appended.
- `{"op": "delete", "index": k}` removes the row at `k`; the rows after it move up. With LIMIT (`"limit": n` in the snapshot), each insert that overflows the result is followed by the delete it causes.
- A `diff` is atomic: its changes apply in sequence, each index into the list as the previous change left it, and only the state after all of them is meaningful (in between, an insert and its delete can briefly make one row more than the LIMIT). The client applies a whole diff to one copy of the rows before its listeners see it.
- Error codes: `SYNTAX_ERROR` and `INVALID_QUERY` (the query; positioned), `HISTORY_FAILED` (ClickHouse), `STREAM_FAILED` (Kafka), `BAD_MESSAGE` (unparseable, no id), `DUPLICATE_ID`. After an error, that subscription is over.
- Rows are exactly the selected columns, rendered like ClickHouse's JSON output.

## Tests

```sh
cargo test --workspace     # engine (lexer, parser, binder, planner, types, watermark), server protocol over a real WebSocket
bun run test               # client state + reconnect/resubscribe (bun test)
e2e                        # = CHLIVE_E2E=1 cargo test -p chlive-engine -p chlive-e2e, against `dnvr up`
```

`e2e` runs the engine's ClickHouse and Kafka tests, then the server end to end. The latter subscribe over WebSocket while producing events, and check that each live result (snapshot plus inserts) equals the same query run directly on ClickHouse once everything is ingested. They count rows across the whole `events` table, so don't produce other events (e.g. with the playground's buttons) while they run.

## Supported queries

The engine's dialect, for now:

```
SELECT * | col, … FROM <table> [WHERE <condition>] [ORDER BY ALL | col [ASC|DESC] [NULLS FIRST|LAST], …] [LIMIT n]
```

- A table needs a ClickHouse table (with `_partition` and `_offset`) and a Kafka topic of the same name.
- Conditions: comparisons, `AND`/`OR`/`NOT`, `IS [NOT] NULL`, `[NOT] IN (…)` over columns and literals; see [Engine](#engine-custom-dialect).
- With `ORDER BY`, each insert lands at its index; without it, rows arrive in Kafka order per partition after the snapshot. `LIMIT` keeps the top N (with `ORDER BY`) or the latest N (without), deleting the rows that fall out.
- No OFFSET, `GROUP BY`/aggregates, joins, functions or parameters yet. Without `LIMIT`, results only grow.
