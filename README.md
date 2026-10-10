# chlive

A minimal **query-driven live sync layer** for a Kafka → ClickHouse analytics stack, in the spirit of Zero and ElectricSQL. A client subscribes with a query in chlive's SQL dialect. It gets the current result right away, then the changes over WebSocket as new Kafka events arrive. History is read from ClickHouse once per subscription; after that only new events are processed.

## Quickstart

Everything except Redpanda comes from the nix flake: the Rust and Bun toolchains, **ClickHouse 26.7.2** (packaged in `nix/packages/clickhouse.nix`, because nixpkgs has another version), Kafka Connect, and from [continuous-backup](https://github.com/dialohq/clickhouse-continuous-backup) (a flake input, for now the local checkout `/Users/adam/continuous-backup`) the ClickHouse sink connector and the pause server. Redpanda runs in **Docker**, so Docker must be running. Local services run through [dnvr](https://github.com/dialohq/dnvr). One dev shell runs the whole stack, for the server, the engine and the examples alike.

```sh
nix develop      # or `direnv allow` once (run `direnv reload` after flake changes)
dnvr up          # starts everything in a tmux dashboard
```

`dnvr up` starts these processes; the arrows are the start-up order dnvr derives from `dnvr://` dependencies:

| process | what | where |
|---|---|---|
| `redpanda` | Redpanda in Docker (container `chlive-redpanda`, volume `chlive-redpanda`); creates topics `events`, `customers`, `orders` (3 partitions each) | Kafka `127.0.0.1:9092`, HTTP proxy `127.0.0.1:8082` |
| `clickhouse` | ClickHouse 26.7.2 (dnvr's clickhouse preset), with an embedded Keeper (for KeeperMap) | HTTP `127.0.0.1:18123`, native `127.0.0.1:19000`, Keeper `19181` |
| `schema` → clickhouse | applies `scripts/schema.sql` (tables, the sink's KeeperMap state tables, the `chlive_ro` and `chlive_snap` users) | |
| `connect` → redpanda | Kafka Connect, with the ClickHouse sink plugin | REST `127.0.0.1:18083` |
| `connectors` → connect, schema | one sink connector per topic (`chlive-sink-<topic>`, exactly once), into the table of the same name; waits until they run | |
| `pause-server` → connectors | continuous-backup's pause server: pauses a table's connector and reports its exact offsets, for snapshots | `127.0.0.1:18091` |
| `seed` → connectors | if ClickHouse has fewer than `$SEED_COUNT` events: `scripts/seed.sql` generates backdated events, produced to Kafka and ingested by the sink | |
| `server` → pause-server | chlive-server: WebSocket over chlive-engine, as `chlive_ro` (snapshots as `chlive_snap`), on its default ports | `:8080` (`/live`, `/debug/subscriptions`, `/healthz`) |
| `playground` | the example query playground | <http://localhost:5174> |

Dashboard keys: `j`/`k` selects, `Enter` shows a process (a second `Enter` focuses it), `r` restarts, `x` interrupts, `Q` stops everything, and `Ctrl-G` detaches. Running `dnvr up` again reattaches. `dnvr ps` and `dnvr logs <process>` work from any terminal. There is no live traffic generator; new events come from the playground's buttons, `produce`, or anything else producing to Kafka. The shell also has these scripts: `ch` (clickhouse client), `produce` (JSON events to a topic, e.g. `produce events '{"user_id": "u7", "latency_ms": 120}'`; or JSON lines on stdin), `engine` (the engine CLI), `e2e` (the tests that need the stack), and `reset-data` (wipe Redpanda and ClickHouse data together, since the sink's offsets in ClickHouse must match Kafka's; run it with the group stopped).

State lives in `.dnvr/` (gitignored) and in the `chlive-redpanda` Docker volume. Redpanda uses its standard ports (Kafka 9092, HTTP proxy 8082) and the server listens on 8080; ClickHouse uses 18123/19000 rather than 8123/9000, so it can run next to another local ClickHouse. These are the built-in defaults of the server, the engine CLI, the tests and the example, so nothing needs configuring; `CHLIVE_CLICKHOUSE_URL`, `CHLIVE_KAFKA` and `CHLIVE_LISTEN` override them.

### Example: query playground

`examples/playground` (plain React, Bun workspace) is served by the `playground` process at <http://localhost:5174>. Two panes:

- **Live query:** a query box with presets over `events`, `orders` and `customers` (⌘/Ctrl+Enter runs it), and the live result: newest rows first, with its status, row count and the engine's positioned error (e.g. `unknown column latency in table events (…) at 1:17`).
- **Write to Kafka:** pick a topic, write JSON by hand (one object, an array, or JSON lines) or start from a preset: a slow checkout, numbers as strings, missing keys (zero values), an unreadable value that both ClickHouse and the engine skip, a batch, a big order, a customer rename… Each write reports the partition and offset it landed at; `Random` and `50 random` write generated rows.

- **Client state** (under the query): what `@chlive/client` holds for the subscription, from `state.stats`: its wire id and how often it was (re)subscribed, the rows in state against the LIMIT and how many are rendered, counts of diffs, inserts and deletes and bytes received, the live insert rate, how long the last message took to apply, and a log of the last 30 messages (e.g. `diff: 2 changes · + -@0`); click one for its JSON.
- **Generator:** a loop that writes generated rows to a topic, then sleeps, until stopped. Pick the rows per write, the interval, and how rows are drawn: uniform, spiky latency, Poland only, lots of ties, big spenders, past timestamps, ever-rising latency, or big orders.

Writes go **straight to Kafka** through Redpanda's HTTP proxy (`127.0.0.1:8082`, proxied as `/kafka`), so you can watch each one travel Kafka → server → UI.

### Example: an event-driven Rust service

`examples/rust-alerts` (`chlive-example-rust-alerts`, run by the `rust-alerts` process; `cargo run -p chlive-example-rust-alerts` on its own) reacts to what changes in two live queries over one `chlive-client` connection, with typed rows:
- **the 5 slowest requests:** `ALERT: <user> entered the top 5 slowest at #<rank>` for each insert, and which request was pushed out (the client hands back the row that left);
- **requests per country, top 3:** each country's new count and average latency as its group updates, and its new rank when it moves.

It reconnects on its own when the server restarts (the fresh snapshots replace its rows). To see an alert, produce a request slower than the current top 5, e.g. on freshly seeded data `produce events '{"user_id": "u7", "country": "PL", "path": "/checkout", "latency_ms": 99999}'`, and watch the process's log (`dnvr logs rust-alerts`).

## How it works

Ingestion is Kafka Connect's ClickHouse sink, exactly once, which keeps no Kafka offsets in the rows. The **engine** (`engine/`, see [Engine](#engine-custom-dialect)) parses and binds the query against ClickHouse's schema, runs the history query on a **snapshot** of the table, and consumes the table's Kafka topic from right after the snapshot's offsets, decoding, filtering and projecting each event itself. Every event is in exactly one of history and stream.

A snapshot is a clone of the table taken while the pause server holds its ingestion: `/pause` reports, per partition, the next offset the sink would ingest; `CREATE TABLE … AS` + `ATTACH PARTITION ALL FROM` clones the table (it links the existing parts, so it's fast whatever the size); `/resume`. Any snapshot is correct, however old (an older one only means more Kafka to replay), so the engine's snapshot task keeps one per table in use, refreshed every 10 s while subscriptions come, stopped after 1 h without any; replaced clones are dropped once no subscription reads them (`engine/src/snapshot/`).

The **server** (`server/`) is a thin WebSocket layer: one engine subscription per client subscription, its history sent as a `snapshot` and its events as `diff`s. Subscriptions share snapshots, but each has its own Kafka consumer.

```
Kafka ──▶ Kafka Connect: ClickHouse sink ──▶ ClickHouse ◀── clone (CREATE … AS + ATTACH PARTITION)
  │                  ▲ pause / resume                 │ history query on the clone
  │             pause server ◀── snapshot task ───────┤
  │                  (offsets of the clone)           ▼
  └──────▶ chlive-server = WebSocket + chlive-engine ── snapshot, diffs ──▶ @chlive/client
   events after the clone's offsets
```

## Layout

| path | what |
|---|---|
| `flake.nix` | the dnvr dev shells: toolchain, env, the local stacks' processes (Redpanda in Docker, ClickHouse 26.7.2, Kafka Connect with the ClickHouse sink, the pause server, schema, seed, server, examples) and scripts |
| `nix/packages/` | custom derivations: `clickhouse.nix` (26.7.2), `redpanda.nix` (the Docker-run Redpanda process) |
| `scripts/` | `schema.sql` (tables, the sink's state tables, users) and `seed.sql` (historical events, as JSON to produce); |
| `engine/` | `chlive-engine` (Rust): the SQL dialect (parser, binder, planner) and the event loop over ClickHouse and Kafka; also a CLI that prints each iteration's diffs |
| `protocol/` | `chlive-protocol` (Rust): the WebSocket protocol's messages, shared by the server and the Rust client |
| `server/` | `chlive-server` (Rust): the WebSocket protocol over the engine |
| `client/` | `@chlive/client` (zero dependencies) plus the `@chlive/client/react` hook `useLiveQuery` |
| `client-rs/` | `chlive-client` (Rust): the client for services; `examples/watch.rs` prints any query's updates |
| `examples/` | example apps: `playground`, the query playground (React); `rust-alerts`, an event-driven service on `chlive-client` |
| `tests/` | end-to-end tests (`chlive-e2e`): the server over the full stack, with a small Kafka producer |

## Engine (custom dialect)

The engine is a library (used by the server) with a CLI that prints each iteration's diffs. With `dnvr up` running:

```sh
engine --explain "select * from events"     # plan: ClickHouse SQL, topics, keys, filter
engine "select * from events where latency_ms > 500"   # history, then live events until Ctrl-C
produce events '{"user_id": "u7", "latency_ms": 900}'  # one event per JSON argument
```

**History and stream meet at a snapshot's watermark.** At subscribe time the engine takes the table's current snapshot (see [How it works](#how-it-works)): a clone holding exactly the offsets below the pause server's `N[p]` per partition. History is the whole clone, and each Kafka partition is consumed from `N[p]`. So every event is in exactly one of the two, wherever ClickHouse's ingestion is at. The engine assigns partitions directly and never commits offsets, so subscriptions don't affect each other. A table must be ingested by exactly one sink connector, from the topic of the same name. The engine's ClickHouse node must be the one clones are made on (`clickhouse_url` and `clickhouse_native` point to one node); for replicated tables it runs `SYSTEM SYNC REPLICA` first.

The full grammar is under [Supported queries](#supported-queries). A condition can use:
- comparisons (`=`, `!=`/`<>`, `<`, `<=`, `>`, `>=`), with `AND`, `OR`, `NOT` and parentheses;
- `IS [NOT] NULL` and `[NOT] IN (…)`;
- operands that are columns or literals: numbers, strings, `TRUE`/`FALSE`.

Functions, arithmetic, `LIKE` and `BETWEEN` aren't supported yet.

**ORDER BY** takes columns (selected or not), each with `ASC`/`DESC` and `NULLS FIRST`/`NULLS LAST`, or `ALL`, which sorts by the SELECT list in SELECT order, as in ClickHouse. Positions, expressions, `COLLATE` and `WITH FILL` aren't supported yet. The order is **managed**:
- **Semantics match ClickHouse.** NULLs default to last in both directions; NaN sits between the values and the NULLs; strings compare by bytes; `-0` ties with `0`. The history query always spells out each key's direction and NULL placement.
- **State.** Per subscription, the engine keeps only the ORDER BY values of the result, with equal keys merged into tie groups, never the rows. Order-only columns are fetched from ClickHouse under hidden aliases (`__order_0`, …) and stripped from the output.
- **Placement.** The history arrives in ClickHouse's order. Each new matching row goes after the last row it ties with, and its insert carries that `index`. So the live list has the same sequence of keys as a fresh query; only the order of rows within a tie group can differ.
- **Tested.** `engine/tests/clickhouse.rs` checks 26 orderings (6 of them with LIMIT, 6 with OFFSET) (NULL, NaN, ±inf, `-0`, byte-order strings, DateTime precision, Bool, duplicates, DESC, NULLS FIRST/LAST, several keys, `ALL`, an unselected column, with WHERE) in the same three setups as WHERE.

**LIMIT n** (a whole number; `WITH TIES` and `LIMIT … BY` aren't supported yet) goes into the history query and bounds the result:
- **With ORDER BY: the top N.** The state holds at most N sort keys. A new row placed inside the top N is inserted at its index, and the row pushed past the end is deleted (`delete @N`). A row placed at or past the end, including one tying with the last row, changes nothing and sends nothing. Which rows of the boundary tie group make it in is arbitrary, as it is in ClickHouse.
- **Without ORDER BY: a window of the latest N rows.** History is whatever N rows ClickHouse returns. Each new matching row is appended, and once the window is full, the first row is deleted (`delete @0`). Only a row count is kept.
- **Atomic diffs.** An insert and the delete it causes are in the same iteration, so always in the same `diff`. The result has at most N rows after every diff, and the client only ever exposes whole diffs.

**OFFSET m** (`LIMIT n OFFSET m`, ClickHouse's `LIMIT m, n`, or `OFFSET m` alone) starts the result after m rows. The history query fetches `LIMIT m + n` (everything without LIMIT) and the engine keeps the first m rows itself, because they can come into the result later:
- **With ORDER BY: ranks m to m + n.** The state holds the sort keys of the top m + n, and the rows of the top m. A row landing above the result shifts it down: the row at rank m slides in at the top (`insert @0`), and, with LIMIT, the last one leaves (`delete @n`). A row landing inside is inserted at its index, as with LIMIT alone; one below changes nothing. Which rows of the tie groups at either boundary make it in is arbitrary, as in ClickHouse.
- **Without ORDER BY: a sliding window that holds back the newest m rows.** The arrival order is the history in ClickHouse's order, then the stream. The newest m rows are held back, and the result is the n rows before them. Each new row is held back, the oldest held-back row is appended to the result, and once the result has n rows, the first one leaves (`delete @0`). With `OFFSET 0`, this is LIMIT's window.
- **Memory.** A subscription holds m rows (plus m + n sort keys when ordered), whatever m is.
- **Not resumable yet:** a `since` gives a full snapshot.

**WHERE runs in two places, which must agree on every row.** ClickHouse evaluates it for history; the engine evaluates it for each event.
- **Typed decoding.** Each event's needed JSON fields are decoded by their ClickHouse column types, following ClickHouse's input rules: a quoted number is read as a number, and a DateTime string is parsed.
- **Missing keys.** A missing key is NULL for a `Nullable` column, and the type's zero value otherwise, just as ClickHouse's ingestion stores it.
- **Conditions.** They're evaluated with SQL's three-valued logic.
- **Literals.** They're typed and checked at bind time; for example, comparing `latency_ms` with a string is a positioned error.
- **Output.** Event rows are rendered the way ClickHouse renders rows.
- **Tested.** The agreement test in `engine/tests/clickhouse.rs` checks, for 22 predicates, that three setups return the same rows: everything in history; everything from the stream; and half from each, split at the snapshot (real clones, with a stand-in for the pause server). `engine/tests/kafka.rs` produces events before, around and after subscribing, against the real Redpanda and ClickHouse, and checks that each arrives exactly once.

The query goes through parser → binder → planner. The binder resolves FROM, then WHERE, then SELECT, in SQL's evaluation order:
- **FROM:** the table must exist in ClickHouse and have a Kafka topic. Snapshots (`__chlive_snap_*`) aren't tables.
- **SELECT:** listed columns must exist. `col AS name` renames a column in the output; a column can be selected more than once under different names, but output names must be unique. An alias can't be another column's name (in ClickHouse it would shadow that column in WHERE and ORDER BY) or start with `_` (Kafka columns and the engine's hidden fields). Aliases only name output columns: WHERE and ORDER BY refer to table columns.

The planner selects only the needed columns from ClickHouse, and the stream decodes only the needed JSON keys of each event; the other keys are skipped unparsed. Hard-coded naming rules map table `t` to ClickHouse `default.t` and to topic `t`, and keep column names the same on both sides; columns starting with `_` are hidden.

Each iteration prints its diff: first the history (`+ {row}` per ClickHouse row, then `history: N rows`), then one line per relevant event of the subscribed topics, as `<topic>/<partition>:<offset> + {…}`, e.g. `events/0:21 + {…}`. Only the query's topics are consumed.

## Client

```ts
// React
import { useLiveQuery } from "@chlive/client/react";
const { status, rows, error } = useLiveQuery<Event>(sql); // error: { code, message }

// without React
import { LiveClient } from "@chlive/client";
const live = new LiveClient({ url: "ws://localhost:8080/live", cache: true });
const sub = live.subscribe(sql, (s) => render(s.status, s.rows, s.error));
sub.unsubscribe();
```

`status` is `loading` (nothing yet), `cached` (rows from the offline cache, being caught up), `live` or `error`. Rows are exactly the selected columns, in the query's order, so columns come from the rows themselves; the row type is the hook's type parameter. ORDER BY and LIMIT are already applied to `rows`. One socket carries all of a client's subscriptions; it reconnects with backoff and resubscribes from where it was.

For dev tools, `useLiveQueryDebug` (and `subscribe`'s listener) also give `stats`: per-subscription counters (snapshots, diffs, inserts, deletes, bytes, the last message and how long it took to apply, what the cache did).

### Rust client

`chlive-client` (`client-rs/`) does what `@chlive/client` does, for services: one WebSocket (tokio, `ws://` or `wss://`) shared by any number of subscriptions, results kept by applying each diff atomically, and reconnecting with backoff. It has no offline cache and doesn't resume: after a reconnect, a fresh snapshot replaces the rows.

```rust
use chlive_client::{Change, ClientOptions, LiveClient, Status};
use futures::StreamExt;

#[derive(serde::Deserialize)]
struct Slow { user_id: String, latency_ms: u32 }

let client = LiveClient::new(ClientOptions::new("ws://127.0.0.1:8080/live"));
let mut sub = client.subscribe::<Slow>("select user_id, latency_ms from events order by latency_ms desc limit 5");
while let Some(u) = sub.next().await {
    if let Status::Error(e) = &u.status { eprintln!("{e}"); break; }
    for change in &u.changes {
        match change {
            Change::Insert { row, index, .. } => println!("{} is now #{}", row.user_id, index.unwrap_or(0) + 1),
            Change::Delete { row, .. } => println!("{} dropped out", row.user_id),
            Change::Update { id, row, index } => {} // aggregated rows, by group key
        }
    }
    // u.rows: the whole result after this update
}
```

- **A subscription is a `Stream` of `Update`s.** Each has a `kind`: `Subscribed` (the first), `Snapshot` (the whole result; again after each reconnect), `Diff`, `Disconnected` (the rows are kept until the next snapshot), or `Error` (the last; the stream ends). Each also has a `status` (`Loading`, `Live` or `Error { code, message }`), `rows` (the result after it), `changes`, `columns`, the cursor `at`, and `stats` (as in TS, without the cache's).
- **Changes are resolved against the client's rows.** A `Delete` carries the row that left (the wire only has its index), and an `Update` carries the whole row after it, not just the changed columns.
- **Rows** are deserialized into the type you subscribe with, once per row. `subscribe::<Row>` keeps them as JSON objects. A row that doesn't fit the type fails the subscription with `CLIENT_ERROR`, as an unknown change op or an index outside the result does, and the rows stay as they were before that diff.
- **Backpressure.** Messages are applied when the stream is polled. A subscription that isn't read fills a small buffer, and then the connection waits for it. Nothing is dropped, and the server just falls behind on Kafka. Subscriptions sharing a client should all be read, e.g. with `tokio::select!` as in `examples/rust-alerts`. Use a client per subscription where one might lag.
- **Cancelling:** dropping a subscription unsubscribes; `client.close().await` ends every stream.

### Protocol

JSON text frames on `/live` (`protocol/src/lib.rs`, the `chlive-protocol` crate the server and the Rust client share; mirrored by `client/src/protocol.ts`). Ids are the client's, per connection:

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
- `{"op": "delete", "index": k}` removes the row at `k`; the rows after it move up. With LIMIT (`"limit": n` in the snapshot), each insert that overflows the result is followed by the delete it causes. With OFFSET (`"offset": m` in the snapshot, informational), the diffs already account for the rows above the result: a row sliding in from above is an ordinary insert.
- A `diff` is atomic: its changes apply in sequence, each index into the list as the previous change left it, and only the state after all of them is meaningful (in between, an insert and its delete can briefly make one row more than the LIMIT). The client applies a whole diff to one copy of the rows before its listeners see it.
- **Resuming.** `snapshot`, `diff` and `{"type": "progress", "id", "at"}` carry `at`, an opaque cursor: how far the result is complete, per Kafka partition, counting events that didn't match. A client that kept a result subscribes with `"since": <at>`; the server answers `{"type": "resume", "id", "columns", "order"?, "limit"?}` instead of a snapshot (the client's rows are still valid), then diffs with only what was missed. If it can't resume (another query or schema, a topic recreated since, …), it sends a normal `snapshot`.
- Error codes: `SYNTAX_ERROR` and `INVALID_QUERY` (the query; positioned), `HISTORY_FAILED` (ClickHouse), `STREAM_FAILED` (Kafka), `AGGREGATE_FAILED` (chDB), `SNAPSHOT_FAILED` (the pause server, or cloning), `BAD_MESSAGE` (unparseable, no id), `DUPLICATE_ID`. After an error, that subscription is over.
- Rows are exactly the selected columns, rendered like ClickHouse's JSON output.

### GROUP BY and aggregates

`SELECT keys, agg(c) [AS name], … FROM t [WHERE …] [GROUP BY keys] [ORDER BY …] [LIMIT n] [OFFSET m]`, with ClickHouse's result types and output names (see [Supported queries](#supported-queries)). Functions: `count()`, `count(c)`, `sum`, `avg`, `min`, `max` (native), and `uniq`, `uniqExact`, `median`, `any`, `anyLast` (chDB). Not yet: HAVING, expressions as arguments, and resuming aggregates (they start from a snapshot).

- **Native** when every aggregate in the query has a native implementation (`engine/src/aggregate/`: the `AggregateFunction` and `AggregateState` traits). ClickHouse computes the parts of each group's state over HTTP (`avg(x)` → `sum(x)`, `count(x)`), and the engine keeps the states in memory and updates them per event. `--explain` says which way a query runs.
- **chDB** (ClickHouse as a library, inside the engine) otherwise. Main ClickHouse computes the history's aggregate states (`uniqExactState(x)`, …) with `remote()` into a chDB table per subscription; each batch of events (up to 1000 that arrived together) adds its states, and the groups it touched are read back finalized with `…Merge`.
- Either way, the groups touched by a batch are compared with what was sent.
- **Rows have ids**, their group's key as JSON (`["PL"]`, `[]` for a global aggregate). The snapshot carries `ids`; new groups are `insert`s with an `id`; changed ones `{"op": "update", "id", "set": {…}}` with only the changed columns, and, when an ordered result moves the row, `"index"`: where it is after the move.
- **Cost:** see [docs/bench/group-by.md](docs/bench/group-by.md). Natively ~1 M events/s per subscription one at a time, up to several M/s batched. chDB costs ~2–5 ms per batch, so ~500 events/s one at a time and ~150–500 k/s in batches of 1000.
- Binaries call `chlive_engine::chdb::shutdown()` before exiting: with chDB open, its exit handlers crash the process.

### IN and EXISTS sub-queries

`SELECT … FROM events WHERE user_id IN (SELECT customer_id FROM customers WHERE country = 'PL')`: both tables are append-only, so the sub-query's keys (`S`) only grow, and outer rows only enter the result. A correlated `EXISTS (SELECT 1 FROM customers c WHERE c.customer_id = e.user_id AND c.country = 'PL')` is bound to exactly this; an uncorrelated `EXISTS` is the same with one constant key, so the first matching inner row lets every outer row in.

- **History** is ClickHouse running the whole query over snapshots of both tables; `S` is read from the inner snapshot (`SELECT DISTINCT …`).
- **Then both topics.** An inner event that passes the sub-query's WHERE adds its key to `S`. An outer event whose key is in `S` is placed as usual; one whose key isn't yet is **kept back**, by key.
- **When a key enters `S`**, the outer rows of that key the result skipped come in, once each: those up to the outer table's latest snapshot from that snapshot (one query per batch of entering keys), those after it from the kept-back rows.
- **The kept-back rows stay few:** every refresh interval (10 s), the subscription moves to the outer table's newest snapshot, once its stream is past it, and drops the kept-back rows that snapshot holds. So the state is the keys of `S` plus about one refresh interval of filtered, projected outer events. (Not before the stream is past it: back-filling from a snapshot ahead of the stream would send rows twice.)
- An event of the same table in both roles counts for the sub-query first, so an event that brings its key into `S` is itself in the result, as in ClickHouse.
- **Not yet:** resuming (a `since` gives a full snapshot), `NOT IN` (rows would have to leave the result), GROUP BY over a semi-join. Back-fills are cheap when the outer table's sort key (or a skip index) covers the key column.
- In the dev stack, `customers` is a natural sub-query table: `produce customers '{"customer_id": "u7", "country": "PL"}'` brings u7's events into a query like the one above.

### Offline cache

`new LiveClient({ url, cache: true })` keeps results between sessions in SQLite (sqlite-wasm) on OPFS, in a dedicated worker, using the `opfs-sahpool` VFS, so no COOP/COEP headers are needed. `cache: { budgetBytes }` sets its size (2 GB by default); `live.clearCache()` empties it (e.g. on logout). It's loaded only when enabled (`@sqlite.org/sqlite-wasm` is an optional peer dependency).

The client takes care of the page's lifecycle: it saves pending results when the tab goes to the background or the page is hidden, then closes the database so a reload can open it at once, and reopens it if the page comes back from the back/forward cache.

- **On subscribe**, the cached result (if any) is shown at once (`status: "cached"`, with `cachedAt`), and the subscription is sent with `since` = its cursor. The server resumes and sends only what was missed; reconnects resume the same way.
- **On the server** (`Engine::subscribe_from`), resuming from cursor `C`; the table's current snapshot holds offsets up to `W`:
  - Unordered, unlimited: nothing to rebuild and no ClickHouse at all; the stream continues from `C`, replaying from Kafka what the client missed.
  - With ORDER BY (or LIMIT alone), the server rebuilds the client's state: the snapshot's sort keys (or row count), then the client's own events between `W` and `C`, re-read from Kafka and not sent again; then the stream continues from `C`. This needs `C ≥ W` in every partition; a client behind the snapshot gets a full snapshot instead (rare: snapshots are recent, and clients usually stream past them).
  - The cursor must fit the topic: past its end means the topic was recreated, and before the start of what the resume reads means retention deleted it. Either way the server sends a full snapshot instead.
- **What's stored:** per query (the SQL text, for now), its rows as JSON chunks, the cursor and when it was last used. A database with an older layout is emptied (`PRAGMA user_version`). Each write is one transaction: snapshots and ordered diffs are saved in full (batched, about 1 s), append-only diffs are appended, and `progress` moves the cursor.
- **GC:** above the budget, the least recently used queries are evicted.
- **MVP limits:** one tab holds the database at a time; other tabs run without a cache (the playground shows why). Keys are query strings, not shapes yet.

The playground uses it: the "Client state" panel shows a hit or miss, whether the server resumed and how big the catch-up was, the cursor, and the store's size, with a "Clear cache" button.

## Tests

```sh
cargo test --workspace     # engine (lexer, parser, binder, planner, types, watermark), server protocol over a real WebSocket,
                           # the protocol's messages, the Rust client's result store and connection (against a fake server)
bun run test               # client state, reconnect/resume, the cache store on bun:sqlite (bun test)
e2e                        # = CHLIVE_E2E=1 cargo test -p chlive-engine -p chlive-e2e, against `dnvr up`
```

`e2e` runs the engine's ClickHouse and Kafka tests, then the server end to end. The latter subscribe over WebSocket while producing events, and check that each live result (snapshot plus inserts) equals the same query run directly on ClickHouse once everything is ingested; one does so through the Rust client. They count rows across the whole `events` table, so don't produce other events (e.g. with the playground's buttons) while they run.

## Supported queries

The engine's dialect, in full (keywords are case-insensitive):

```
SELECT * | select_item [, select_item ...]
FROM table [[AS] alias]
[WHERE condition]
[GROUP BY column [, column ...]]
[ORDER BY ALL [ASC|DESC] [NULLS FIRST|LAST] | order_item [ASC|DESC] [NULLS FIRST|LAST] [, ...]]
[LIMIT n [OFFSET m] | LIMIT m, n | OFFSET m]
[;]

order_item  := column
             | aggregate(column) | count() | count(*)        -- with GROUP BY or aggregates

select_item := column [AS alias]
             | aggregate(column) [AS alias]
             | count() | count(*) [AS alias]
             | literal                                        -- only in EXISTS sub-queries
aggregate   := count | sum | avg | min | max                   -- native, in the engine
             | uniq | uniqExact | median | any | anyLast       -- through chDB
condition   := condition OR condition | condition AND condition | NOT condition | (condition)
             | operand {= | != | <> | < | <= | > | >=} operand
             | operand IS [NOT] NULL
             | operand [NOT] IN (literal [, literal ...])
             | column IN (SELECT column FROM table [WHERE condition])   -- see below
             | EXISTS (SELECT … FROM table [[AS] alias] [WHERE inner.col = outer.col] [AND condition …])
             | operand
operand     := column | literal
literal     := number | -number | 'string' | TRUE | FALSE | NULL
column      := [table_or_alias.]name
name, table, alias := identifier | "identifier" | `identifier`
```

- **Tables:** a table needs a ClickHouse table, ingested by one sink connector (known to the pause server) from a Kafka topic of the same name.
- **Table aliases and qualified names:** `FROM events e` (or `AS e`); `e.user_id` or `events.user_id` anywhere a column goes. In a sub-query, an unqualified name is its own table's column first, then the outer query's.
- **Aliases** only name output columns: WHERE and (plain queries') ORDER BY refer to table columns. Output names must be unique; an alias can't be another column's name or start with `_`.
- **ORDER BY / LIMIT:** with `ORDER BY`, each insert lands at its index; without it, rows arrive in Kafka order per partition after the snapshot. `LIMIT` keeps the top N (with `ORDER BY`) or the latest N (without), deleting the rows that fall out. Without `LIMIT`, plain results only grow. `OFFSET m` starts after the top m (with `ORDER BY`; rows above shift the result) or holds back the newest m (without); see [OFFSET](#engine-custom-dialect).
- **GROUP BY / aggregates** (also without GROUP BY: one global group): selected columns must be grouping columns; no `SELECT *`. Groups are inserted, then updated.
  - **ORDER BY** takes an output name (an alias or a default name like `count()`), a grouping column, or an aggregate call (`order by count() desc`; one that isn't selected is computed anyway), or `ALL`. As values change, groups move: an `update` with an `index`. A group whose sort key changes goes after the groups it ties with.
  - **LIMIT n** keeps the first n groups by ORDER BY: a group falling out is deleted and the next one comes in, and the others are kept to come back in later. Without ORDER BY, it's the first n groups seen.
  - **OFFSET m** starts the result after the first m groups: with ORDER BY, groups come in and go out at both ends as they move; without, it's the groups after the first m seen.
- **EXISTS (subquery)**: correlated (`… WHERE c.customer_id = e.user_id AND …`) it is the same semi-join as the matching `IN`: exactly one `inner = outer` equality, a top-level `AND` condition of the sub-query, is the key; the rest of its WHERE may only use its own table. Uncorrelated (no such equality), it's true for every row once the sub-query has a row: until then outer rows are kept back, and then they all come in. Its SELECT list doesn't matter (`SELECT 1`, `SELECT *`).
- **IN (subquery)**: one per query (IN or EXISTS), as a top-level `AND` condition of the WHERE, over plain queries (with ORDER BY, LIMIT and OFFSET); the sub-query selects one column of one table, with an optional WHERE (the same table as the outer one is fine). See [IN and EXISTS sub-queries](#in-and-exists-sub-queries).
- **Not yet:** `WITH TIES`, `LIMIT … BY`, `DISTINCT`, HAVING, joins, `NOT IN (subquery)`, `NOT EXISTS`, EXISTS with several correlations (composite keys), constants as output columns, and other sub-queries, IN sub-queries with GROUP BY, expressions and other functions (arithmetic, `LIKE`, `BETWEEN`), positions in ORDER BY/GROUP BY, parameters.
