# chlive

A minimal **query-driven live sync layer** for a Kafka → ClickHouse analytics stack, in the spirit of Zero and ElectricSQL. A client subscribes with a query in chlive's SQL dialect. It gets the current result right away, then the changes over WebSocket as new Kafka events arrive. History is read from ClickHouse once per subscription; after that only new events are processed.

## Quickstart

Everything except Redpanda comes from the nix flake: the Rust and Bun toolchains, **ClickHouse 26.7.2** (packaged in `nix/packages/clickhouse.nix`, because nixpkgs has another version), Kafka Connect, and from [continuous-backup](https://github.com/dialohq/clickhouse-continuous-backup) (a flake input, its `pause-server` branch for now) the ClickHouse sink connector and the pause server. Redpanda runs in **Docker**, so Docker must be running. Local services run through [dnvr](https://github.com/dialohq/dnvr). One dev shell runs the whole stack, for the server, the engine and the examples alike.

```sh
nix develop      # or `direnv allow` once (run `direnv reload` after flake changes)
dnvr up          # starts everything as a Rex session, a tab per process
```

`dnvr up` starts these processes; the arrows are the start-up order dnvr derives from `dnvr://` dependencies:

| process | what | publishes |
|---|---|---|
| `redpanda` | Redpanda in Docker (container `chlive-redpanda`, volume `chlive-redpanda`); creates topics `events`, `customers`, `orders` (3 partitions each) | `bootstrap` (Kafka), `proxyUrl` (HTTP proxy), `topics` |
| `clickhouse` | ClickHouse 26.7.2 (dnvr's clickhouse preset), with an embedded Keeper (for KeeperMap) | `host`, `httpPort`, `tcpPort`, `httpUrl`, then `database` once it answers |
| `schema` → clickhouse | applies `scripts/schema.sql` (tables, the sink's KeeperMap state tables, the `chlive_ro` and `chlive_snap` users) | |
| `connect` → redpanda | Kafka Connect, with the ClickHouse sink plugin | `url` (REST) |
| `connectors` → connect, schema, clickhouse | one sink connector per topic (`chlive-sink-<topic>`, exactly once), into the table of the same name; waits until they run | |
| `pause-server` → connectors | continuous-backup's pause server: pauses a table's connector and reports its exact offsets, for snapshots | `url` |
| `seed` → connectors, clickhouse, redpanda | if ClickHouse has fewer than `$SEED_COUNT` events: `scripts/seed.sql` generates backdated events, produced to Kafka and ingested by the sink | |
| `server` → pause-server, clickhouse, redpanda | chlive-server on `nix/chlive.dev.toml` (the three tables, snapshots in `chlive_snapshots`): WebSocket over chlive-engine, as `chlive_ro` (snapshots as `chlive_snap`) | `url` (the WebSocket, `/live`), `httpUrl` (`/debug`, `/debug/state`, `/debug/subscriptions`, `/healthz`) |
| `playground` → server, redpanda | the example query playground | `url` |
| `rust-alerts` → server | the Rust client's example service | |

`dnvr up` runs the stack in [Rex](https://www.superlogical.com/rex) (the `rex` CLI must be on PATH; `dnvr up --runner tmux` runs it in a tmux dashboard instead): a session `chlive@<branch>:default` with one tab per process, kept open after its process exits. Running `dnvr up` again goes back to it. From any terminal: `dnvr ps`, `dnvr logs <process>` (`-f` follows), `dnvr restart <process>`, and `dnvr down` to stop everything. There is no live traffic generator; new events come from the playground's buttons, `produce`, or anything else producing to Kafka. The shell also has these scripts: `ch` (clickhouse client), `produce` (JSON events to a topic, e.g. `produce events '{"user_id": "u7", "latency_ms": 120}'`; or JSON lines on stdin), `tui` (live queries as a table in the terminal), `e2e` (the tests that need the stack), `bench` (the query benchmark), `wasm` (builds `chlive-wasm` into `client/wasm/`), and `reset-data` (wipe Redpanda and ClickHouse data together, since the sink's offsets in ClickHouse must match Kafka's; run it with the group stopped).

Every process picks free ports on each start (`dnvr-state pick-port`), so the stack runs next to anything else, and publishes its addresses; the processes get each other's through `dnvr://<process>/<key>` refs. From a terminal, read them with `dnvr-state get <process>.<key>` (e.g. `dnvr-state get playground.url`, `dnvr-state get server.url`), or list everything with `dnvr-state dump`. The `ch`, `produce`, `tui`, `e2e` and `bench` scripts look them up the same way; `CHLIVE_CLICKHOUSE_URL`, `CHLIVE_KAFKA` and `CHLIVE_PAUSE_SERVER` (and `PROXY_URL` for `produce`, `CHLIVE_URL`/`CH_URL`/`PROXY_URL` for `bench`) override them.

State lives in `.dnvr/` (gitignored) and in the `chlive-redpanda` Docker volume.

### Example: query playground

`examples/playground` (plain React, Bun workspace) is served by the `playground` process, at `dnvr-state get playground.url`. Two panes:

- **Live query:** a query box with presets over `events`, `orders` and `customers` (⌘/Ctrl+Enter runs it), and the live result: newest rows first, with its status, row count and the engine's positioned error (e.g. `unknown column latency in table events (…) at 1:17`).
- **Write to Kafka:** pick a topic, write JSON by hand (one object, an array, or JSON lines) or start from a preset: a slow checkout, numbers as strings, missing keys (zero values), an unreadable value that both ClickHouse and the engine skip, a batch, a big order, a customer rename… Each write reports the partition and offset it landed at; `Random` and `50 random` write generated rows.

- **Client state** (under the query): what `@chlive/client` holds for the subscription, from `state.stats`: its wire id and how often it was (re)subscribed, the rows in state against the LIMIT and how many are rendered, counts of diffs, inserts and deletes and bytes received, the live insert rate, how long the last message took to apply, and a log of the last 30 messages (e.g. `diff: 2 changes · + -@0`); click one for its JSON.
- **Generator:** a loop that writes generated rows to a topic, then sleeps, until stopped. Pick the rows per write, the interval, and how rows are drawn: uniform, spiky latency, Poland only, lots of ties, big spenders, past timestamps, ever-rising latency, or big orders.

Writes go **straight to Kafka** through Redpanda's HTTP proxy (proxied as `/kafka`), so you can watch each one travel Kafka → server → UI.

### Example: an event-driven Rust service

`examples/rust-alerts` (`chlive-example-rust-alerts`, run by the `rust-alerts` process; `cargo run -p chlive-example-rust-alerts` on its own) reacts to what changes in three live queries over one `chlive-client` connection, with typed rows, each through the client's view that fits:
- **the 5 slowest requests** (`inserts`, each row entering the result): `ALERT: <user> entered the top 5 slowest` for each;
- **how many requests there are** (`cell`, one value kept current): the count as it changes;
- **requests per country, top 3** (`raw`, every update): each country's new count and average latency as its group updates, and its new rank when it moves.

The client's other views are `rows`, `first` / `last`, `exists`, `deletes`, `history` (the result now, then each row inserted after it) and the one-off `fetch`. It reconnects on its own when the server restarts, resuming each query from its cursor (or taking a fresh snapshot when the server can't). To see an alert, produce a request slower than the current top 5, e.g. on freshly seeded data `produce events '{"user_id": "u7", "country": "PL", "path": "/checkout", "latency_ms": 99999}'`, and watch the process's log (`dnvr logs rust-alerts`).

`examples/chat` is a group-chat demo, two users side by side, with its own stack and schema, in its own dev shell: `nix develop .#chat`, then `dnvr up`. Each user sees the messages they're permitted to see, by row-level permissions followed live from Postgres; see [examples/chat/README.md](examples/chat/README.md).

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
| `flake.nix` | the flake: packages (`nix build` builds `chlive`: the server `chlive` and the terminal client `clc`) and the dnvr dev shell |
| `nix/shell.nix` | the dnvr dev shell: toolchain, env, the local stack's processes (Redpanda in Docker, ClickHouse 26.7.2, Kafka Connect with the ClickHouse sink, the pause server, schema, seed, server, examples) and scripts |
| `nix/chlive.dev.toml` | the dev server's config (the stack's addresses come as flags) |
| `nix/packages/` | custom derivations: `chlive.nix` (the server and `clc`), `clickhouse.nix` (26.7.2), `libchdb.nix` (chDB, same version), `redpanda.nix` (the Docker-run Redpanda process) |
| `scripts/` | `schema.sql` (tables, the sink's state tables, users) and `seed.sql` (historical events, as JSON to produce); |
| `core/` | `chlive-core` (Rust, no IO; also WebAssembly): the dialect (parser), bound queries, plans, a live result's state as events come (ordering, windows, FINAL, time windows, native aggregates), the diffs it makes, and a client's result rows |
| `engine/` | `chlive-engine` (Rust): binding against the catalog, and the event loop over ClickHouse and Kafka, on `chlive-core` |
| `wasm/` | `chlive-wasm`: `chlive-core` for the TS client (`wasm` builds it into `client/wasm/`); see `docs/bench/wasm.md` |
| `protocol/` | `chlive-protocol` (Rust): the WebSocket protocol's messages, shared by the server and the Rust client |
| `server/` | `chlive-server` (Rust; binary `chlive`): the WebSocket protocol over the engine, and its config (`chlive.example.toml`: every key and default) |
| `client/` | `@chlive/client` (zero dependencies) plus the `@chlive/client/react` hook `useLiveQuery` |
| `client-rs/` | `chlive-client` (Rust): the client for services, with views of a query (raw updates, a row or cell kept current, each insert or delete, history then new rows, one-off fetches); `examples/watch.rs` prints any query's updates |
| `examples/` | example apps: `playground`, the query playground (React); `rust-alerts`, an event-driven service on `chlive-client`; `chat`, the permissions demo (its own dev shell) |
| `tui/` | `chlive-tui` (Rust; binary `clc`): a terminal UI on `chlive-client`; type a query, watch its result as a table updated on every diff |
| `tests/` | end-to-end tests (`chlive-e2e`): the server over the full stack, with a small Kafka producer |
| `bench/` | `queries.ts`: a ladder of queries, simple to most complex, benchmarked through `@chlive/client` against the stack; `wasm.ts`: `chlive-wasm` against the TS client's store; results in `docs/bench/` |

## Server configuration

`chlive` reads its settings from built-in defaults, then a TOML file (`--config chlive.toml`, or `CHLIVE_CONFIG`), then environment variables, then flags; each layer overrides the one before. [`server/chlive.example.toml`](server/chlive.example.toml) lists every key with its default (a test keeps it exact), and `nix/chlive.dev.toml` is the dev stack's.

```toml
[server]
listen = "0.0.0.0:8080"
allowed_origins = ["https://app.example.com"]   # browsers; clients without an Origin always connect

[clickhouse]
url = "http://clickhouse:8123"                  # everything goes over HTTP(S)
password_env = "CHLIVE_RO_PASSWORD"             # or password, or password_file
snapshot_password_file = "/run/secrets/chlive_snap"
snapshot_database = "chlive_snapshots"          # clones go there instead of next to each table

[kafka]
brokers = "kafka-1:9092,kafka-2:9092"
[kafka.properties]                              # librdkafka's, for every client
"security.protocol" = "SASL_SSL"

[pause_server]
url = "http://pause-server:18091"

[tables]                                        # only listed tables can be queried (expose = "listed")
[tables.events]                                 # default.events, topic events, columns as they are
[tables.orders]
clickhouse = "shop.orders_v2"
topic = "prod.orders"
hidden_columns = ["internal_*"]                 # besides the global ["_*"]
[tables.orders.columns]
customer = { clickhouse = "customer_id", json = "customerId" }
```

- **Sections:**
  - `[cache]`: shared results, see [Shared results](#shared-results);
  - `[server]`: `listen`, `path` (`/live`), `allowed_origins`, `max_connections` (10 000; more get 503), `max_subscriptions_per_connection` (100; more get `TOO_MANY_SUBSCRIPTIONS`), `progress_interval` (1 s), `complete_wait` (30 s; how long a complete snapshot waits for its result), `max_insert_rows` (1000) and `max_inserts_in_flight` (100 per connection);
  - `[clickhouse]`: `url` (HTTP or HTTPS; the only way the server talks to ClickHouse), `user` (`chlive_ro`) and `snapshot_user` (`chlive_snap`) with their passwords, and `snapshot_database`;
  - `[kafka]`: `brokers`, `metadata_refresh` (30 s) and `properties`;
  - `[pause_server]`: `url`;
  - `[snapshots]`: `refresh` (10 s) and `idle` (1 h);
  - `[log]`: `level` (an `EnvFilter` directive; `RUST_LOG` wins when set) and `format` (`text` or `json`);
  - `[tables]`: see below.
- **Durations** are written `"250ms"`, `"10s"`, `"5m"` or `"1h"`. Unknown keys are errors, so a typo stops the server at start, with the key named.
- **Env vars** are `CHLIVE_<SECTION>__<KEY>`, with `__` for each level, e.g. `CHLIVE_KAFKA__BROKERS`, `CHLIVE_SERVER__MAX_CONNECTIONS=500` or `CHLIVE_TABLES__EVENTS__TOPIC=prod.events`. Values are read as TOML when they can be (`500`, `["a", "b"]`). Only names whose first part is a section count: `CHLIVE_URL`, `CHLIVE_E2E` and the test scripts' `CHLIVE_CLICKHOUSE_URL` aren't config.
- **Flags:** `--set key.path=value` (repeatable; the value as TOML, or a string), and the shortcuts `--listen`, `--clickhouse-url`, `--kafka` and `--pause-server`.
- **Passwords** come as is (`password`), from an env var (`password_env`) or from a file (`password_file`, as Docker and Kubernetes mount secrets; a trailing newline is trimmed). Each user takes at most one of the three; none means an empty password.
- **`chlive config show`** prints the effective config, each value with where it came from (`default`, `file`, `env CHLIVE_…` or `flag`), passwords hidden. `chlive config check [FILE]` checks a file.
- **Tables.** `expose = "listed"` (the default) allows only the tables listed as `[tables.<name>]`, so a new ClickHouse table is never queryable by accident; `"all"` allows every table in `database` too. Each listed table can override:
  - `clickhouse`: `database.table`, or `table` in `database` (default: its name in `database`);
  - `topic` (default: the `topic` template, `{table}`, where `{table}` and `{database}` are replaced);
  - `hidden_columns`: globs, added to the global ones (`["_*"]`);
  - `columns`: renames, by the name queries use; `clickhouse` defaults to that name, and `json` (the key in the topic's events) to the ClickHouse name. Or, instead of `json`, `kafka`: the message's `"offset"`, `"partition"` or `"timestamp"` (see [Kafka columns](#kafka-columns)).
  - `insert`: whether clients may insert rows, and whose (see [Inserts](#inserts)).
- **`snapshot_database`** must exist, with the same storage policy as the tables, since ATTACH PARTITION links their parts. The snapshot user needs CREATE TABLE, ALTER, INSERT, SELECT and DROP TABLE there, and the history user SELECT. A separate, non-Replicated database also keeps the clones' DDL off a Replicated database's log.
- **Kafka properties** are librdkafka's (TLS, SASL, …). They override chlive's own consumer settings, except `enable.auto.commit` and `enable.auto.offset.store`, which stay off since chlive never commits.

### Inserts

Clients can write too, through the server, so an app needs chlive's connection and no Kafka credentials: `client.insert(table, rows)` (TS and Rust). The server checks the rows against the user's write permissions and the table, and produces them to the table's topic. The sink stores them, and every subscription sees them like any other event. The ack holds each row's Kafka partition and offset.

```toml
[tables.messages.insert]                        # without this section, a table takes no inserts
user = "user_id"                                # set by the server to the `hello` user, never by a row
key = "chat_id"                                 # the Kafka key: one chat's rows in order, in one partition
# who may insert what: each its own grant, like reads' (`[security.providers]`)
table = { provider = "workos", grants = "messages:write" }                                   # at all
rows = { column = "chat_id", provider = "pg", grants = "SELECT chat_id FROM members WHERE user_id = {subject:String}" }
columns = { provider = "pg", grants = "SELECT column_name FROM writable WHERE user_id = {subject:String}" }
# public = true                                 # anyone, anonymous too (no user, no grants)
```

- **Opt-in per table**, whatever the read security: a table without `[tables.<t>.insert]` refuses inserts (`PERMISSION_DENIED`). With the section, either `public = true` or every insert needs a user (`hello`).
- **Rows are checked before anything is produced.** Each key is a column of the table, by the name queries use; not a hidden one, and not one filled from Kafka. Each value must read as its column's type, strictly: NULL only in a Nullable column, an integer only within its type's range. One bad row refuses the whole insert (`INVALID_INSERT`, naming the row).
- **The message** holds each value as ClickHouse renders it (`"2026-11-01 12:00:00.250"`, a dashed UUID), under the column's JSON key, so what ClickHouse stores and what the stream decodes are the same. Columns a row leaves out stay out: ClickHouse stores their DEFAULT, and the stream reads NULL or the type's zero, as for any message.
- **Grants** are the read grants' kinds, with their own queries or permissions: `table` (inserts allowed while it grants anything), `rows` (the row's `column` must be one of the values granted), and `columns` (the columns it may set). They're asked on each insert.
- **Not atomic.** The rows of one insert are produced together, each acknowledged by the brokers (idempotently). If Kafka fails partway, `WRITE_FAILED` says which rows went in.
- **Updates and deletes are inserts**, by the table's own convention: a new version of a key in a ReplacingMergeTree (with `ReplacingMergeTree(kafka_offset)`, Kafka orders the versions; see [Kafka columns](#kafka-columns)), with its is-deleted column set to delete. FINAL queries then show the latest.

### Kafka columns

A table can also store each message's **partition**, **offset** and **timestamp**, which Kafka Connect's `InsertField$Value` transform adds to the row before the sink writes it. The event JSON doesn't have them, so chlive must be told which columns hold them; it then fills them from the messages, exactly as the sink stored them:

```toml
[tables.events.columns]
kafka_partition = { kafka = "partition" }
kafka_offset = { kafka = "offset" }
kafka_ts = { kafka = "timestamp" }
offset = { clickhouse = "_offset", kafka = "offset" }   # a hidden name, still queryable
```

and in the table's sink connector:

```json
"transforms": "kafka",
"transforms.kafka.type": "org.apache.kafka.connect.transforms.InsertField$Value",
"transforms.kafka.partition.field": "kafka_partition",
"transforms.kafka.offset.field": "kafka_offset",
"transforms.kafka.timestamp.field": "kafka_ts"
```

- **Types:** the partition and offset in an integer column (`UInt64` for offsets); the timestamp in `DateTime64(3)` or a 64-bit integer, since `InsertField` writes milliseconds. A `DateTime` (or another precision) would read them as its own unit (`DateTime` overflows to 2106), so it's refused; so is any other type, at bind time, naming the column.
- **Only from the message:** a JSON key of the same name is overwritten by the transform and ignored by chlive. A message without a timestamp leaves the column at its default (NULL, or the zero value).
- **Shown:** a column configured with `kafka` is queryable even when a `hidden_columns` glob (`_*`) matches it.
- **Usable like any column:** in WHERE, ORDER BY, aggregates, and as a ReplacingMergeTree's version column (`ReplacingMergeTree(kafka_offset)`: the latest message of each key wins, as the engine sees them).
- **Not yet:** the message's topic, key and headers (Kafka Connect has no built-in transform copying a header into the row).
- The dev stack's `events` has these three columns (`scripts/schema.sql`, the connector's transforms in `nix/shell.nix`, `nix/chlive.dev.toml`).

### Shared results

Every subscription to the same query, with the same parameter values (any spelling, any order), shares one **entry** on the server (`server/src/hub.rs`):

- **One engine subscription per entry:** one ClickHouse snapshot and history query, and one Kafka consumer, however many clients watch it.
- **The result is kept** (its rows, and the cursor they're complete to), so a new subscriber gets it at once as a `snapshot`, without ClickHouse. Before the history is in, joiners wait for it.
- **A change log:** the entry keeps its last `cache.log_length` `diff` messages. A client returning with a cursor from this entry gets `resume` and just what it missed. A cursor from another entry of the same query (another *lineage*: entries started at different times can hold the same rows in a different order, or a different subset of tied rows), one older than the log, garbage or an old format gets a `snapshot` instead.
- **Stages:** plain queries and native aggregates are also kept on disk, in a SQLite store at `cache.dir` (`./chlive-cache`; `""` turns it off, kept across restarts): their rows as of some cursor (the *base*), every `diff` message after it (the *log*), and the engine's state (a *checkpoint*, every `cache.checkpoint_interval`, 60 s). So an entry goes through:

  | stage | follows Kafka | rows and log | engine state |
  |---|---|---|---|
  | **live** (subscribed, then `cache.keep_alive`, 30 s) | yes | memory, mirrored to disk | memory |
  | **warm** (`cache.keep_warm`, 1 h) | yes | disk only | memory |
  | **cold** | no | disk | disk |
  | **gone** | — | — | — |

  A subscriber brings a warm entry back to live, its rows loaded from disk, and a cold one, restored from its checkpoint and caught up from Kafka (no ClickHouse). Cold entries go, least recently used first, beyond `cache.disk_budget` (10 GiB). At most `cache.max_warm` (100) entries are warm (each has a Kafka consumer); the one warm the longest goes cold first. A stored result is rebuilt from ClickHouse instead when its snapshot is older than `cache.max_age` (24 h), its cursor is no longer in Kafka, it's more than `cache.max_catch_up` (1M) events behind, or the query plans differently now. After a restart every entry is cold. A log past `cache.log_bytes` (16 MiB) is folded into the base. Other queries (chDB aggregates, semi-joins) are live, then dropped.
- **Restores fork the lineage:** a restored entry is a new lineage that continues from the old one's checkpoint, so a cursor of the old lineage up to there still resumes; one past it (an unclean stop that lost messages after the last checkpoint) gets a snapshot.
- **Slow clients** don't hold anyone up: a subscriber whose connection's queue is full falls behind (nothing more is queued for it) and catches up once it has room, from the log, or with a snapshot if the log no longer reaches back.
- **When an entry fails** (ClickHouse, Kafka, …), every subscriber gets the error and the entry goes; subscribing again starts a new one.
- **Debugging:** `/debug` is a page showing the whole server live (it polls `/debug/state`, the same as JSON): the shared results by stage with their countdowns, the recent joins (for each subscription: where its result came from — memory, warm rows from disk, restored from disk, or a ClickHouse history query on a new or reused snapshot, and why a stored result couldn't be used — and what it got: a resume from the memory or disk log, or a snapshot and why), the disk store (bytes against the budget, files, the writer's batches, errors and evictions), each table's snapshots (current clone, leases, users, takes and failures), the open connections (outbox, subscriptions) and the cache settings. `/debug/subscriptions` lists the entries: query, lineage, stage (`starting`, `live`, `keep_alive`, `warm`) and how long it has left, subscribers (and whether they're lagging), rows (and whether they're in memory), whether it's on disk or was restored, its forks, cursor, log length and start, how long the history took, and the joins by outcome (snapshot, resume, wait) and catch-ups. `RUST_LOG=info,chlive_server::hub=debug` logs every entry's life by its `key` (a fingerprint of the query) and `lineage`: created, history loaded, each join with its decision and reason, leaves, stage changes, lagging and catching up, and why it was dropped; `chlive_server::store=debug` adds the disk store: checkpoints, compactions, write batches, restores and why a stored result couldn't be used, evictions. Cold entries are listed from the store (stage `cold`, bytes, last used); `sqlite3 chlive-cache/cache.sqlite3` shows the tables (`entries`, `base`, `log`). `trace` adds every diff applied.

```toml
[cache]
dir = "./chlive-cache"    # "": no disk stages (live, then dropped)
keep_alive = "30s"
keep_warm = "1h"          # "0s": cold right after the keep-alive
max_warm = 100
log_length = 1000         # diff messages in memory per live entry
disk_budget = "10GiB"
max_age = "24h"
max_catch_up = 1000000
checkpoint_interval = "60s"
log_bytes = "16MiB"
```

## Engine (custom dialect)

The engine is a library, used by the server.

**History and stream meet at a snapshot's watermark.** At subscribe time the engine takes the table's current snapshot (see [How it works](#how-it-works)): a clone holding exactly the offsets below the pause server's `N[p]` per partition. History is the whole clone, and each Kafka partition is consumed from `N[p]`. So every event is in exactly one of the two, wherever ClickHouse's ingestion is at. The engine assigns partitions directly and never commits offsets, so subscriptions don't affect each other. A table must be ingested by exactly one sink connector, from its topic (by default the one of the same name; see [Server configuration](#server-configuration)). The engine talks to ClickHouse only over HTTP(S), and `clickhouse.url` must reach the node clones are made on; for replicated tables it runs `SYSTEM SYNC REPLICA` first.

The full grammar is under [Supported queries](#supported-queries). A condition can use:
- comparisons (`=`, `!=`/`<>`, `<`, `<=`, `>`, `>=`), with `AND`, `OR`, `NOT` and parentheses;
- `IS [NOT] NULL` and `[NOT] IN (…)`;
- operands that are columns or literals: numbers, strings, `TRUE`/`FALSE`, or parameters.

Functions (except `now()`, see [Time](#time)), arithmetic, `LIKE` and `BETWEEN` aren't supported yet.

**Parameters** work as in ClickHouse: `WHERE session_id = {s:UUID} AND latency_ms > {l:UInt32}`, with the values sent separately (`params` in the protocol and the clients).
- They're allowed wherever a literal is in WHERE: comparisons, `IN (…)` items and `IS NULL`, in sub-queries too.
- Each value is read by its declared type, with the rules event JSON is read by. `"ABCDEF00-…"` is a UUID, normalized; `100` or `"100"` is a UInt32; `null` is accepted only for a `Nullable(…)` type.
- The declared type must be comparable with the column, as a literal must.
- Errors point at the placeholder: a missing value, one that doesn't fit its type, an unsupported type, or one name declared with two types. A value the query doesn't use is an error too, to catch typos.
- **Bound as literals.** Values become typed literals at bind time. The history query has them inlined, escaped by their type (`toUUID('…')`), and nothing is sent to ClickHouse as a parameter. A cursor taken with other values doesn't resume.
- **Not yet:** arrays (`IN {ids:Array(UUID)}`), parameters in LIMIT/OFFSET, and `Identifier`.

**ORDER BY** takes columns (selected or not), each with `ASC`/`DESC` and `NULLS FIRST`/`NULLS LAST`, or `ALL`, which sorts by the SELECT list in SELECT order, as in ClickHouse. Positions, expressions, `COLLATE` and `WITH FILL` aren't supported yet. The order is **managed**:
- **Semantics match ClickHouse.** NULLs default to last in both directions; NaN sits between the values and the NULLs; strings compare by bytes; `-0` ties with `0`. The history query always spells out each key's direction and NULL placement.
- **State.** Per subscription, the engine keeps only the ORDER BY values of the result, with equal keys merged into tie groups, never the rows. Order-only columns are fetched from ClickHouse under hidden aliases (`__order_0`, …) and stripped from the output.
- **Placement.** The history arrives in ClickHouse's order. Each new matching row goes after the last row it ties with, and its insert carries that `index`. So the live list has the same sequence of keys as a fresh query; only the order of rows within a tie group can differ.
- **Tested.** `engine/tests/clickhouse.rs` checks 26 orderings (6 of them with LIMIT, 6 with OFFSET) (NULL, NaN, ±inf, `-0`, byte-order strings, DateTime precision, Bool, duplicates, DESC, NULLS FIRST/LAST, several keys, `ALL`, an unselected column, with WHERE) in the same three setups as WHERE.

**LIMIT n** (a whole number; `WITH TIES` and `LIMIT … BY` aren't supported yet) goes into the history query and bounds the result:
- **With ORDER BY: the top N.** The state holds at most N sort keys. A new row placed inside the top N is inserted at its index, and the row pushed past the end is deleted (`delete @N`). A row placed at or past the end, including one tying with the last row, changes nothing and sends nothing. Which rows of the boundary tie group make it in is arbitrary, as it is in ClickHouse.
- **Without ORDER BY: a window of the latest N rows, newest first.** History is whatever N rows ClickHouse returns, in its order reversed (its last row counts as the newest). Each new matching row is inserted at the top (`insert @0`), and once the window is full, the last (oldest) row is deleted (`delete @N`). Only a row count is kept.
- **Atomic diffs.** An insert and the delete it causes are in the same iteration, so always in the same `diff`. The result has at most N rows after every diff, and the client only ever exposes whole diffs.

**OFFSET m** (`LIMIT n OFFSET m`, ClickHouse's `LIMIT m, n`, or `OFFSET m` alone) starts the result after m rows. The history query fetches `LIMIT m + n` (everything without LIMIT) and the engine keeps the first m rows itself, because they can come into the result later:
- **With ORDER BY: ranks m to m + n.** The state holds the sort keys of the top m + n, and the rows of the top m. A row landing above the result shifts it down: the row at rank m slides in at the top (`insert @0`), and, with LIMIT, the last one leaves (`delete @n`). A row landing inside is inserted at its index, as with LIMIT alone; one below changes nothing. Which rows of the tie groups at either boundary make it in is arbitrary, as in ClickHouse.
- **Without ORDER BY: a sliding window that holds back the newest m rows, shown newest first.** The arrival order is the history in ClickHouse's order, then the stream. The newest m rows are held back, and the result is the n rows before them, newest first. Each new row is held back, the oldest held-back row comes in at the top (`insert @0`), and once the result has n rows, the last (oldest) one leaves (`delete @n`). With `OFFSET 0`, this is LIMIT's window.
- **Memory.** A subscription holds m rows (plus m + n sort keys when ordered), whatever m is.
- **Not resumable yet:** a `since` gives a full snapshot.

**WHERE runs in two places, which must agree on every row.** ClickHouse evaluates it for history; the engine evaluates it for each event.
- **Typed decoding.** Each event's needed JSON fields are decoded by their ClickHouse column types, following ClickHouse's input rules: a quoted number is read as a number, a DateTime string is parsed, and a UUID is normalized to lowercase (see [Column types](#column-types)).
- **Missing keys.** A missing key is NULL for a `Nullable` column, and the type's zero value otherwise, just as ClickHouse's ingestion stores it.
- **Conditions.** They're evaluated with SQL's three-valued logic.
- **Literals.** They're typed and checked at bind time; for example, comparing `latency_ms` with a string is a positioned error.
- **Output.** Event rows are rendered the way ClickHouse renders rows.
- **Tested.** The agreement test in `engine/tests/clickhouse.rs` checks, for 22 predicates, that three setups return the same rows: everything in history; everything from the stream; and half from each, split at the snapshot (real clones, with a stand-in for the pause server). `engine/tests/kafka.rs` produces events before, around and after subscribing, against the real Redpanda and ClickHouse, and checks that each arrives exactly once.

### Time

`now()` is the server's clock, so a live query with it changes with no event: `WHERE ts > now() - INTERVAL 1 HOUR` keeps the last hour's rows, `WHERE due <= now()` lets rows in as their time comes, and rows go out and come in as the clock passes them.
- **`now(['timezone'])`** as in ClickHouse: `now('Europe/Warsaw')`. The timezone doesn't change the instant. It only matters for `DAY` and `WEEK`, which go by the calendar in that timezone: across a DST change, `INTERVAL 1 DAY` is 23 or 25 hours, as in ClickHouse. `now()` without one uses ClickHouse's server timezone (read once).
- **`INTERVAL n unit`** can be added or subtracted, any number of times: `now() - INTERVAL 1 HOUR - INTERVAL 30 MINUTE`. Units: `MILLISECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK` (calendar units, `MONTH`/`QUARTER`/`YEAR`, aren't supported yet).
- **Conditions:** a `DateTime`/`DateTime64` column `<`, `<=`, `>` or `>=` `now() ± INTERVAL …` (either side), anywhere in the WHERE, any number of them, under AND, OR and NOT, with any other conditions. For example:
  - `ts > now() - INTERVAL 1 HOUR` (rows leave an hour after their `ts`);
  - `due <= now()` (rows come in at their `due`);
  - `starts <= now() AND ends > now()` (in between);
  - `ts > now() - INTERVAL 10 MINUTE OR pinned` (pinned rows stay);
  - `NOT (ts > now() - INTERVAL 1 DAY)` (rows come in a day after their `ts`).
- **How:** each comparison flips once for a row, at an instant computed from its value (the first one at which `now() ± INTERVAL …` catches up with it: measured from the row's own time, not from when its event arrived). So a row's state can only change at those instants. The engine computes each row's **schedule** (in or out now, then the instants it goes out or comes in), keeps every row whose state can still change (in the result, or pending), and wakes at the earliest instant.
  - A row going out is a `delete` (by index; FINAL: by id), one coming in an `insert` at its place, as one diff with no event behind it.
  - `server.now_coalescing` (default 100 ms) is how long after the earliest instant it waits, so rows due within it change together: never early, at most that late (`0s`: each at its own instant).
  - A result with nothing due doesn't wake. The shared result's change log keeps the changes, so a client resumes through them; a result restored from disk lets everything that came due meanwhile go out and come in at once. A `since` cursor gets a fresh snapshot.
- **History** reads every row that is in now **or can come in later**: the WHERE with its NOTs pushed down, each comparison a row leaves by (`>`, `>=`) with the clock's value inlined and the intervals as written (`ts > fromUnixTimestamp64Nano(…, 'Europe/Warsaw') - INTERVAL 1 DAY`, so ClickHouse does the calendar itself), and each one a row comes in by as `IS NOT NULL` (it will be true). It can fetch rows that never come in; their schedules drop them. Pending rows are kept in memory (the small-data contract: see FINAL's measurements, about 1 KB per row). The plan's identity keeps `now()` symbolic (with its timezone), so a result is shared and stored like any other.
- **Shapes:**
  - plain results and ORDER BY (rows go out wherever they are, and come in at their place);
  - LIMIT only with ORDER BY the window's column DESC first, every `now()` comparison a top-level AND `>`/`>=` ("the latest n in the last hour"), where rows only leave, from the bottom;
  - FINAL rows (without LIMIT or OFFSET): a key's new version replaces its schedule;
  - **aggregates** (`count`, `sum`, `avg`, `min`, `max`, `groupArray`; GROUP BY or global, with ORDER BY, LIMIT and OFFSET over the groups), on MergeTree and FINAL: `select country, count() from events where ts > now() - INTERVAL 5 MINUTE group by country`.
    - The engine keeps every row whose state can still change, with its group and argument values. A row coming in adds its values to its group (making it if new); one going out takes them back out: counts and sums go down, `min`/`max` fall back to the next value, a `groupArray` loses the row's element.
    - A group whose last row left is deleted (with LIMIT, the next group comes in); a global aggregate goes back to ClickHouse's values for no rows (`count` 0, `avg` `nan`, …).
    - History reads every row that's in or can come in (`SELECT <groups>, <arguments>, <the WHERE's columns> … WHERE …`), not per-group states.
    - **Size:** about 1 KB per row that can still change. A busy stream over a long interval can be millions. Other functions with `now()` are refused, as on FINAL.
- **Not yet:** `=`/`!=` with `now()`; `now()` compared with anything but a DateTime column, or in SELECT or ORDER BY; LIMIT where rows can come in or come back; OFFSET; FINAL rows with LIMIT or OFFSET; sub-queries; `Date` columns and `today()`.

### Table engines

The binder reads each table's engine from `system.tables` (so the history user needs `SELECT` on it).
- **`MergeTree`:** as described above. `FROM t FINAL` is accepted and changes nothing.
- **`ReplacingMergeTree` without FINAL:** read the way ClickHouse reads one without FINAL, i.e. like a MergeTree. History has the versions the snapshot holds, as many as merges have left; every new version, a deleted one too, comes as a new row. `explain` says so.
- **`ReplacingMergeTree` with FINAL** (`SELECT … FROM t FINAL [WHERE …] [GROUP BY …] [ORDER BY …] [LIMIT n] [OFFSET m]`): one row per key, its latest version, as ClickHouse's `SELECT … FINAL` has it.
  - **Rows have ids:** their key (the table's sorting key) as JSON, e.g. `["p1"]`. History is `… FROM <snapshot> FINAL`, with the key and version under hidden fields.
  - **Each event is a new version of its key:** an `insert` (a key not in the result whose version matches the WHERE), an `update` of the whole row by id (a new version that still matches), or a `delete` by id (a deleted version, or one that no longer matches). An event with a lower version than its key's row is ignored. Ties, and tables without a version column, go to the later event, as ClickHouse keeps the last inserted.
  - **With ORDER BY,** inserts carry their `index`, and a new version that changes a row's sort values moves it: an `update` with an `index`, where the row is after the move (one whose sort values didn't change keeps its place). History comes in ClickHouse's order, ties included; after that a new or moved row goes after the rows it ties with. The engine keeps the result's sort keys in order (also in checkpoints, so a restore keeps the order within ties).
  - **With LIMIT (no ORDER BY),** where ClickHouse may return any n rows, the result is the n most recently active matching rows, newest first. The engine keeps them and a buffer of 2000 more, with their rows:
    - a matching version of a key that isn't kept comes in at the top (`insert @0`); the last visible row slides into the buffer (`delete @n`), and with a full buffer its last row is let go;
    - a visible row's new version updates it in place; one that's deleted or stops matching leaves (`delete` by id) and the buffer's first row comes in at the end;
    - the history reads `LIMIT n + 2000`; when departures drain the buffer (under 500, or fewer than n visible), a **refill** reads `LIMIT n + 4000` from the table's current snapshot, between two events. Its rows join the buffer unless their key is kept already or left after the snapshot (the engine remembers the last 100,000 departures to tell). A refill waits until the stream has reached its snapshot, and needs a newer snapshot than the last refill's (at most every second);
    - the result has `min(n, matching rows)` rows, except briefly while a refill is due: when every matching row is kept, none is needed.
  - **With ORDER BY and LIMIT,** the engine keeps the top n (the result) and the next 2000 ranks, always exactly a prefix of the result, with their rows:
    - a version is placed by its sort values, whatever its key's old ones were (a key that wasn't kept counts as new); across the top n's edge, a move is an insert at its index and a delete of the row pushed out at index n, or a delete and the next row coming in at the end;
    - past the last kept row (the bound), rows aren't known: a version ranking there is ignored, or leaves if its row was kept. With every matching row kept, there's no bound;
    - when departures leave fewer than n + 500 kept, a **rebuild**: the top n + 2000 of the table's current snapshot, then the events since it re-read from Kafka up to the stream's position, between two events. While replaying, the rebuilt state lets nothing go; if departures among those events still leave it short, it starts over from a bigger top (four times as many rows), until it has enough or every row. The difference of the visible rows (by id: deletes, moves, inserts, usually nothing) is sent, and the rebuilt state takes over. A rebuild needs a snapshot the stream has reached, and a newer one than the last unless rows were dropped since (let go, or ignored past the bound).
  - **With OFFSET m,** the same windows show rows `[m, m+n)` of what they keep: the first m (the newest, or the top-ranked) held back, exactly m of them, then the result, then the spare rows. A row leaving the held-back part makes the first shown row held back (`delete @0`) and the next one come in at the end; a row coming into it pushes its last one into the top of the result (`insert @0`). Without LIMIT, every matching row is kept and only the first m are held back.
  - **With GROUP BY or aggregates** (`count`, `sum`, `avg`, `min`, `max`, `groupArray`; with or without GROUP BY, ORDER BY, LIMIT and OFFSET over the groups): the groups of `SELECT … FINAL`, as ClickHouse computes them.
    - The engine keeps every matching key's row: its version, group and argument values. A new version takes the old row's values back out of their group and puts its own in, if it isn't deleted and matches the WHERE. A row can move between groups. A group whose last row leaves is deleted (`delete @i`, and with LIMIT the next group comes in); a global aggregate keeps its one row, back to ClickHouse's values for no rows (`count` 0, `sum` 0, `avg` `nan`, `min`/`max` the type's zero, or NULL for a NULL-able column).
    - History reads every row (`SELECT <key>, <version>, <groups>, <arguments> FROM <snapshot> FINAL WHERE …`), not per-group states. Integer sums are exact; a `Float64` sum that values were taken back out of can differ from ClickHouse's in the last digits.
    - Other functions (`uniq`, `median`, …) on FINAL are refused; they come natively, one at a time.
    - **Size:** this keeps every matching row in memory (and in the disk store's checkpoints), so FINAL aggregates are meant for small tables. Measured (`final_aggregates_at_scale`, release build, five aggregates of one column, 100 groups): about **1 KB per row**, so 100k rows ≈ 100 MB and 1M rows ≈ 1 GB, with about 25% more at the peak while the history loads (1M rows: 2.5 s). Events run at 200–650k per second. **Recommended: up to about 100k matching rows per query**; beyond that it works, but the memory is yours to plan for. A shared result holds one state however many clients follow it.
  - **Stream order is trusted:** each key's versions must only grow and all be in one Kafka partition (a keyed topic). The engine keeps only the result's keys and their versions (also in checkpoints, for the disk store), so a late, older version of a key that isn't in the result would wrongly come in.
  - **Keys:** the sorting and partition keys must be plain columns, and the partition key's must be among the sorting key's. A key, version or is-deleted column hidden by the naming rules is still read (queries just can't name it).
  - **With `now()`** (see [Time](#time)): rows and aggregates; a key's new version replaces its schedule. Not with LIMIT or OFFSET.
  - **Not yet:** FINAL with aggregates other than `count`/`sum`/`avg`/`min`/`max`/`groupArray`, with a sub-query, and FINAL inside a sub-query: each is a positioned error. A `since` cursor gets a fresh snapshot (the server's shared result still resumes from its change log).
  - Snapshots keep the engine with its version and is-deleted columns, so `FINAL` on a snapshot dedups as on the table.
- **Any other engine** (`Collapsing`, `Summing`, `Aggregating`, `Distributed`, views, `Shared…` on ClickHouse Cloud, …): refused.

The query goes through parser → binder → planner. The binder resolves FROM, then WHERE, then SELECT, in SQL's evaluation order:
- **FROM:** the table must exist in ClickHouse and have a Kafka topic. Snapshots (`__chlive_snap_*`) aren't tables. Its engine must be a `MergeTree` or a `ReplacingMergeTree` (or their `Replicated…` forms); anything else is refused. See [Table engines](#table-engines).
- **SELECT:** listed columns must exist. `col AS name` renames a column in the output; a column can be selected more than once under different names, but output names must be unique. An alias can't be another column's name (in ClickHouse it would shadow that column in WHERE and ORDER BY) or start with `_` (Kafka columns and the engine's hidden fields). Aliases only name output columns: WHERE and ORDER BY refer to table columns.

The planner selects only the needed columns from ClickHouse, and the stream decodes only the needed JSON keys of each event; the other keys are skipped unparsed. Hard-coded naming rules map table `t` to ClickHouse `default.t` and to topic `t`, and keep column names the same on both sides; columns starting with `_` are hidden.

Each iteration prints its diff: first the history (`+ {row}` per ClickHouse row, then `history: N rows`), then one line per relevant event of the subscribed topics, as `<topic>/<partition>:<offset> + {…}`, e.g. `events/0:21 + {…}`. Only the query's topics are consumed.

## Client

```ts
// React
import { useLiveQuery } from "@chlive/client/react";
const { status, rows, error } = useLiveQuery<Event>(sql); // error: { code, message }
// with parameters, and only once there's a session to watch
const q = useLiveQuery<Event>("select * from events where session_id = {s:UUID}", {
  params: { s: sessionId },
  enabled: sessionId !== undefined,
});

// without React
import { LiveClient } from "@chlive/client";
const live = new LiveClient({ url: "ws://localhost:8080/live" }); // offline cache on by default
const sub = live.subscribe(sql, (s) => render(s.status, s.rows, s.error), { params });
sub.unsubscribe();

// writing (see Inserts): each row's [partition, offset], or an InsertError with a code
const { offsets } = await live.insert("messages", { chat_id: "c7", body: "hi" });
```

`status` is `loading` (nothing yet), `cached` (rows from the offline cache, being caught up), `live`, `error`, or, from the hooks, `idle`.
- **`enabled: false`** makes a hook subscribe to nothing, the way TanStack Query's `enabled` holds off a fetch: hooks can't be called conditionally. Its status is then `idle`: with no rows if it never ran, or with its last rows (frozen) if it was turned off.
- **`params`** resubscribe only when their values change; a new object with the same values doesn't. A `Date` is sent in ClickHouse's form (UTC), a `bigint` as its digits.
- **Results are shared and kept in memory**, per query and parameter values: components using the same query share one server subscription, and a component mounting on a query the client already holds gets its rows on the first render (the hooks read the client with `useSyncExternalStore`), with no empty frame. After the last subscriber leaves, the server subscription stays for `keepAliveMs` (5 s; StrictMode's double mount, route changes and toggles reuse it), then the rows and cursor stay for `gcTimeMs` (5 min): coming back shows them (`cached`) and resumes from the cursor. Idle results hold at most `memoryRows` rows together (100 000); the oldest go first.
- **`live.preload(sql, { params })`** starts a query before the component that shows it renders, e.g. in a route loader: it resolves once the rows are there (cached or live), and the component then has them on its first render. `live.peek(sql, { params })` reads what the client holds, synchronously.
- **The offline cache** keeps results per query and parameter values. Rows are exactly the selected columns, in the query's order, so columns come from the rows themselves; the row type is the hook's type parameter. ORDER BY and LIMIT are already applied to `rows`. One socket carries all of a client's subscriptions; it reconnects with backoff and resubscribes from where it was.

**Values are parsed by column type.** By default, `Date`, `Date32`, `DateTime` and `DateTime64` columns arrive as JS `Date`s. They're read as UTC, and DateTime64 is cut to the millisecond, since a `Date` holds no more. The `parsers` option sets how a ClickHouse type (its base name) becomes a row value, and is merged over the defaults:

```ts
import { LiveClient, parseClickHouseDate } from "@chlive/client";
import spacetime from "spacetime";

const live = new LiveClient({
  url,
  parsers: {
    // (value as ClickHouse writes it in JSON, never null; the column's type, taken apart)
    DateTime64: (v, type) => spacetime(parseClickHouseDate(v as string).getTime()),
    UUID: (v) => (v as string).toUpperCase(),
    Date: null, // keep Date columns as strings
  },
});
```

- `parsers: false` keeps every value as it arrives.
- A parser that throws fails the subscription (`CLIENT_ERROR`).
- The offline cache keeps values as they came over the wire, so cached results don't depend on the parsers.
- Results with a parsed column hold their rows twice, once parsed and once as received.
- Strings are read as UTC. That's correct for columns without a timezone of their own. For a `DateTime('Europe/Warsaw')` column, ClickHouse writes local times, while the engine still renders stream rows in UTC. A custom parser gets the timezone in `type.args`.

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
// with parameters: client.subscribe_with::<Row>(sql, Params::from([("s".into(), json!(session))]))
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
- **Complete histories.** `client.query(sql).history()` returns the rows as of subscribing and a stream of the rows inserted after, each row in exactly one. It asks for a `complete` snapshot (see the protocol), so the history holds every event in Kafka when it subscribed, including ones ClickHouse hasn't ingested yet or a shared result hasn't read yet. The server holds the snapshot until the result gets there, or ends the subscription with `CATCH_UP_TIMEOUT` after `server.complete_wait`. Any other view asks for it with `.complete()`, e.g. `client.query(sql).complete().fetch()`. Without it, a snapshot is whatever the server's result has, and the newer events come as diffs.
- **Writing:** `client.insert("messages", rows).await` takes anything that serializes to JSON objects and returns an `Ack` with each row's `(partition, offset)` (see [Inserts](#inserts)). `ClientOptions::new(url).user("alice")` says who the client is (`hello`, on every connect), for what it may read and insert. An insert waits for the connection; one sent but unanswered when it drops fails with `DISCONNECTED` (it may or may not have gone in) and isn't sent again.
- **Cancelling:** dropping a subscription unsubscribes; `client.close().await` ends every stream.

**Terminal UI.** `clc` (the `chlive-tui` crate, `tui/`) is a small user of the client: an input for the query and a table of its result (columns with their ClickHouse types), updated on every diff, with the rows a diff inserted or changed highlighted for a second. In the dev shell, `tui` connects it to the stack's server:

```sh
tui "select user_id, latency_ms from events order by latency_ms desc limit 10"
tui --param s=0d1c2b3a-0000-4000-8000-00000000000a "select ts, path from events where session_id = {s:UUID}"
clc --url ws://host:8080/live             # elsewhere (or CHLIVE_URL); `nix build` has it in result/bin
```

Enter runs the query in the input, replacing the last one. Tab moves the focus between the input and the table, ↑/↓/PgUp/PgDn scroll, and Esc or Ctrl-C quits. The input has the usual readline and macOS shortcuts:

| | back | forward |
|---|---|---|
| move a char | ←, Ctrl-B | →, Ctrl-F |
| move a word | Option-← / Ctrl-←, Option-B | Option-→ / Ctrl-→, Option-F |
| move to the line's start / end | Home, Ctrl-A, Cmd-← | End, Ctrl-E, Cmd-→ |
| delete a char | Backspace, Ctrl-H | Delete, Ctrl-D |
| delete a word | Option-Backspace, Ctrl-W (to whitespace) | Option-Delete, Option-D |
| delete to the line's start / end | Cmd-Backspace, Ctrl-U | Cmd-Delete, Ctrl-K |

Option shortcuts need the terminal to send Option as Meta/Alt (e.g. Terminal.app's "Use Option as Meta key", iTerm2's "Esc+"). Cmd shortcuts arrive where the terminal reports Cmd (the kitty keyboard protocol, which `clc` turns on when available) or sends Cmd-Backspace as Ctrl-U, as most macOS terminals do. `--param name=value` (repeatable; JSON values as such, anything else a string) is sent with every query.

### Protocol

JSON text frames on `/live` (`protocol/src/lib.rs`, the `chlive-protocol` crate the server and the Rust client share; mirrored by `client/src/protocol.ts`). Ids are the client's, per connection:

```jsonc
// client → server
{"type": "subscribe", "id": "s1", "sql": "select user_id, latency_ms from events where latency_ms > 100"}
{"type": "subscribe", "id": "s2", "sql": "select * from events where session_id = {s:UUID}", "params": {"s": "abcdef00-…"}}
{"type": "unsubscribe", "id": "s1"}
{"type": "insert", "id": "w1", "table": "messages", "rows": [{"chat_id": "c7", "body": "hi"}]}
// server → client
{"type": "snapshot", "id": "s1", "columns": [{"name": "user_id", "type": "String"}, …], "rows": [{"user_id": "u1", "latency_ms": 340}, …]}
{"type": "diff", "id": "s1", "changes": [{"op": "insert", "row": {…}}, …]}
// with ORDER BY: the snapshot is in order and says so, and inserts carry their index
{"type": "snapshot", "id": "s2", "columns": […], "rows": […], "order": [{"column": "latency_ms", "direction": "desc", "nulls": "last"}]}
{"type": "diff", "id": "s2", "changes": [{"op": "insert", "row": {…}, "index": 2}, …]}
{"type": "error", "id": "s1", "code": "INVALID_QUERY", "message": "unknown column nope in table events (…) at 1:8"}
{"type": "ack", "id": "w1", "offsets": [[3, 1842]]}
```

- `snapshot` comes first and once; `diff`s follow, each with the changes of the events that were ready (never empty). The ops are `insert`, `delete` and `update`; clients reject ops they don't know.
- With an `index`, the row is inserted there: the row at that index and every row after it move one down. Without one, it's appended.
- Aggregated and FINAL results have row ids (`"ids"` in the snapshot, `"id"` on inserts): `{"op": "update", "id", "set"}` sets the columns in `set` (the changed ones, or all of them), and, with an `index`, moves the row there.
- An `update` can also carry `"edit": {"<array column>": {"remove": [[start, count], …], "append": […]}}` (`groupArray`): first the `remove` ranges go (positions in the array before the edit, ascending and apart), then `append` is added at the end. A column is in `set` or in `edit`, never both; the server sends whichever is smaller, and inserts and snapshots always carry whole arrays.
- `{"op": "delete", "index": k}` removes the row at `k`, and `{"op": "delete", "id": …}` the row with that id (FINAL); the rows after it move up. With LIMIT (`"limit": n` in the snapshot), each insert that overflows the result is followed by the delete it causes. With OFFSET (`"offset": m` in the snapshot, informational), the diffs already account for the rows above the result: a row sliding in from above is an ordinary insert.
- A `diff` is atomic: its changes apply in sequence, each index into the list as the previous change left it, and only the state after all of them is meaningful (in between, an insert and its delete can briefly make one row more than the LIMIT). The client applies a whole diff to one copy of the rows before its listeners see it.
- **Resuming.** `snapshot`, `diff` and `{"type": "progress", "id", "at"}` carry `at`, an opaque cursor: how far the result is complete, per Kafka partition, counting events that didn't match. A client that kept a result subscribes with `"since": <at>`; the server answers `{"type": "resume", "id", "columns", "order"?, "limit"?}` instead of a snapshot (the client's rows are still valid), then diffs with only what was missed. If it can't resume (another query or schema, a cursor from an earlier server result for the query, one older than the result's change log, …), it sends a normal `snapshot`.
- **Complete snapshots.** A `subscribe` with `"complete": true` gets a snapshot holding every event in the query's topics up to their latest offsets when the server got the `subscribe`. Otherwise a snapshot is whatever the result has: a new result's ClickHouse snapshot (which trails Kafka by the sink's lag and the snapshot's age), or a shared result's current state (behind on a burst). The newer events then come as diffs. The server waits for the result to get there; if it can't within `server.complete_wait` (30 s), the subscription ends with `CATCH_UP_TIMEOUT`. A `resume` is unaffected. `@chlive/client` doesn't ask for it; the Rust client's `history()` does.
- **Inserts.** `insert` writes rows into a table (see [Inserts](#inserts)); its id is the client's, apart from subscriptions'. The answer is an `ack` with each row's `[partition, offset]`, in order, or an `error` with that id: `PERMISSION_DENIED`, `INVALID_INSERT` (nothing went in) or `WRITE_FAILED` (Kafka; the message says which rows went in).
- Error codes: `SYNTAX_ERROR` and `INVALID_QUERY` (the query; positioned), `HISTORY_FAILED` (ClickHouse), `STREAM_FAILED` (Kafka), `AGGREGATE_FAILED` (chDB), `SNAPSHOT_FAILED` (the pause server, or cloning), `BAD_MESSAGE` (unparseable, no id), `DUPLICATE_ID`, `TOO_MANY_SUBSCRIPTIONS` (over `server.max_subscriptions_per_connection`), `CATCH_UP_TIMEOUT` (a complete snapshot not had within `server.complete_wait`), and for inserts `INVALID_INSERT` and `WRITE_FAILED`. After an error, that subscription is over.
- Rows are exactly the selected columns, rendered like ClickHouse's JSON output.

### GROUP BY and aggregates

`SELECT keys, agg(c) [AS name], … FROM t [WHERE …] [GROUP BY keys] [ORDER BY …] [LIMIT n] [OFFSET m]`, with ClickHouse's result types and output names (see [Supported queries](#supported-queries)). Functions: `count()`, `count(c)`, `sum`, `avg`, `min`, `max`, `groupArray` (also `array_agg`) (native), and `uniq`, `uniqExact`, `median`, `any`, `anyLast` (chDB).
- **`groupArray`:** every value, NULLs skipped, in arrival order: the history's in ClickHouse's order (unspecified, as ClickHouse's is), then the events'. Treat the order as unspecified. An array can't be ordered by. As a group changes, its array is edited in place (an `edit` in the `update`: new values appended, values that left removed by position), not sent whole; with FINAL a new version's value moves to the end, and with `now()` values leave from the front. Not yet: `groupArray(N)(x)`. Output names keep the function as written, as ClickHouse's do (`COUNT()`, `array_agg(x)`). Not yet: HAVING, expressions as arguments, and resuming aggregates (they start from a snapshot).

- **Native** when every aggregate in the query has a native implementation (`engine/src/aggregate/`: the `AggregateFunction` and `AggregateState` traits). ClickHouse computes the parts of each group's state over HTTP (`avg(x)` → `sum(x)`, `count(x)`), and the engine keeps the states in memory and updates them per event. `--explain` says which way a query runs.
- **chDB** (ClickHouse as a library, inside the engine) otherwise. Main ClickHouse computes the history's aggregate states (`uniqExactState(x)`, …) from the snapshot, and a chDB table per subscription loads them over HTTP (`url()`, as RowBinaryWithNamesAndTypes, which carries states as they are); each batch of events (up to 1000 that arrived together) adds its states, and the groups it touched are read back finalized with `…Merge`.
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

### Column types

Each ClickHouse type the engine understands is one module in `engine/src/types/`. It implements two traits, with no defaults:
- `ColumnType`: what a column of it accepts (an event's JSON, literals, its zero value for a missing field) and allows (filtering, ordering, grouping, `min`/`max`).
- `Scalar`: how its values compare and render, as ClickHouse does.

`engine/tests/types.rs` checks every type against ClickHouse. ClickHouse and the engine must read the same sample values, render them the same, sort them the same way and compare them the same way with literals.

| type | events read as ClickHouse does | notes |
|---|---|---|
| `Int8`…`UInt64` | numbers, quoted numbers, booleans | out-of-range values wrap around; unsigned columns reject negatives |
| `Float32`, `Float64` | numbers, quoted numbers (`"nan"`, `"inf"`, `"1e3"`), booleans | |
| `String`, `FixedString`, `Enum8/16` | strings, numbers and booleans as text | byte order |
| `DateTime`, `DateTime64(p)` | `YYYY-MM-DD[ hh:mm:ss[.f]]`; integers are ticks at the column's precision | UTC |
| `Date`, `Date32` | `YYYY-MM-DD` | |
| `Bool` | `true`/`false`, `0`/`1` | `=` and `!=` only |
| `UUID` | 36 characters with dashes, or 32 hex digits, in either case | stored and sent lowercase. Ordered as ClickHouse orders them: by the last 8 bytes first, so not alphabetically |
| others (`Array`, `Map`, `Decimal`, …) | passed through | can only be selected |

Any of these can be `Nullable`, and `LowCardinality` is transparent. Literals follow the column's type: `session_id = 'ABCDEF00-…'` matches the lowercase value, and an invalid UUID or date is a positioned error.

Known differences, where the engine skips an event that ClickHouse reads:
- DateTime strings that are only digits, such as `"1700000000"` (Unix seconds) or `"2026"`;
- partial DateTime strings, such as `" 7"`.

### Offline cache

`LiveClient` keeps results between sessions in SQLite (sqlite-wasm) on OPFS, in a dedicated worker, using the `opfs-sahpool` VFS, so no COOP/COEP headers are needed. It's on by default wherever it can run (a browser with OPFS and workers; `opfsAvailable()`) and off elsewhere (Bun, Node, SSR), with nothing to configure. `cache: false` turns it off, `cache: { budgetBytes }` sets its size (2 GB by default), and `live.clearCache()` empties it (e.g. on logout). sqlite-wasm is loaded (a separate chunk, from `@sqlite.org/sqlite-wasm`, a dependency) only when the cache is used.

**Warm start:** as soon as the cache opens, the client reads its most recently used results (up to `cache: { warmRows }` rows together, 10 000 by default; 0 turns it off) into memory, without marking them as used, so their first subscribers get them synchronously. This finishes once sqlite-wasm has started, tens to a few hundred milliseconds after the page loads: often before lazily loaded routes render, but not always before the very first one; `preload` in a route loader is the dependable way.

The client takes care of the page's lifecycle: it saves pending results when the tab goes to the background or the page is hidden, then closes the database so a reload can open it at once, and reopens it if the page comes back from the back/forward cache.

- **On subscribe**, the cached result (if any) is shown at once (`status: "cached"`, with `cachedAt`), and the subscription is sent with `since` = its cursor. The server resumes and sends only what was missed; reconnects resume the same way.
- **On the server**, the cursor resumes against the query's shared result (see [Shared results](#shared-results)): if it comes from that result (the same lineage) and its change log still reaches back to it, the client gets `resume` and just the diffs it missed; otherwise a snapshot of the result as it is, with no ClickHouse query while the result is live, warm or cold.
- **What's stored:** per query and param values (the SQL text, plus the params as sent in stable JSON), its rows as JSON chunks, the cursor and when it was last used. A database with an older layout is emptied (`PRAGMA user_version`). Each write is one transaction: snapshots and ordered diffs are saved in full (batched, about 1 s), append-only diffs are appended, and `progress` moves the cursor.
- **GC:** above the budget, the least recently used queries are evicted.
- **MVP limits:** one tab holds the database at a time; other tabs run without a cache (the playground shows why). Keys are query texts with their param values, not shapes yet.

The playground uses it: the "Client state" panel shows a hit or miss, whether the server resumed and how big the catch-up was, the cursor, and the store's size, with a "Clear cache" button.

## Tests

```sh
cargo test --workspace     # engine (lexer, parser, binder, planner, types, watermark), server protocol over a real WebSocket,
                           # the protocol's messages, the Rust client's result store and connection (against a fake server)
bun run test               # client state, reconnect/resume, the cache store on bun:sqlite (bun test)
e2e                        # = CHLIVE_E2E=1 cargo test -p chlive-engine -p chlive-e2e, against `dnvr up`; output kept in .dnvr/e2e.log
```

`e2e` runs the engine's ClickHouse and Kafka tests, then the server end to end. The latter subscribe over WebSocket while producing events, and check that each live result (snapshot plus inserts) equals the same query run directly on ClickHouse once everything is ingested; one does so through the Rust client. They count rows across the whole `events` table, so don't produce other events (e.g. with the playground's buttons) while they run.

## Benchmarks

```sh
bench                      # = bun bench/queries.ts, against `dnvr up`: subscribe time, latency and throughput per query
```

`bench` runs 14 queries, from one filtered column to the most complex plain and aggregated queries the dialect has, through `@chlive/client`, and prints a table per measure. It first tops `events` up to 1M rows (`--seed`) and `customers` to 5000; it adds its own events, so `reset-data` afterwards gives a fresh seed. Results and what they show: [docs/bench/queries.md](docs/bench/queries.md); aggregates natively vs in chDB: [docs/bench/group-by.md](docs/bench/group-by.md).

## Supported queries

The engine's dialect, in full (keywords are case-insensitive):

```
SELECT * | select_item [, select_item ...]
FROM table [[AS] alias] [FINAL]                              -- FINAL: see Table engines
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
             | groupArray | array_agg                         -- native
             | uniq | uniqExact | median | any | anyLast       -- through chDB
condition   := condition OR condition | condition AND condition | NOT condition | (condition)
             | operand {= | != | <> | < | <= | > | >=} operand
             | operand IS [NOT] NULL
             | operand [NOT] IN (literal [, literal ...])
             | column IN (SELECT column FROM table [WHERE condition])   -- see below
             | EXISTS (SELECT … FROM table [[AS] alias] [WHERE inner.col = outer.col] [AND condition …])
             | operand
operand     := column | literal
             | now(['timezone']) [{+ | -} INTERVAL n unit …]    -- see Time
literal     := number | -number | 'string' | TRUE | FALSE | NULL
column      := [table_or_alias.]name
name, table, alias := identifier | "identifier" | `identifier`
```

- **Tables:** a table needs a ClickHouse table (a `MergeTree` or `ReplacingMergeTree`; see [Table engines](#table-engines)), ingested by one sink connector (known to the pause server) from a Kafka topic of the same name.
- **Table aliases and qualified names:** `FROM events e` (or `AS e`); `e.user_id` or `events.user_id` anywhere a column goes. In a sub-query, an unqualified name is its own table's column first, then the outer query's.
- **Aliases** only name output columns: WHERE and (plain queries') ORDER BY refer to table columns. Output names must be unique; an alias can't be another column's name or start with `_`.
- **ORDER BY / LIMIT:** with `ORDER BY`, each insert lands at its index; without it, rows arrive in Kafka order per partition after the snapshot. `LIMIT` keeps the top N (with `ORDER BY`) or the latest N, newest first (without), deleting the rows that fall out. Without `LIMIT`, plain results only grow. `OFFSET m` starts after the top m (with `ORDER BY`; rows above shift the result) or holds back the newest m (without); see [OFFSET](#engine-custom-dialect).
- **GROUP BY / aggregates** (also without GROUP BY: one global group): selected columns must be grouping columns; no `SELECT *`. Groups are inserted, then updated.
  - **ORDER BY** takes an output name (an alias or a default name like `count()`), a grouping column, or an aggregate call (`order by count() desc`; one that isn't selected is computed anyway), or `ALL`. As values change, groups move: an `update` with an `index`. A group whose sort key changes goes after the groups it ties with.
  - **LIMIT n** keeps the first n groups by ORDER BY: a group falling out is deleted and the next one comes in, and the others are kept to come back in later. Without ORDER BY, it's the first n groups seen.
  - **OFFSET m** starts the result after the first m groups: with ORDER BY, groups come in and go out at both ends as they move; without, it's the groups after the first m seen.
- **EXISTS (subquery)**: correlated (`… WHERE c.customer_id = e.user_id AND …`) it is the same semi-join as the matching `IN`: exactly one `inner = outer` equality, a top-level `AND` condition of the sub-query, is the key; the rest of its WHERE may only use its own table. Uncorrelated (no such equality), it's true for every row once the sub-query has a row: until then outer rows are kept back, and then they all come in. Its SELECT list doesn't matter (`SELECT 1`, `SELECT *`).
- **IN (subquery)**: one per query (IN or EXISTS), as a top-level `AND` condition of the WHERE, over plain queries (with ORDER BY, LIMIT and OFFSET); the sub-query selects one column of one table, with an optional WHERE (the same table as the outer one is fine). See [IN and EXISTS sub-queries](#in-and-exists-sub-queries).
- **Not yet:** `WITH TIES`, `LIMIT … BY`, `DISTINCT`, HAVING, joins, `NOT IN (subquery)`, `NOT EXISTS`, EXISTS with several correlations (composite keys), constants as output columns, and other sub-queries, IN sub-queries with GROUP BY, expressions and other functions (arithmetic, `LIKE`, `BETWEEN`), positions in ORDER BY/GROUP BY, parameters, `now()` beyond comparisons with DateTime columns in the WHERE (see [Time](#time)).
