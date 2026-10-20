# chat: the permissions demo (frontend)

One group chat where every list is a live chlive query: two users side by side, each with a
composer. It's built to drive chlive's security design.

**Step 3 (this):** permissions from Postgres, on tables, rows and columns. Each pane is its own
connection, as its user, and sees only what that user may read. The permissions are rows of
three tables in the stack's Postgres; chlive follows them with logical replication, so a
change there shows in the panes at once.

| Postgres table | says | in the panes |
|---|---|---|
| `table_permissions (user_id, table_name)` | which tables a user may read | a pane without `messages` says "No access to messages" (a revoke ends its subscription; Retry subscribes again) |
| `permissions (viewer_id, author_id)` | whose messages a user may see | only those authors' messages |
| `column_permissions (user_id, table_name, column_name)` | which columns of `users` a user may read | names in colour only if the user may read `color` (the panes read `select *`, which follows the columns; naming `color` would be denied) |

## Running it

This directory is the frontend (React, Vite). Its stack is in its own dev shell, `nix/chat.nix`,
with the server config in `nix/chlive.chat.toml` and the schema, seed and smoke test in
`scripts/chat/`. Run it instead of the main stack, not next to it:

```sh
nix develop .#chat
dnvr up                       # Redpanda, ClickHouse, sink, pause server, chlive-server, seed, app
dnvr-state get chat-app.url   # open it
chat-smoke                    # end to end: tables, rows and columns per user, granted and revoked live
pg                            # psql on the permissions
```

Change what users may read, and watch the panes:

```sh
pg -c "insert into permissions values ('alice', 'carol')"                 # Alice sees Carol's messages
pg -c "insert into column_permissions values ('alice', 'users', 'color')" # Alice sees colours
pg -c "insert into table_permissions values ('dave', 'messages')"         # Dave may read messages (Retry)
pg -c "delete from table_permissions where user_id = 'alice' and table_name = 'messages'"
```

Every process is prefixed `chat-` (`dnvr logs chat-server`, …); `ch` is a ClickHouse client,
`produce <topic> '<json>'` writes to Kafka, `reset-data` wipes the stack's data (stopped).

Messages go straight to Kafka through Redpanda's HTTP proxy (`/kafka`); the sink puts them into
ClickHouse and chlive into the panes permitted to see them.

## Permissions (`nix/chlive.chat.toml`, `scripts/chat/postgres.sql`)

```toml
[security]
default = "deny"                    # tables without security can't be read
# which tables a user may read (every table needs this grant)
tables = { provider = "pg", grants = "SELECT table_name FROM table_permissions WHERE user_id = {subject:String}" }

[security.providers.pg]
kind = "postgres"
url = "postgres://postgres@127.0.0.1:54329/chat"
publication = "chlive_permissions"
slot = "chlive_chat"                # made anew at each start: one per server

# whose messages: the rows whose user_id is one of the user's grants
[tables.messages.security]
provider = "pg"
column = "user_id"
grants = "SELECT author_id FROM permissions WHERE viewer_id = {subject:String}"

# all users' rows, but only the columns the user is granted
[tables.users.security]
public = true
columns = { provider = "pg", grants = "SELECT column_name FROM column_permissions WHERE user_id = {subject:String} AND table_name = 'users'" }
```

The app says who each connection is (`new LiveClient({ url, user })`). chlive checks every table
a query reads, leaves out the columns the user may not read (`*` expands to the others; naming
one is `PERMISSION_DENIED`), and ANDs `user_id IN (<their grants>)` into every query on
`messages`. When the permissions change, a subscription gets a new snapshot, or ends with
`PERMISSION_DENIED` if it lost a table, or a column it names.

The seed (into empty tables): everyone reads `users` and `messages`, except Dave (no
`messages`); everyone sees their own messages, Alice also Bob's, Bob also Alice's and Carol's,
Carol everyone's; everyone reads the users' ids, names and versions, only Carol their colours.

## Schema (`scripts/chat/schema.sql`) and seed (`scripts/chat/seed/`)

| table | engine | |
|---|---|---|
| `messages` (ts, message_id, user_id, body) | MergeTree | written once |
| `users` (user_id, name, color, version) | ReplacingMergeTree | read with FINAL |

The seed (into an empty stack, through Kafka): Alice, Bob, Carol and Dave, and an hour of
messages.
