# chat: the permissions demo (frontend)

One group chat where every list is a live chlive query: two users side by side, each with a
composer. It's built to drive chlive's security design.

**Step 2 (this):** row-level permissions from Postgres. Each pane is its own connection, as its
user, and sees the messages of the authors that user is permitted to see. The permissions are
rows of `permissions (viewer_id, author_id)` in the stack's Postgres; chlive follows them with
logical replication, so a change there shows in the panes at once.

## Running it

This directory is the frontend (React, Vite). Its stack is in its own dev shell, `nix/chat.nix`,
with the server config in `nix/chlive.chat.toml` and the schema, seed and smoke test in
`scripts/chat/`. Run it instead of the main stack, not next to it:

```sh
nix develop .#chat
dnvr up                       # Redpanda, ClickHouse, sink, pause server, chlive-server, seed, app
dnvr-state get chat-app.url   # open it
chat-smoke                    # end to end: messages per user, a grant and a revoke, live
pg                            # psql on the permissions
```

Change who sees whom, and watch the panes:

```sh
pg -c "insert into permissions values ('alice', 'carol')"   # Alice now sees Carol's messages
pg -c "delete from permissions where viewer_id = 'alice' and author_id = 'carol'"
```

Every process is prefixed `chat-` (`dnvr logs chat-server`, …); `ch` is a ClickHouse client,
`produce <topic> '<json>'` writes to Kafka, `reset-data` wipes the stack's data (stopped).

Messages go straight to Kafka through Redpanda's HTTP proxy (`/kafka`); the sink puts them into
ClickHouse and chlive into the panes permitted to see them.

## Permissions (`nix/chlive.chat.toml`, `scripts/chat/postgres.sql`)

```toml
[security]
default = "deny"                    # tables without security can't be read

[security.providers.pg]
kind = "postgres"
url = "postgres://postgres@127.0.0.1:54329/chat"
publication = "chlive_permissions"
slot = "chlive_chat"           # made anew at each start: one per server

[tables.messages.security]
provider = "pg"
column = "user_id"                  # the ClickHouse column filtered
grants = "SELECT author_id FROM permissions WHERE viewer_id = {subject:String}"

[tables.users.security]
public = true
```

The app says who each connection is (`new LiveClient({ url, user })`); chlive ANDs
`user_id IN (<what the user is granted>)` into every query on `messages`, and when the grants
change, the pane gets a new snapshot. The seed: everyone sees themselves; Alice also sees Bob;
Bob sees Alice and Carol; Carol sees everyone; Dave only himself.

## Schema (`scripts/chat/schema.sql`) and seed (`scripts/chat/seed/`)

| table | engine | |
|---|---|---|
| `messages` (ts, message_id, user_id, body) | MergeTree | written once |
| `users` (user_id, name, color, version) | ReplacingMergeTree | read with FINAL |

The seed (into an empty stack, through Kafka): Alice, Bob, Carol and Dave, and an hour of
messages.
