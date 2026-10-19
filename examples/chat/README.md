# chat: the permissions demo (frontend)

One group chat where every list is a live chlive query: two users side by side, each with the
whole chat and a composer. It's built to drive chlive's security design.

**Step 1 (this):** no permissions. Everyone sees everyone's messages.

## Running it

This directory is the frontend (React, Vite). Its stack is in its own dev shell, `nix/chat.nix`,
with the server config in `nix/chlive.chat.toml` and the schema, seed and smoke test in
`scripts/chat/`. Run it instead of the main stack, not next to it:

```sh
nix develop .#chat
dnvr up                       # Redpanda, ClickHouse, sink, pause server, chlive-server, seed, app
dnvr-state get chat-app.url   # open it
chat-smoke                    # end to end: a message and the users, live
```

Every process is prefixed `chat-` (`dnvr logs chat-server`, …); `ch` is a ClickHouse client,
`produce <topic> '<json>'` writes to Kafka, `reset-data` wipes the stack's data (stopped).

Messages go straight to Kafka through Redpanda's HTTP proxy (`/kafka`); the sink puts them into
ClickHouse and chlive into both panes.

## Schema (`scripts/chat/schema.sql`) and seed (`scripts/chat/seed/`)

| table | engine | |
|---|---|---|
| `messages` (ts, message_id, user_id, body) | MergeTree | written once |
| `users` (user_id, name, color, version) | ReplacingMergeTree | read with FINAL |

The seed (into an empty stack, through Kafka): Alice, Bob, Carol and Dave, and an hour of
messages.
