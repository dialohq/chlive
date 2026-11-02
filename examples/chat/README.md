# chat: the permissions demo (frontend)

One group chat where every list is a live chlive query: two users side by side, each with a
composer. It's built to drive chlive's security design.

**Step 4 (this):** permissions from WorkOS (or, as an alternative, from Postgres). Each pane is
its own connection, as its user, and sees only what that user may read. chlive asks WorkOS
again every 2 s, so a change there shows in the panes at once.

| WorkOS | says | in the panes |
|---|---|---|
| the member's role in the organization `chlive-chat`: `chatter` (has `messages:read`) or `member` | whether they may read `messages` | a `member`'s pane says "No access to messages" (a revoke ends its subscription; Retry subscribes again) |
| `feed-reader` (has `feed:read`) on feeds, one per user (its external_id the user's id) | whose messages | only those authors' messages |

## Running it

This directory is the frontend (React, Vite). Its stack is in its own dev shell, `nix/chat.nix`,
with the server configs in `nix/chlive.chat.toml` (WorkOS) and `nix/chlive.chat.postgres.toml`,
and the schema, seed, WorkOS setup and smoke tests in `scripts/chat/`. Run it instead of the
main stack, not next to it:

```sh
export WORKOS_API_KEY=sk_test_...   # a test environment's key (e.g. in your gitignored .envrc)
nix develop .#chat
workos-setup                  # once: users, roles, feeds in WorkOS (adds only what's missing)
dnvr up                       # Redpanda, ClickHouse, sink, pause server, chlive-server, seed, app
dnvr-state get chat-app.url   # open it
chat-smoke                    # end to end: per user, granted and revoked live in WorkOS
```

WorkOS needs a `feed` resource type, made once in its dashboard (Authorization > Resource types;
there's no API for those); `workos-setup` makes the rest. Change what users may read in the
dashboard (a member's role, their role assignments on feeds), or with the API, and watch the
panes:

```sh
# Dave may read the messages (Retry): `chatter` as his role in the organization
curl -X PUT https://api.workos.com/user_management/organization_memberships/<dave's om_…> \
  -H "Authorization: Bearer $WORKOS_API_KEY" -H "Content-Type: application/json" \
  -d '{"role_slug": "chatter"}'
# Alice sees Carol's messages: `feed-reader` on Carol's feed
curl -X POST https://api.workos.com/authorization/organization_memberships/<alice's om_…>/role_assignments \
  -H "Authorization: Bearer $WORKOS_API_KEY" -H "Content-Type: application/json" \
  -d '{"role_slug": "feed-reader", "resource_type_slug": "feed", "resource_external_id": "carol"}'
```

Every process is prefixed `chat-` (`dnvr logs chat-server`, …); `ch` is a ClickHouse client,
`produce <topic> '<json>'` writes to Kafka, `reset-data` wipes the stack's data (stopped).

Messages are inserted through chlive (`client.insert("messages", …)`), as the pane's user: the
server sets `user_id` to who's inserting (`[tables.messages.insert] user = "user_id"`), so a pane
can't post as someone else, and the app holds no Kafka credentials. The sink puts them into
ClickHouse and chlive into the panes permitted to see them. The clients are `optimistic`: a sent
message shows at once in its own pane, faded and stamped with its author, where the server will
put it, until the server's diff brings it (see the main README's "Optimistic inserts").

## Permissions from WorkOS (`nix/chlive.chat.toml`, `scripts/chat/workos.ts`)

```toml
[security]
default = "deny"                    # tables without security can't be read

[security.providers.workos]
kind = "workos"
organization = "chlive-chat"        # its external_id; the members are its users
poll = "2s"                         # how often watched users are asked again
# api_key_env = "WORKOS_API_KEY"    # where the key is (the default)

[tables.messages.security]
# who may read the table: their role in the organization has the permission
table = { provider = "workos", grants = "messages:read" }
# whose messages: the rows whose user_id is the external_id of a resource (a feed) they hold
# the permission on
provider = "workos"
column = "user_id"
grants = "feed:read"

[tables.users.security]
public = true
```

The app says who each connection is (`new LiveClient({ url, user })`): the user's external_id in
WorkOS, taken as said, since the demo's server trusts declared users (`[auth] mode = "declared"`).
A real app presents a credential instead (`token`, verified by the server: see the main README's
"Authentication"). chlive finds their membership in the organization, and asks, per grant: the resources
under the organization they hold `feed:read` on (their external_ids are the values of
`user_id` they may read), and whether they hold `messages:read` on the organization itself.
Each answer is asked again every `poll` (so it's up to that old), a new user's at once. When it
changes, a subscription gets a new snapshot, or ends with `PERMISSION_DENIED` if it lost the
table. Someone WorkOS doesn't know reads nothing.

As set up: Alice, Bob and Carol are `chatter`s, Dave a `member`; everyone reads their own feed,
Alice also Bob's, Bob also Alice's and Carol's, Carol everyone's.

## Permissions from Postgres (`CHAT_SECURITY=postgres`)

The same, from three tables in the stack's Postgres, followed with logical replication (CDC),
and with columns too: `CHAT_SECURITY=postgres dnvr up`, then `CHAT_SECURITY=postgres chat-smoke`;
`pg` is psql on them (`nix/chlive.chat.postgres.toml`, `scripts/chat/postgres.sql`).

| Postgres table | says | in the panes |
|---|---|---|
| `table_permissions (user_id, table_name)` | which tables a user may read | a pane without `messages` says "No access to messages" |
| `permissions (viewer_id, author_id)` | whose messages a user may see | only those authors' messages |
| `column_permissions (user_id, table_name, column_name)` | which columns of `users` a user may read | names in colour only if the user may read `color` (the panes read `select *`, which follows the columns; naming `color` would be denied) |

```sh
pg -c "insert into permissions values ('alice', 'carol')"                 # Alice sees Carol's messages
pg -c "insert into column_permissions values ('alice', 'users', 'color')" # Alice sees colours
pg -c "insert into table_permissions values ('dave', 'messages')"         # Dave may read messages (Retry)
```

```toml
[security]
default = "deny"
# which tables a user may read (every table needs this grant)
tables = { provider = "pg", grants = "SELECT table_name FROM table_permissions WHERE user_id = {subject:String}" }

[security.providers.pg]
kind = "postgres"
url = "postgres://postgres@127.0.0.1:54329/chat"
publication = "chlive_permissions"
slot = "chlive_chat"                # made anew at each start: one per server

[tables.messages.security]
provider = "pg"
column = "user_id"
grants = "SELECT author_id FROM permissions WHERE viewer_id = {subject:String}"

[tables.users.security]
public = true
columns = { provider = "pg", grants = "SELECT column_name FROM column_permissions WHERE user_id = {subject:String} AND table_name = 'users'" }
```

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
