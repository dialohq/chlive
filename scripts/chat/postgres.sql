-- The chat demo's Postgres schema: the permissions source, applied by the dnvr
-- `chat-postgres-schema` process on every start (idempotent). chlive reads the permissions
-- through logical replication (CDC): the server runs with wal_level = logical, and the tables
-- are in a publication (see [security] in nix/chlive.chat.postgres.toml).

-- User-to-user permissions: `viewer_id` may see `author_id`'s messages.
CREATE TABLE IF NOT EXISTS permissions (
  viewer_id text NOT NULL,
  author_id text NOT NULL,
  PRIMARY KEY (viewer_id, author_id)
);

-- Which tables (chlive's names) each user may read.
CREATE TABLE IF NOT EXISTS table_permissions (
  user_id text NOT NULL,
  table_name text NOT NULL,
  PRIMARY KEY (user_id, table_name)
);

-- Which columns of a table each user may read.
CREATE TABLE IF NOT EXISTS column_permissions (
  user_id text NOT NULL,
  table_name text NOT NULL,
  column_name text NOT NULL,
  PRIMARY KEY (user_id, table_name, column_name)
);

-- What's replicated: inserts, updates and deletes of the three (deletes identify the row by
-- its primary key, the tables' default replica identity).
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'chlive_permissions') THEN
    CREATE PUBLICATION chlive_permissions;
  END IF;
END
$$;
ALTER PUBLICATION chlive_permissions SET TABLE permissions, table_permissions, column_permissions;

-- The starting permissions, into an empty table only (changes made since stay): everyone sees
-- themselves; Alice also sees Bob; Bob sees Alice and Carol; Carol sees everyone; Dave only
-- himself.
INSERT INTO permissions (viewer_id, author_id)
SELECT * FROM (VALUES
  ('alice', 'alice'), ('alice', 'bob'),
  ('bob', 'bob'), ('bob', 'alice'), ('bob', 'carol'),
  ('carol', 'carol'), ('carol', 'alice'), ('carol', 'bob'), ('carol', 'dave'),
  ('dave', 'dave')
) AS seed (viewer_id, author_id)
WHERE NOT EXISTS (SELECT 1 FROM permissions);

-- Everyone reads the users; everyone but Dave the messages.
INSERT INTO table_permissions (user_id, table_name)
SELECT u, t FROM unnest(ARRAY['alice', 'bob', 'carol', 'dave']) AS u,
  unnest(ARRAY['users', 'messages']) AS t
WHERE (u, t) <> ('dave', 'messages')
  AND NOT EXISTS (SELECT 1 FROM table_permissions);

-- Of the users, everyone reads the ids, names and versions; only Carol their colours.
INSERT INTO column_permissions (user_id, table_name, column_name)
SELECT u, 'users', c FROM unnest(ARRAY['alice', 'bob', 'carol', 'dave']) AS u,
  unnest(ARRAY['user_id', 'name', 'version', 'color']) AS c
WHERE (c <> 'color' OR u = 'carol')
  AND NOT EXISTS (SELECT 1 FROM column_permissions);
