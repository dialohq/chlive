-- The chat demo's Postgres schema: the permissions source, applied by the dnvr
-- `chat-postgres-schema` process on every start (idempotent). chlive reads the permissions
-- through logical replication (CDC): the server runs with wal_level = logical, and the table
-- is in a publication (see [security] in nix/chlive.chat.toml).

-- User-to-user permissions: `viewer_id` may see `author_id`'s messages.
CREATE TABLE IF NOT EXISTS permissions (
  viewer_id text NOT NULL,
  author_id text NOT NULL,
  PRIMARY KEY (viewer_id, author_id)
);

-- What's replicated: inserts, updates and deletes of the permissions (deletes identify the
-- row by its primary key, the table's default replica identity).
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'chlive_permissions') THEN
    CREATE PUBLICATION chlive_permissions FOR TABLE permissions;
  END IF;
END
$$;

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
