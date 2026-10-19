-- The chat demo's ClickHouse schema, applied by the dnvr `chat-schema` process on every start
-- (idempotent). Ingestion is Kafka Connect's ClickHouse sink (one connector per topic, exactly
-- once; it creates its own KeeperMap state tables): one Kafka message = one row.
--
-- One group chat: everyone sees everyone's messages.

-- Messages: written once, never changed.
CREATE TABLE IF NOT EXISTS default.messages (
  ts DateTime64(3), message_id UUID, user_id String, body String
) ENGINE = MergeTree ORDER BY ts;

-- Users: the latest version of each wins (with FINAL).
CREATE TABLE IF NOT EXISTS default.users (
  user_id String, name String, color String, version UInt64
) ENGINE = ReplacingMergeTree(version) ORDER BY user_id;

-- chlive-server's users, as in the main stack: chlive_ro reads (history, from snapshots),
-- chlive_snap makes the snapshots.
CREATE USER IF NOT EXISTS chlive_ro IDENTIFIED WITH plaintext_password BY 'chlive_ro'
  SETTINGS readonly = 2, max_execution_time = 60, max_memory_usage = 4000000000;
GRANT SELECT ON default.* TO chlive_ro;
GRANT SELECT ON system.tables TO chlive_ro;

CREATE USER IF NOT EXISTS chlive_snap IDENTIFIED WITH plaintext_password BY 'chlive_snap';
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER ON default.* TO chlive_snap;
GRANT SYSTEM SYNC REPLICA ON default.* TO chlive_snap;
GRANT SELECT ON system.tables TO chlive_snap;

CREATE DATABASE IF NOT EXISTS chlive_snapshots;
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER ON chlive_snapshots.* TO chlive_snap;
GRANT SELECT ON chlive_snapshots.* TO chlive_ro;
