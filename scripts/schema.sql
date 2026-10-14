-- ClickHouse schema, applied by the dnvr `schema` process on every start (idempotent).
-- Ingestion is Kafka Connect's ClickHouse sink (one connector per topic, exactly once): it
-- inserts each message as one row into the table of the same name, and keeps its offsets in a
-- KeeperMap table per connector, which the pause server reads when chlive snapshots a table.
-- Assumptions:
--  * one Kafka message = one row (JSON), plain append-only MergeTree, no per-row Kafka offsets
--  * a message that can't be inserted goes to the dead-letter queue (and its offset counts as
--    ingested); chlive-engine skips messages it can't decode, so history and stream agree on
--    malformed events either way
CREATE TABLE IF NOT EXISTS default.events (
  ts DateTime64(3), user_id String, country LowCardinality(String),
  path String, latency_ms UInt32, amount Float64, session_id Nullable(UUID)
) ENGINE = MergeTree ORDER BY ts;
-- added later; for tables created before it (before the sink connectors start, so they see it)
ALTER TABLE default.events ADD COLUMN IF NOT EXISTS session_id Nullable(UUID);

-- Related tables for the example (same assumptions).
CREATE TABLE IF NOT EXISTS default.customers (
  ts DateTime64(3), customer_id String, name String, country LowCardinality(String)
) ENGINE = MergeTree ORDER BY (customer_id, ts);

CREATE TABLE IF NOT EXISTS default.orders (
  ts DateTime64(3), order_id String, customer_id String, amount Float64
) ENGINE = MergeTree ORDER BY ts;

-- The sink's exactly-once state (the connectors' `zkDatabase` / `zkPath`), as continuous-backup
-- creates it: per partition (`<topic>-<p>`), the offsets of the last insert and its state.
CREATE TABLE IF NOT EXISTS default.chlive_sink_events_state
  (key String, minOffset Int64, maxOffset Int64, state String)
  ENGINE = KeeperMap('/chlive-sink/events') PRIMARY KEY key;
CREATE TABLE IF NOT EXISTS default.chlive_sink_customers_state
  (key String, minOffset Int64, maxOffset Int64, state String)
  ENGINE = KeeperMap('/chlive-sink/customers') PRIMARY KEY key;
CREATE TABLE IF NOT EXISTS default.chlive_sink_orders_state
  (key String, minOffset Int64, maxOffset Int64, state String)
  ENGINE = KeeperMap('/chlive-sink/orders') PRIMARY KEY key;

-- Read-only user chlive-server's engine connects as (DESCRIBE and history, from snapshots).
-- readonly=2: read-only, but per-query settings are allowed (the engine sends output formats).
CREATE USER IF NOT EXISTS chlive_ro IDENTIFIED WITH plaintext_password BY 'chlive_ro'
  SETTINGS readonly = 2, max_execution_time = 60, max_memory_usage = 4000000000;
GRANT SELECT ON default.* TO chlive_ro;

-- The user chlive-server takes snapshots as: clones (`__chlive_snap_*`) of the tables, created
-- next to them with CREATE TABLE … AS + ATTACH PARTITION ALL FROM, and dropped when replaced.
CREATE USER IF NOT EXISTS chlive_snap IDENTIFIED WITH plaintext_password BY 'chlive_snap';
-- ATTACH PARTITION FROM needs INSERT and ALTER on the clone
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER ON default.* TO chlive_snap;
GRANT SYSTEM SYNC REPLICA ON default.* TO chlive_snap;
GRANT SELECT ON system.tables TO chlive_snap;

-- Where the dev server makes its snapshots (`clickhouse.snapshot_database` in
-- nix/chlive.dev.toml): clones are created and dropped there, and history is read from them.
CREATE DATABASE IF NOT EXISTS chlive_snapshots;
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER ON chlive_snapshots.* TO chlive_snap;
GRANT SELECT ON chlive_snapshots.* TO chlive_ro;
