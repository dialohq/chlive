-- Schema for the `engine` dev shell: main ClickHouse without Kafka ingestion, for testing
-- chlive-engine on its own. Same columns as the full stack's `events` table (scripts/schema.sql),
-- including the hidden Kafka bookkeeping columns.
CREATE TABLE IF NOT EXISTS default.events (
  ts DateTime64(3), user_id String, country LowCardinality(String),
  path String, latency_ms UInt32, amount Float64,
  _partition UInt64, _offset UInt64
) ENGINE = MergeTree ORDER BY (ts, _partition, _offset);
