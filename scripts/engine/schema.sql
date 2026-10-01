-- Schema for the `engine` dev shell: the `events` table, fed from the engine shell's Redpanda
-- the same way scripts/schema.sql feeds the full stack, including the Kafka bookkeeping columns
-- chlive uses to align history with the stream. Applied idempotently on every start.
CREATE TABLE IF NOT EXISTS default.events_queue (
  ts DateTime64(3), user_id String, country LowCardinality(String),
  path String, latency_ms UInt32, amount Float64
) ENGINE = Kafka
SETTINGS kafka_broker_list = '127.0.0.1:29092', kafka_topic_list = 'events',
         kafka_group_name = 'ch-engine', kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3;

CREATE TABLE IF NOT EXISTS default.events (
  ts DateTime64(3), user_id String, country LowCardinality(String),
  path String, latency_ms UInt32, amount Float64,
  _partition UInt64, _offset UInt64
) ENGINE = MergeTree ORDER BY (ts, _partition, _offset);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.events_mv TO default.events AS
SELECT *, _partition, _offset FROM default.events_queue;
