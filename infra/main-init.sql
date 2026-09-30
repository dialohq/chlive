-- Main ClickHouse schema. Assumptions (see NOTES.md):
--  * one Kafka message = one row (JSONEachRow), MV is a pure passthrough adding _partition/_offset
--  * plain append-only MergeTree
CREATE TABLE IF NOT EXISTS default.events_queue (
  ts DateTime64(3), user_id String, country LowCardinality(String),
  path String, latency_ms UInt32, amount Float64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:9092', kafka_topic_list = 'events',
         kafka_group_name = 'ch-main', kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3;

CREATE TABLE IF NOT EXISTS default.events (
  ts DateTime64(3), user_id String, country LowCardinality(String),
  path String, latency_ms UInt32, amount Float64,
  _partition UInt64, _offset UInt64
) ENGINE = MergeTree ORDER BY (ts, _partition, _offset);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.events_mv TO default.events AS
SELECT *, _partition, _offset FROM default.events_queue;
