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

-- Related tables for the simple demo (same assumptions: one message = one row, append-only).
-- A customer "update" is a new row; queries take the latest value with argMax(col, ts).
CREATE TABLE IF NOT EXISTS default.customers_queue (
  ts DateTime64(3), customer_id String, name String, country LowCardinality(String)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:9092', kafka_topic_list = 'customers',
         kafka_group_name = 'ch-main-customers', kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3;

CREATE TABLE IF NOT EXISTS default.customers (
  ts DateTime64(3), customer_id String, name String, country LowCardinality(String),
  _partition UInt64, _offset UInt64
) ENGINE = MergeTree ORDER BY (customer_id, ts, _partition, _offset);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.customers_mv TO default.customers AS
SELECT *, _partition, _offset FROM default.customers_queue;

CREATE TABLE IF NOT EXISTS default.orders_queue (
  ts DateTime64(3), order_id String, customer_id String, amount Float64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:9092', kafka_topic_list = 'orders',
         kafka_group_name = 'ch-main-orders', kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3;

CREATE TABLE IF NOT EXISTS default.orders (
  ts DateTime64(3), order_id String, customer_id String, amount Float64,
  _partition UInt64, _offset UInt64
) ENGINE = MergeTree ORDER BY (ts, _partition, _offset);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.orders_mv TO default.orders AS
SELECT *, _partition, _offset FROM default.orders_queue;
