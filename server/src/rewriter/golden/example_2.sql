-- input
SELECT count() AS total, avg(latency_ms) AS avg_latency, sum(amount) AS revenue FROM events
-- kind: Aggregate
-- key_columns: []
-- output_columns: ["total", "avg_latency", "revenue"]
-- params: {}
-- history_sql
SELECT countState() AS __agg_0, avgState(latency_ms) AS __agg_1, sumState(amount) AS __agg_2 FROM remote(main_ro, database = 'default', table = 'events') WHERE toInt64(_offset) <= multiIf(_partition = 0, 120, _partition = 1, 98, -1)
-- tail_state_sql
SELECT countState() AS __agg_0, avgState(latency_ms) AS __agg_1, sumState(amount) AS __agg_2 FROM tail_events WHERE toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1)
-- merge_sql
SELECT countMerge(__agg_0) AS total, avgMerge(__agg_1) AS avg_latency, sumMerge(__agg_2) AS revenue FROM (SELECT * FROM sub_1_hist UNION ALL SELECT countState() AS __agg_0, avgState(latency_ms) AS __agg_1, sumState(amount) AS __agg_2 FROM tail_events WHERE toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1)) LIMIT 1001
