-- input
SELECT toStartOfMinute(ts) AS minute, count() AS requests, quantile(0.95)(latency_ms) AS p95, quantiles(0.5, 0.99)(latency_ms) AS p50_p99 FROM events WHERE ts >= {since:DateTime64(3)} GROUP BY minute ORDER BY minute
-- kind: Aggregate
-- key_columns: ["minute"]
-- output_columns: ["minute", "requests", "p95", "p50_p99"]
-- params: {"since": "DateTime64(3)"}
-- history_sql
SELECT toStartOfMinute(ts) AS minute, countState() AS __agg_0, quantileState(0.95)(latency_ms) AS __agg_1, quantilesState(0.5, 0.99)(latency_ms) AS __agg_2 FROM remote(main_ro, database = 'default', table = 'events') WHERE (ts >= {since:DateTime64(3)}) AND (toInt64(_offset) <= multiIf(_partition = 0, 120, _partition = 1, 98, -1)) GROUP BY minute
-- tail_state_sql
SELECT toStartOfMinute(ts) AS minute, countState() AS __agg_0, quantileState(0.95)(latency_ms) AS __agg_1, quantilesState(0.5, 0.99)(latency_ms) AS __agg_2 FROM tail_events WHERE (ts >= {since:DateTime64(3)}) AND (toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1)) GROUP BY minute
-- merge_sql
SELECT minute, countMerge(__agg_0) AS requests, quantileMerge(0.95)(__agg_1) AS p95, quantilesMerge(0.5, 0.99)(__agg_2) AS p50_p99 FROM (SELECT * FROM sub_1_hist UNION ALL SELECT toStartOfMinute(ts) AS minute, countState() AS __agg_0, quantileState(0.95)(latency_ms) AS __agg_1, quantilesState(0.5, 0.99)(latency_ms) AS __agg_2 FROM tail_events WHERE (ts >= {since:DateTime64(3)}) AND (toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1)) GROUP BY minute) GROUP BY minute ORDER BY minute LIMIT 1001
