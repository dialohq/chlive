-- input
SELECT path, max(latency_ms) AS worst_ms, argMax(user_id, latency_ms) AS worst_user FROM events GROUP BY path ORDER BY worst_ms DESC LIMIT 10
-- kind: Aggregate
-- key_columns: ["path"]
-- output_columns: ["path", "worst_ms", "worst_user"]
-- params: {}
-- history_sql
SELECT path, maxState(latency_ms) AS __agg_0, argMaxState(user_id, latency_ms) AS __agg_1 FROM remote(main_ro, database = 'default', table = 'events') WHERE toInt64(_offset) <= multiIf(_partition = 0, 120, _partition = 1, 98, -1) GROUP BY path
-- tail_state_sql
SELECT path, maxState(latency_ms) AS __agg_0, argMaxState(user_id, latency_ms) AS __agg_1 FROM tail_events WHERE toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1) GROUP BY path
-- merge_sql
SELECT path, maxMerge(__agg_0) AS worst_ms, argMaxMerge(__agg_1) AS worst_user FROM (SELECT * FROM sub_1_hist UNION ALL SELECT path, maxState(latency_ms) AS __agg_0, argMaxState(user_id, latency_ms) AS __agg_1 FROM tail_events WHERE toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1) GROUP BY path) GROUP BY path ORDER BY (maxMerge(__agg_0)) DESC LIMIT 10
