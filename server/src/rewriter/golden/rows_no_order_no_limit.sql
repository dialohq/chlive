-- input
SELECT * FROM events
-- kind: Rows
-- key_columns: ["__partition", "__offset"]
-- output_columns: ["ts", "user_id", "country", "path", "latency_ms", "amount", "_partition", "_offset"]
-- params: {}
-- history_sql
SELECT ts, user_id, country, path, latency_ms, amount, _partition, _offset, _partition AS __partition, _offset AS __offset FROM remote(main_ro, database = 'default', table = 'events') WHERE toInt64(_offset) <= multiIf(_partition = 0, 120, _partition = 1, 98, -1)
-- tail_state_sql
SELECT ts, user_id, country, path, latency_ms, amount, _partition, _offset, _partition AS __partition, _offset AS __offset FROM tail_events WHERE toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1)
-- merge_sql
SELECT * FROM (SELECT * FROM sub_1_hist UNION ALL SELECT ts, user_id, country, path, latency_ms, amount, _partition, _offset, _partition AS __partition, _offset AS __offset FROM tail_events WHERE toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1)) ORDER BY __partition, __offset
