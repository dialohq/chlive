-- input
SELECT user_id, amount FROM events WHERE amount > 400 LIMIT 5
-- kind: Rows
-- key_columns: ["__partition", "__offset"]
-- output_columns: ["user_id", "amount"]
-- params: {}
-- history_sql
SELECT user_id, amount, _partition AS __partition, _offset AS __offset FROM remote(main_ro, database = 'default', table = 'events') WHERE (amount > 400) AND (toInt64(_offset) <= multiIf(_partition = 0, 120, _partition = 1, 98, -1)) ORDER BY __partition, __offset LIMIT 5
-- tail_state_sql
SELECT user_id, amount, _partition AS __partition, _offset AS __offset FROM tail_events WHERE (amount > 400) AND (toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1)) ORDER BY __partition, __offset LIMIT 5
-- merge_sql
SELECT * FROM (SELECT * FROM sub_1_hist UNION ALL SELECT user_id, amount, _partition AS __partition, _offset AS __offset FROM tail_events WHERE (amount > 400) AND (toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1)) ORDER BY __partition, __offset LIMIT 5) ORDER BY __partition, __offset LIMIT 5
