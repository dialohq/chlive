-- input
SELECT ts, path FROM events WHERE path = {path:String} ORDER BY ts DESC
-- kind: Rows
-- key_columns: ["__partition", "__offset"]
-- output_columns: ["ts", "path"]
-- params: {"path": "String"}
-- history_sql
SELECT ts, path, ts AS __ord_0, _partition AS __partition, _offset AS __offset FROM remote(main_ro, database = 'default', table = 'events') WHERE (path = {path:String}) AND (toInt64(_offset) <= multiIf(_partition = 0, 120, _partition = 1, 98, -1))
-- tail_state_sql
SELECT ts, path, ts AS __ord_0, _partition AS __partition, _offset AS __offset FROM tail_events WHERE (path = {path:String}) AND (toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1))
-- merge_sql
SELECT * FROM (SELECT * FROM sub_1_hist UNION ALL SELECT ts, path, ts AS __ord_0, _partition AS __partition, _offset AS __offset FROM tail_events WHERE (path = {path:String}) AND (toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1))) ORDER BY __ord_0 DESC, __partition, __offset
