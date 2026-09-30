-- input
SELECT country, count() AS n FROM events WHERE path = {path:String} GROUP BY country ORDER BY n DESC LIMIT 10
-- kind: Aggregate
-- key_columns: ["country"]
-- output_columns: ["country", "n"]
-- params: {"path": "String"}
-- history_sql
SELECT country, countState() AS __agg_0 FROM remote(main_ro, database = 'default', table = 'events') WHERE (path = {path:String}) AND (toInt64(_offset) <= multiIf(_partition = 0, 120, _partition = 1, 98, -1)) GROUP BY country
-- tail_state_sql
SELECT country, countState() AS __agg_0 FROM tail_events WHERE (path = {path:String}) AND (toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1)) GROUP BY country
-- merge_sql
SELECT country, countMerge(__agg_0) AS n FROM (SELECT * FROM sub_1_hist UNION ALL SELECT country, countState() AS __agg_0 FROM tail_events WHERE (path = {path:String}) AND (toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1)) GROUP BY country) GROUP BY country ORDER BY (countMerge(__agg_0)) DESC LIMIT 10
