-- input
SELECT country, uniq(user_id) AS users, sum(amount) / count() AS avg_order_value, countIf(amount > 100) AS big_orders, big_orders / count() AS big_order_ratio FROM events WHERE amount > 0 GROUP BY country HAVING users > 50 ORDER BY users DESC LIMIT 5
-- kind: Aggregate
-- key_columns: ["country"]
-- output_columns: ["country", "users", "avg_order_value", "big_orders", "big_order_ratio"]
-- params: {}
-- history_sql
SELECT country, uniqState(user_id) AS __agg_0, sumState(amount) AS __agg_1, countState() AS __agg_2, countIfState(amount > 100) AS __agg_3 FROM remote(main_ro, database = 'default', table = 'events') WHERE (amount > 0) AND (toInt64(_offset) <= multiIf(_partition = 0, 120, _partition = 1, 98, -1)) GROUP BY country
-- tail_state_sql
SELECT country, uniqState(user_id) AS __agg_0, sumState(amount) AS __agg_1, countState() AS __agg_2, countIfState(amount > 100) AS __agg_3 FROM tail_events WHERE (amount > 0) AND (toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1)) GROUP BY country
-- merge_sql
SELECT country, uniqMerge(__agg_0) AS users, sumMerge(__agg_1) / countMerge(__agg_2) AS avg_order_value, countIfMerge(__agg_3) AS big_orders, (countIfMerge(__agg_3)) / countMerge(__agg_2) AS big_order_ratio FROM (SELECT * FROM sub_1_hist UNION ALL SELECT country, uniqState(user_id) AS __agg_0, sumState(amount) AS __agg_1, countState() AS __agg_2, countIfState(amount > 100) AS __agg_3 FROM tail_events WHERE (amount > 0) AND (toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1)) GROUP BY country) GROUP BY country HAVING (uniqMerge(__agg_0)) > 50 ORDER BY (uniqMerge(__agg_0)) DESC LIMIT 5
