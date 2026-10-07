-- Historical events, as JSON lines to produce to the `events` topic; the sink connector then
-- ingests them. (Inserting into `events` directly would put rows in ClickHouse that aren't in
-- Kafka, and the sink's offsets wouldn't match them.)
--
-- Usage: clickhouse local --param_count=1000 --queries-file scripts/seed.sql | produce events
-- Timestamps are spread uniformly over the last 7 days.
WITH
    -- weighted country mix (weights sum to 100)
    arrayFlatten(arrayMap((c, w) -> arrayResize([c], w, c),
        ['US', 'DE', 'GB', 'PL', 'FR', 'IN', 'BR', 'JP', 'CA', 'ES', 'IT', 'NL', 'SE', 'AU', 'MX'],
        [30, 12, 10, 8, 8, 7, 5, 5, 4, 3, 3, 2, 1, 1, 1])) AS countries,
    ['/', '/search', '/product', '/cart', '/checkout', '/login', '/account', '/api/orders'] AS paths
SELECT
    now64(3) - toIntervalMillisecond(rand(1) % (7 * 86400000)) AS ts,
    concat('u', toString(rand(2) % 5000)) AS user_id,
    countries[rand(3) % length(countries) + 1] AS country,
    paths[rand(4) % length(paths) + 1] AS path,
    -- long-tailed latency: mostly 10-200 ms, occasionally seconds
    toUInt32(200 * pow(-log(1 - randCanonical(5)), 1.6)) + 1 AS latency_ms,
    -- 60% of events carry no amount, the rest 1.00-500.00
    if(randCanonical(6) < 0.6, 0, (100 + rand(7) % 49900) / 100) AS amount
FROM numbers({count:UInt64})
FORMAT JSONEachRow;
