export const PATHS = ["/", "/search", "/product", "/cart", "/checkout", "/login", "/account", "/api/orders"];

export const Q = {
  latest: `SELECT ts, user_id, country, path, latency_ms
FROM events
ORDER BY ts DESC
LIMIT 20`,
  counters: `SELECT count() AS total, avg(latency_ms) AS avg_latency, sum(amount) AS revenue
FROM events`,
  topCountries: `SELECT country, count() AS n
FROM events
WHERE path = {path:String}
GROUP BY country
ORDER BY n DESC
LIMIT 10`,
  perMinute: `SELECT toStartOfMinute(ts) AS minute,
       count() AS requests,
       quantile(0.95)(latency_ms) AS p95,
       quantiles(0.5, 0.99)(latency_ms) AS p50_p99
FROM events
WHERE ts >= {since:DateTime64(3)}
GROUP BY minute
ORDER BY minute`,
  orders: `SELECT country,
       uniq(user_id) AS users,
       sum(amount) / count() AS avg_order_value,
       countIf(amount > 100) AS big_orders,
       big_orders / count() AS big_order_ratio
FROM events
WHERE amount > 0
GROUP BY country
HAVING users > 50
ORDER BY users DESC
LIMIT 5`,
  slowest: `SELECT path,
       max(latency_ms) AS worst_ms,
       argMax(user_id, latency_ms) AS worst_user
FROM events
GROUP BY path
ORDER BY worst_ms DESC
LIMIT 10`,
};

export const REJECTIONS = [
  { label: "time-relative", sql: "SELECT count() FROM events WHERE ts > now() - INTERVAL 1 HOUR" },
  { label: "join", sql: "SELECT e.country, d.name FROM events e JOIN countries d ON e.country = d.code" },
  { label: "no LIMIT", sql: "SELECT * FROM events" },
  { label: "window", sql: "SELECT count() OVER () FROM events" },
];

/** ClickHouse DateTime64(3) literal in UTC, e.g. `2026-09-30 16:00:00.000`. */
export function chTime(d: Date): string {
  return d.toISOString().replace("T", " ").replace("Z", "");
}
