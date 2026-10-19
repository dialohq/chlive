-- 60 messages over the last hour, the four users in turn, rendered in UTC (the stack's
-- ClickHouse reads them as UTC); clickhouse local, produced to Kafka by the seed.
WITH
  ['alice', 'bob', 'carol', 'dave'] AS users,
  ['morning!', 'anyone seen the deploy logs?', 'lunch at noon?', 'shipping it today',
   'PR is up for review', 'that test is flaky again', 'nice work 🎉', 'on it',
   'can we move the sync to 3pm?', 'the dashboard looks great', 'coffee?', 'brb'] AS lines
SELECT
  now64(3, 'UTC') - toIntervalSecond((60 - number) * 60) AS ts,
  generateUUIDv4(number) AS message_id,
  users[number % 4 + 1] AS user_id,
  lines[number % length(lines) + 1] AS body
FROM numbers(60)
FORMAT JSONEachRow
