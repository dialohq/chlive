-- 10 fixed events for the `engine` dev shell, produced to Kafka through the Kafka-engine table
-- (so they get real _partition/_offset); applied once, when `default.events` is empty.
INSERT INTO default.events_queue FORMAT JSONEachRow
{"ts":"2026-10-01 12:00:00.000","user_id":"u1","country":"PL","path":"/","latency_ms":12,"amount":0}
{"ts":"2026-10-01 12:00:01.000","user_id":"u2","country":"DE","path":"/search","latency_ms":48,"amount":0}
{"ts":"2026-10-01 12:00:02.000","user_id":"u3","country":"US","path":"/product","latency_ms":95,"amount":0}
{"ts":"2026-10-01 12:00:03.000","user_id":"u1","country":"PL","path":"/cart","latency_ms":31,"amount":0}
{"ts":"2026-10-01 12:00:04.000","user_id":"u4","country":"GB","path":"/checkout","latency_ms":210,"amount":129.99}
{"ts":"2026-10-01 12:00:05.000","user_id":"u2","country":"DE","path":"/login","latency_ms":7,"amount":0}
{"ts":"2026-10-01 12:00:06.000","user_id":"u5","country":"FR","path":"/account","latency_ms":66,"amount":0}
{"ts":"2026-10-01 12:00:07.000","user_id":"u3","country":"US","path":"/api/orders","latency_ms":1530,"amount":42.5}
{"ts":"2026-10-01 12:00:08.000","user_id":"u6","country":"JP","path":"/","latency_ms":18,"amount":0}
{"ts":"2026-10-01 12:00:09.000","user_id":"u4","country":"GB","path":"/checkout","latency_ms":340,"amount":15}
