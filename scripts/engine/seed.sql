-- 10 fixed events for the `engine` dev shell. Re-applied on every start, so the table always
-- holds exactly these rows.
TRUNCATE TABLE default.events;

INSERT INTO default.events VALUES
  ('2026-10-01 12:00:00.000', 'u1', 'PL', '/',          12,  0,      0, 0),
  ('2026-10-01 12:00:01.000', 'u2', 'DE', '/search',    48,  0,      1, 0),
  ('2026-10-01 12:00:02.000', 'u3', 'US', '/product',   95,  0,      2, 0),
  ('2026-10-01 12:00:03.000', 'u1', 'PL', '/cart',      31,  0,      0, 1),
  ('2026-10-01 12:00:04.000', 'u4', 'GB', '/checkout', 210,  129.99, 1, 1),
  ('2026-10-01 12:00:05.000', 'u2', 'DE', '/login',      7,  0,      2, 1),
  ('2026-10-01 12:00:06.000', 'u5', 'FR', '/account',   66,  0,      0, 2),
  ('2026-10-01 12:00:07.000', 'u3', 'US', '/api/orders', 1530, 42.5, 1, 2),
  ('2026-10-01 12:00:08.000', 'u6', 'JP', '/',          18,  0,      2, 2),
  ('2026-10-01 12:00:09.000', 'u4', 'GB', '/checkout', 340,  15,     0, 3);
