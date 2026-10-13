import { test } from "bun:test";
import assert from "node:assert/strict";

import { columnParsers, parseClickHouseDate, parseClickHouseType, resolveParsers } from "../src/parsers.ts";

test("ClickHouse types are taken apart", () => {
  assert.deepEqual(parseClickHouseType("Nullable(DateTime64(3, 'UTC'))"), {
    type: "Nullable(DateTime64(3, 'UTC'))",
    base: "DateTime64",
    args: ["3", "'UTC'"],
    nullable: true,
  });
  assert.deepEqual(parseClickHouseType("LowCardinality(Nullable(Date))"), {
    type: "LowCardinality(Nullable(Date))",
    base: "Date",
    args: [],
    nullable: true,
  });
  assert.deepEqual(parseClickHouseType("Map(String, Tuple(UInt8, String))").args, ["String", "Tuple(UInt8, String)"]);
  assert.deepEqual(parseClickHouseType("Enum8('a, b' = 1, 'c' = 2)").args, ["'a, b' = 1", "'c' = 2"]);
  assert.equal(parseClickHouseType("Array(DateTime)").base, "Array");
});

test("dates and times are read as UTC, to the millisecond", () => {
  const iso = (s: string) => parseClickHouseDate(s).toISOString();
  assert.equal(iso("2026-10-01"), "2026-10-01T00:00:00.000Z");
  assert.equal(iso("2026-10-01 12:34:56"), "2026-10-01T12:34:56.000Z");
  assert.equal(iso("2026-10-01 12:34:56.5"), "2026-10-01T12:34:56.500Z");
  assert.equal(iso("2026-10-01 12:34:56.123456"), "2026-10-01T12:34:56.123Z");
  assert.equal(iso("2026-10-01 12:34:56.123456789"), "2026-10-01T12:34:56.123Z");
  assert.equal(iso("2026-10-01T12:34:56.123Z"), "2026-10-01T12:34:56.123Z");
  assert.equal(iso("1969-12-31 23:59:59.995"), "1969-12-31T23:59:59.995Z");
  assert.equal(parseClickHouseDate("0042-01-01").getUTCFullYear(), 42);
  assert.throws(() => parseClickHouseDate("yesterday"), /not a ClickHouse date or time: "yesterday"/);
  assert.throws(() => parseClickHouseDate("2026-10-01 12:34"));
});

test("date types have parsers by default, which can be replaced, removed or all turned off", () => {
  const columns = [
    { name: "d", type: "Date" },
    { name: "d32", type: "Nullable(Date32)" },
    { name: "t", type: "DateTime('UTC')" },
    { name: "t64", type: "DateTime64(6)" },
    { name: "tags", type: "Array(DateTime)" },
    { name: "id", type: "UUID" },
  ];
  const names = (o?: Parameters<typeof resolveParsers>[0]) => columnParsers(columns, resolveParsers(o)).map((c) => c.name);
  assert.deepEqual(names(), ["d", "d32", "t", "t64"]);
  assert.deepEqual(names({ DateTime: null, UUID: (v) => v }), ["d", "d32", "t64", "id"]);
  assert.deepEqual(names(false), []);
  // the option never changes the defaults themselves
  assert.deepEqual(names(), ["d", "d32", "t", "t64"]);
});
