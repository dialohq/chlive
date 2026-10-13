import { test } from "bun:test";
import assert from "node:assert/strict";

import { ResultStore } from "../src/state.ts";

const columns = [{ name: "n", type: "UInt32" }];

test("snapshot, then inserts in arrival order", () => {
  const s = new ResultStore<{ n: number }>();
  s.applySnapshot({ type: "snapshot", id: "a", columns, rows: [{ n: 1 }, { n: 2 }] });
  assert.equal(s.size, 2);
  const before = s.rows();
  s.applyDiff({ type: "diff", id: "a", changes: [{ op: "insert", row: { n: 3 } }, { op: "insert", row: { n: 4 } }] });
  assert.deepEqual(s.rows().map((r) => r.n), [1, 2, 3, 4]);
  assert.deepEqual(before.map((r) => r.n), [1, 2], "earlier arrays are not mutated");
});

test("a snapshot replaces everything", () => {
  const s = new ResultStore<{ n: number }>();
  s.applySnapshot({ type: "snapshot", id: "a", columns, rows: [{ n: 1 }] });
  s.applyDiff({ type: "diff", id: "a", changes: [{ op: "insert", row: { n: 2 } }] });
  s.applySnapshot({ type: "snapshot", id: "a", columns, rows: [{ n: 1 }, { n: 2 }, { n: 3 }] });
  assert.deepEqual(s.rows().map((r) => r.n), [1, 2, 3]);
});

test("unknown ops throw and leave the rows untouched", () => {
  const s = new ResultStore<{ n: number }>();
  s.applySnapshot({ type: "snapshot", id: "a", columns, rows: [{ n: 1 }] });
  assert.throws(
    () => s.applyDiff({ type: "diff", id: "a", changes: [{ op: "insert", row: { n: 2 } }, { op: "move" } as any] }),
    /unsupported change "move"/,
  );
  assert.deepEqual(s.rows().map((r) => r.n), [1]);
});

test("ordered inserts land at their index, applied in sequence", () => {
  const s = new ResultStore<{ n: number }>();
  const order = [{ column: "n", direction: "desc" as const, nulls: "last" as const }];
  s.applySnapshot({ type: "snapshot", id: "a", columns, rows: [{ n: 9 }, { n: 5 }, { n: 1 }], order });
  s.applyDiff({
    type: "diff",
    id: "a",
    changes: [
      { op: "insert", row: { n: 10 }, index: 0 }, // 10 9 5 1
      { op: "insert", row: { n: 7 }, index: 2 }, // 10 9 7 5 1
      { op: "insert", row: { n: 0 }, index: 5 }, // ... 1 0: the end
    ],
  });
  assert.deepEqual(s.rows().map((r) => r.n), [10, 9, 7, 5, 1, 0]);
});

test("an index outside the list throws and leaves the rows untouched", () => {
  const s = new ResultStore<{ n: number }>();
  s.applySnapshot({ type: "snapshot", id: "a", columns, rows: [{ n: 1 }] });
  for (const index of [3, -1, 0.5]) {
    assert.throws(
      () => s.applyDiff({ type: "diff", id: "a", changes: [{ op: "insert", row: { n: 2 }, index: 0 }, { op: "insert", row: { n: 3 }, index }] }),
      /insert at index/,
    );
  }
  assert.deepEqual(s.rows().map((r) => r.n), [1]);
});

test("LIMIT: a diff's inserts and deletes apply together", () => {
  const s = new ResultStore<{ n: number }>();
  s.applySnapshot({ type: "snapshot", id: "a", columns, rows: [{ n: 1 }, { n: 2 }], limit: 2 });
  // window: append, then the first row goes
  s.applyDiff({ type: "diff", id: "a", changes: [{ op: "insert", row: { n: 3 } }, { op: "delete", index: 0 }] });
  assert.deepEqual(s.rows().map((r) => r.n), [2, 3]);
  // top-N: insert inside, then the row pushed past the end goes
  s.applyDiff({ type: "diff", id: "a", changes: [{ op: "insert", row: { n: 9 }, index: 0 }, { op: "delete", index: 2 }] });
  assert.deepEqual(s.rows().map((r) => r.n), [9, 2]);
  assert.throws(() => s.applyDiff({ type: "diff", id: "a", changes: [{ op: "delete", index: 2 }] }), /delete at index 2/);
  assert.deepEqual(s.rows().map((r) => r.n), [9, 2]);
});

test("aggregated results: rows by id, updated in place", () => {
  const s = new ResultStore<{ country: string; n: number; avg?: number }>();
  s.applySnapshot({
    type: "snapshot",
    id: "a",
    columns,
    rows: [{ country: "PL", n: 1, avg: 10 }, { country: "DE", n: 2, avg: 5 }],
    ids: ['["PL"]', '["DE"]'],
    at: "c1",
  });
  const before = s.rows();
  s.applyDiff({
    type: "diff",
    id: "a",
    at: "c2",
    changes: [
      { op: "update", id: '["DE"]', set: { n: 3, avg: 6 } },
      { op: "insert", row: { country: "FR", n: 1, avg: 2 }, id: '["FR"]' },
      { op: "update", id: '["FR"]', set: { n: 2 } }, // a group inserted in this same diff
    ],
  });
  assert.deepEqual(s.rows(), [
    { country: "PL", n: 1, avg: 10 },
    { country: "DE", n: 3, avg: 6 },
    { country: "FR", n: 2, avg: 2 },
  ]);
  assert.deepEqual(before[1], { country: "DE", n: 2, avg: 5 }, "earlier rows are not mutated");
  assert.deepEqual(s.ids(), ['["PL"]', '["DE"]', '["FR"]']);
  assert.throws(
    () => s.applyDiff({ type: "diff", id: "a", at: "c3", changes: [{ op: "update", id: '["XX"]', set: { n: 1 } }] }),
    /update of unknown row \["XX"\]/,
  );
  assert.equal(s.rows()[2].n, 2);
});

test("ordered aggregates: an update with an index moves the row", () => {
  const s = new ResultStore<{ country: string; n: number }>();
  s.applySnapshot({
    type: "snapshot",
    id: "a",
    columns,
    rows: [{ country: "PL", n: 5 }, { country: "DE", n: 3 }, { country: "FR", n: 1 }],
    ids: ['["PL"]', '["DE"]', '["FR"]'],
    at: "c1",
  });
  s.applyDiff({
    type: "diff",
    id: "a",
    at: "c2",
    changes: [
      { op: "update", id: '["FR"]', set: { n: 9 }, index: 0 }, // to the top
      { op: "update", id: '["PL"]', set: {}, index: 2 }, // only moves (a hidden sort column changed)
      { op: "update", id: '["FR"]', set: { n: 10 } }, // found by id after moving
    ],
  });
  assert.deepEqual(s.rows(), [
    { country: "FR", n: 10 },
    { country: "DE", n: 3 },
    { country: "PL", n: 5 },
  ]);
  assert.deepEqual(s.ids(), ['["FR"]', '["DE"]', '["PL"]']);
  assert.throws(
    () => s.applyDiff({ type: "diff", id: "a", at: "c3", changes: [{ op: "update", id: '["DE"]', set: {}, index: 3 }] }),
    /move to index 3 of a result with 3 rows/,
  );
});

// --- value parsers ---------------------------------------------------------------------------

const TS = [
  { name: "id", type: "String" },
  { name: "ts", type: "Nullable(DateTime64(3))" },
];
const at = (s: string) => new Date(s.replace(" ", "T") + "Z");

test("date columns are Dates; the wire rows keep the strings", () => {
  const s = new ResultStore<{ id: string; ts: Date | null }>();
  const wire = { id: "a", ts: "2026-10-01 12:00:00.500" };
  s.applySnapshot({ type: "snapshot", id: "q", columns: TS, rows: [wire, { id: "n", ts: null }] });
  assert.deepEqual(s.rows(), [{ id: "a", ts: at("2026-10-01 12:00:00.500") }, { id: "n", ts: null }]);
  assert.deepEqual(s.wireRows(), [wire, { id: "n", ts: null }]);
  assert.equal(wire.ts, "2026-10-01 12:00:00.500", "the message is not mutated");
  s.applyDiff({ type: "diff", id: "q", changes: [{ op: "insert", row: { id: "b", ts: "2026-10-02 00:00:00.000" } }] });
  assert.ok(s.rows()[2].ts instanceof Date);
  assert.equal(s.wireRows()[2].ts, "2026-10-02 00:00:00.000");
});

test("parsed and wire rows stay aligned through inserts, deletes and moves", () => {
  const cols = [
    { name: "k", type: "String" },
    { name: "last", type: "DateTime" },
    { name: "n", type: "UInt64" },
  ];
  const s = new ResultStore<{ k: string; last: Date; n: number }>();
  const row = (k: string, sec: number, n: number) => ({ k, last: `2026-10-01 00:00:${String(sec).padStart(2, "0")}`, n });
  s.applySnapshot({ type: "snapshot", id: "q", columns: cols, rows: [row("a", 1, 1), row("b", 2, 1)], ids: ['["a"]', '["b"]'] });
  s.applyDiff({
    type: "diff",
    id: "q",
    changes: [
      { op: "insert", row: row("c", 3, 1), id: '["c"]', index: 0 },
      { op: "update", id: '["a"]', set: { last: "2026-10-01 00:00:09", n: 2 }, index: 0 },
      { op: "delete", index: 2 },
    ],
  });
  assert.deepEqual(s.wireRows(), [row("a", 9, 2), row("c", 3, 1)]);
  assert.deepEqual(
    s.rows(),
    s.wireRows().map((r) => ({ ...r, last: at(r.last as string) })),
  );
  assert.deepEqual(s.ids(), ['["a"]', '["c"]']);
});

test("custom parsers replace or add to the defaults; null or false turns them off", () => {
  const cols = [
    { name: "ts", type: "DateTime64(3)" },
    { name: "u", type: "UUID" },
    { name: "d", type: "Date" },
  ];
  const rows = [{ ts: "2026-10-01 00:00:00.000", u: "abc", d: "2026-10-01" }];
  const parse = (parsers: ConstructorParameters<typeof ResultStore>[0]) => {
    const s = new ResultStore(parsers);
    s.applySnapshot({ type: "snapshot", id: "q", columns: cols, rows });
    return s.rows()[0];
  };
  assert.deepEqual(parse({ parsers: { DateTime64: (v, t) => `${t.args[0]}:${v}`, UUID: (v) => String(v).toUpperCase() } }), {
    ts: "3:2026-10-01 00:00:00.000",
    u: "ABC",
    d: at("2026-10-01 00:00:00"),
  });
  assert.deepEqual(parse({ parsers: { Date: null } }), { ts: at("2026-10-01 00:00:00.000"), u: "abc", d: "2026-10-01" });
  assert.deepEqual(parse({ parsers: false }), rows[0]);
});

test("without parsed columns, rows are the wire rows themselves", () => {
  const s = new ResultStore<{ n: number }>();
  s.applySnapshot({ type: "snapshot", id: "a", columns, rows: [{ n: 1 }] });
  s.applyDiff({ type: "diff", id: "a", changes: [{ op: "insert", row: { n: 2 } }] });
  assert.equal(s.rows(), s.wireRows());
});

test("a value a parser can't read fails the diff and leaves the rows untouched", () => {
  const s = new ResultStore<{ id: string; ts: Date | null }>();
  s.applySnapshot({ type: "snapshot", id: "q", columns: TS, rows: [{ id: "a", ts: "2026-10-01 00:00:00.000" }] });
  assert.throws(
    () =>
      s.applyDiff({
        type: "diff",
        id: "q",
        changes: [
          { op: "insert", row: { id: "b", ts: "2026-10-02 00:00:00.000" } },
          { op: "insert", row: { id: "c", ts: "soon" } },
        ],
      }),
    /not a ClickHouse date or time: "soon"/,
  );
  assert.equal(s.size, 1);
  assert.equal(s.wireRows().length, 1);
});

test("cached rows are parsed by their saved columns, and again by a resume's", () => {
  const s = new ResultStore<{ id: string; ts: Date | null }>();
  s.applyCached({ rows: [{ id: "a", ts: "2026-10-01 00:00:00.000" }], columns: TS, at: "c1", savedAt: 1 });
  assert.ok(s.rows()[0].ts instanceof Date);
  // a cache without columns: strings until the server says what they are
  const t = new ResultStore<{ id: string; ts: Date | string | null }>();
  t.applyCached({ rows: [{ id: "a", ts: "2026-10-01 00:00:00.000" }], at: "c1", savedAt: 1 });
  assert.equal(t.rows()[0].ts, "2026-10-01 00:00:00.000");
  t.applyResume(TS);
  assert.ok(t.rows()[0].ts instanceof Date);
  assert.deepEqual(t.columns(), TS);
});
