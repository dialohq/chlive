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
