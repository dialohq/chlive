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
  assert.deepEqual(s.columns, columns);
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
    () => s.applyDiff({ type: "diff", id: "a", changes: [{ op: "insert", row: { n: 2 } }, { op: "update" } as any] }),
    /unsupported change "update"/,
  );
  assert.deepEqual(s.rows().map((r) => r.n), [1]);
});

test("ordered inserts land at their index, applied in sequence", () => {
  const s = new ResultStore<{ n: number }>();
  const order = [{ column: "n", direction: "desc" as const, nulls: "last" as const }];
  s.applySnapshot({ type: "snapshot", id: "a", columns, rows: [{ n: 9 }, { n: 5 }, { n: 1 }], order });
  assert.deepEqual(s.order, order);
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
  assert.equal(s.limit, 2);
  // window: append, then the first row goes
  s.applyDiff({ type: "diff", id: "a", changes: [{ op: "insert", row: { n: 3 } }, { op: "delete", index: 0 }] });
  assert.deepEqual(s.rows().map((r) => r.n), [2, 3]);
  // top-N: insert inside, then the row pushed past the end goes
  s.applyDiff({ type: "diff", id: "a", changes: [{ op: "insert", row: { n: 9 }, index: 0 }, { op: "delete", index: 2 }] });
  assert.deepEqual(s.rows().map((r) => r.n), [9, 2]);
  assert.throws(() => s.applyDiff({ type: "diff", id: "a", changes: [{ op: "delete", index: 2 }] }), /delete at index 2/);
  assert.deepEqual(s.rows().map((r) => r.n), [9, 2]);
});
