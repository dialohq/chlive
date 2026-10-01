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
    () => s.applyDiff({ type: "diff", id: "a", changes: [{ op: "insert", row: { n: 2 } }, { op: "delete" } as any] }),
    /unsupported change "delete"/,
  );
  assert.deepEqual(s.rows().map((r) => r.n), [1]);
});
