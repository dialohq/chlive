import { test } from "node:test";
import assert from "node:assert/strict";

import { ResultStore } from "../src/state.ts";

const row = (c: string, n: number) => ({ __key: JSON.stringify([c]), country: c, n });

test("snapshot then patches reconstructs the latest result in order", () => {
  const s = new ResultStore();
  s.applySnapshot({
    type: "snapshot", id: "a", columns: [], keyColumns: ["country"],
    rows: [row("PL", 3), row("DE", 2), row("FR", 1)],
    order: ['["PL"]', '["DE"]', '["FR"]'],
  });
  s.applyPatch({ type: "patch", id: "a", upserts: [row("DE", 5), row("US", 4)], deletes: ['["FR"]'], order: ['["DE"]', '["US"]', '["PL"]'] });
  assert.deepEqual(s.rows(), [row("DE", 5), row("US", 4), row("PL", 3)]);
  s.applyPatch({ type: "patch", id: "a", upserts: [row("PL", 9)], deletes: [], order: ['["PL"]', '["DE"]', '["US"]'] });
  assert.deepEqual(s.rows().map((r) => r.n), [9, 5, 4]);
});

test("without order rows keep insertion order and a snapshot replaces state", () => {
  const s = new ResultStore();
  s.applySnapshot({ type: "snapshot", id: "a", columns: [], keyColumns: [], rows: [row("A", 1)] });
  s.applyPatch({ type: "patch", id: "a", upserts: [row("B", 1), row("A", 2)], deletes: [] });
  assert.deepEqual(s.rows(), [row("A", 2), row("B", 1)]);
  s.applySnapshot({ type: "snapshot", id: "a", columns: [], keyColumns: [], rows: [row("C", 1)] });
  assert.deepEqual(s.rows(), [row("C", 1)]);
});
