import { Database } from "bun:sqlite";
import { test } from "bun:test";
import assert from "node:assert/strict";

import { type Db, SqliteResultStore } from "../src/cache/store.ts";

// the same store the browser runs on sqlite-wasm, here on bun:sqlite
function db(): Db {
  const d = new Database(":memory:");
  return {
    exec: (sql, params = []) => void d.query(sql).run(...(params as never[])),
    all: (sql, params = []) => d.query(sql).all(...(params as never[])) as Record<string, unknown>[],
    transaction: (fn) => d.transaction(fn)(),
  };
}

const result = (n: number, at = "c1") => ({
  rows: Array.from({ length: n }, (_, i) => ({ n: i })),
  columns: [{ name: "n", type: "UInt32" }],
  at,
  savedAt: 1,
});

test("save, load (in order, across chunks), append, cursor", () => {
  const s = new SqliteResultStore(db(), { chunkRows: 3 });
  assert.equal(s.load("q"), undefined);
  s.save("q", { ...result(7), order: [{ column: "n", direction: "asc", nulls: "last" }], limit: 50 });
  const r = s.load("q")!;
  assert.deepEqual(r.rows.map((x) => x.n), [0, 1, 2, 3, 4, 5, 6]);
  assert.deepEqual(r.order, [{ column: "n", direction: "asc", nulls: "last" }]);
  assert.equal(r.limit, 50);
  assert.equal(r.at, "c1");
  s.append("q", [{ n: 7 }, { n: 8 }], "c2");
  s.setCursor("q", "c3");
  const r2 = s.load("q")!;
  assert.deepEqual(r2.rows.map((x) => x.n), [0, 1, 2, 3, 4, 5, 6, 7, 8]);
  assert.equal(r2.at, "c3");
  assert.equal(r2.limit, 50);
  // a save replaces everything
  s.save("q", result(2, "c4"));
  assert.deepEqual(s.load("q")!.rows.length, 2);
  assert.equal(s.load("q")!.limit, undefined);
  // appending to nothing does nothing
  s.append("nope", [{ n: 1 }], "c");
  assert.equal(s.load("nope"), undefined);
  assert.deepEqual(s.stats(), { queries: 1, rows: 2, bytes: JSON.stringify(result(2).rows).length, budgetBytes: s.budgetBytes });
});

test("over the budget, the least recently used results go, never the one just written", async () => {
  const one = JSON.stringify(result(100).rows).length;
  const s = new SqliteResultStore(db(), { budgetBytes: one * 2 + 10 });
  const tick = () => new Promise((r) => setTimeout(r, 2));
  s.save("a", result(100));
  await tick();
  s.save("b", result(100));
  await tick();
  s.load("a"); // a is now more recent than b
  await tick();
  s.save("c", result(100)); // over budget: b goes
  assert.deepEqual(
    ["a", "b", "c"].map((k) => s.load(k) !== undefined),
    [true, false, true],
  );
  // one result bigger than the budget is kept (it's the one just written)
  s.save("huge", result(1000));
  assert.ok(s.load("huge"));
  assert.equal(s.stats().queries, 1);
  s.clear();
  assert.equal(s.stats().queries, 0);
});
