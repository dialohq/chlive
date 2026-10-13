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
  at,
  savedAt: 1,
});

test("save, load (in order, across chunks), append, cursor", () => {
  const s = new SqliteResultStore(db(), { chunkRows: 3 });
  assert.equal(s.load("q"), undefined);
  s.save("q", result(7));
  const r = s.load("q")!;
  assert.deepEqual(r.rows.map((x) => x.n), [0, 1, 2, 3, 4, 5, 6]);
  assert.equal(r.at, "c1");
  s.append("q", [{ n: 7 }, { n: 8 }], "c2");
  s.setCursor("q", "c3");
  const r2 = s.load("q")!;
  assert.deepEqual(r2.rows.map((x) => x.n), [0, 1, 2, 3, 4, 5, 6, 7, 8]);
  assert.equal(r2.at, "c3");
  // a save replaces everything
  s.save("q", result(2, "c4"));
  assert.deepEqual(s.load("q")!.rows.length, 2);
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

test("a database with an older layout starts empty", () => {
  const d = db();
  d.exec("CREATE TABLE results (key TEXT PRIMARY KEY, columns TEXT NOT NULL, cursor TEXT NOT NULL)");
  d.exec("INSERT INTO results VALUES ('q', '[]', 'c1')");
  const s = new SqliteResultStore(d);
  assert.equal(s.load("q"), undefined);
  s.save("q", result(2));
  assert.equal(s.load("q")!.rows.length, 2);
  // the same layout again: kept
  assert.equal(new SqliteResultStore(d).load("q")!.rows.length, 2);
});

test("columns are saved with the rows", () => {
  const s = new SqliteResultStore(db());
  const columns = [{ name: "ts", type: "DateTime64(3)" }];
  s.save("q", { rows: [{ ts: "2026-10-01 00:00:00.000" }], columns, at: "c1", savedAt: 1 });
  assert.deepEqual(s.load("q")!.columns, columns);
  assert.deepEqual(s.load("q")!.rows, [{ ts: "2026-10-01 00:00:00.000" }]);
  s.save("old", result(1));
  assert.equal(s.load("old")!.columns, undefined);
});

test("recent: the most recently used results that fit, without marking them as used", async () => {
  const s = new SqliteResultStore(db());
  s.save("a", result(5));
  await Bun.sleep(2);
  s.save("b", result(3));
  await Bun.sleep(2);
  s.save("c", result(4));
  await Bun.sleep(2);
  s.load("a"); // a is now the most recently used
  const keys = (n: number) => s.recent(n).map((r) => r.key);
  assert.deepEqual(keys(100), ["a", "c", "b"]);
  // c doesn't fit after a; b does
  assert.deepEqual(keys(8), ["a", "b"]);
  assert.deepEqual(keys(2), []);
  assert.deepEqual(s.recent(5)[0].result.rows.length, 5);
  // reading them didn't change the order
  assert.deepEqual(keys(100), ["a", "c", "b"]);
});
