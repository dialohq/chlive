import { test } from "bun:test";
import assert from "node:assert/strict";

import { idle, project, subscriptionKey } from "../src/react.ts";

test("the subscription key follows the query, the params' values and enabled", () => {
  const sql = "select * from events where id = {id:UUID}";
  const a = subscriptionKey(sql, { id: "x", n: 1 }, true);
  assert.equal(subscriptionKey(sql, { n: 1, id: "x" }, true), a, "the same values in another order");
  assert.notEqual(subscriptionKey(sql, { id: "y", n: 1 }, true), a);
  assert.notEqual(subscriptionKey("select 1", { id: "x", n: 1 }, true), a);
  assert.equal(subscriptionKey(sql, undefined, true), subscriptionKey(sql, {}, true));
  assert.equal(subscriptionKey(sql, { id: "x" }, false), "", "disabled: one key whatever the query");
});

test("disabled, a hook keeps its last rows and says idle", () => {
  const rows = [{ n: 1 }];
  const last = { status: "live" as const, rows };
  const off = idle(last);
  assert.deepEqual(off, { status: "idle", rows });
  assert.equal(off.rows, rows, "the same rows, frozen");
  assert.equal(idle(off), off, "already idle: unchanged (no re-render)");
  assert.deepEqual(idle({ status: "loading" as const, rows: [] }), { status: "idle", rows: [] });
});

test("the hook's view stays the same object until its status, rows or error change", () => {
  const rows = [{ n: 1 }];
  const a = project({ status: "loading", rows: [] }, { status: "live", rows });
  assert.deepEqual(a, { status: "live", rows, error: undefined });
  // e.g. a progress message: new stats, same rows
  assert.equal(project(a, { status: "live", rows }), a);
  assert.notEqual(project(a, { status: "live", rows: [...rows] }), a);
  assert.notEqual(project(a, { status: "cached", rows }), a);
  assert.notEqual(project(a, { status: "error", rows, error: { code: "X", message: "x" } }), a);
});
