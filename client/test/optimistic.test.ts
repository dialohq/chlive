import { test } from "bun:test";
import assert from "node:assert/strict";

import { InsertError, LiveClient, type LiveState } from "../src/index.ts";
import { loadWasm } from "../src/optimistic.ts";

// chlive-core as WebAssembly: built by `wasm` (the dev shell's script) into client/wasm/
await loadWasm().catch((e) => {
  throw new Error(`run \`wasm\` first (client/wasm/ is missing or broken): ${e}`);
});

class FakeSocket {
  static all: FakeSocket[] = [];
  readyState = 0;
  sent: any[] = [];
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  constructor(_url: string) {
    FakeSocket.all.push(this);
  }
  send(data: string) {
    this.sent.push(JSON.parse(data));
  }
  close() {
    this.readyState = 3;
    this.onclose?.();
  }
  open() {
    this.readyState = 1;
    this.onopen?.();
  }
  deliver(msg: unknown) {
    this.onmessage?.({ data: JSON.stringify(msg) });
  }
}

const tick = (ms = 0) => new Promise((r) => setTimeout(r, ms));
/** A cursor: complete to `offset` in partition 0. */
const at = (offset: number) => JSON.stringify({ v: 2, fp: "0", o: { "0": offset } });

const SQL = "select user_id, latency_ms from events order by latency_ms desc limit 3";
const COLUMNS = [
  { name: "user_id", type: "String" },
  { name: "latency_ms", type: "UInt32" },
];
/** The server's plan for SQL (as `Plan::client` makes it), with an optional WHERE. */
function plan(opts: { filter?: unknown; userColumn?: string } = {}) {
  const col = (id: number, name: string, ty: string) => ({ id, name, source: { json: name }, ty });
  return {
    v: 1,
    table: "events",
    ...(opts.userColumn ? { user_column: opts.userColumn } : {}),
    stream: {
      topics: [],
      columns: [col(0, "user_id", "String"), col(1, "latency_ms", "UInt32")],
      width: 2,
      output: [
        ["user_id", 0],
        ["latency_ms", 1],
      ],
      filter: opts.filter ?? "all",
      order: {
        keys: [{ column: 1, desc: true, nulls_first: false }],
        columns: [{ id: 1, name: "latency_ms", history_field: "latency_ms", ty: "UInt32" }],
        hidden: [],
        text: "latency_ms DESC NULLS LAST",
      },
      limit: 3,
      offset: null,
      keyed: null,
      windows: [],
      history_fields: [],
    },
  };
}
const SNAPSHOT = {
  type: "snapshot",
  id: "s1",
  columns: COLUMNS,
  rows: [
    { user_id: "a", latency_ms: 900 },
    { user_id: "b", latency_ms: 500 },
    { user_id: "c", latency_ms: 100 },
  ],
  order: [{ column: "latency_ms", direction: "desc", nulls: "last" }],
  limit: 3,
  at: at(10),
};

/** An optimistic client subscribed to SQL, with its plan (or reason) and the snapshot. */
async function subscribed(planMsg: Record<string, unknown>, user?: string) {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any, optimistic: true, user });
  const states: LiveState[] = [];
  client.subscribe(SQL, (s) => states.push(s));
  const ws = FakeSocket.all[0];
  ws.open();
  ws.deliver({ type: "plan", id: "s1", ...planMsg });
  ws.deliver(SNAPSHOT);
  await tick();
  return { client, ws, states, last: () => states.at(-1)! };
}

const users = (s: LiveState) => s.rows.map((r) => `${r.user_id}:${r.latency_ms}`);

test("an optimistic subscription asks for its plan; another doesn't", () => {
  FakeSocket.all = [];
  new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any, optimistic: true }).subscribe(SQL, () => {});
  FakeSocket.all[0].open();
  assert.deepEqual(FakeSocket.all[0].sent, [{ type: "subscribe", id: "s1", sql: SQL, optimistic: true }]);
  FakeSocket.all = [];
  new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any }).subscribe(SQL, () => {});
  FakeSocket.all[0].open();
  assert.deepEqual(FakeSocket.all[0].sent, [{ type: "subscribe", id: "s1", sql: SQL }]);
});

test("an insert shows at once where the server will put it, until the server's rows have it", async () => {
  const { client, ws, last } = await subscribed({ plan: plan() });
  assert.equal(last().stats.optimistic?.predicted, true);

  const answer = client.insert("events", { user_id: "me", latency_ms: 600 });
  // ranked second; the LIMIT pushes c out
  assert.deepEqual(users(last()), ["a:900", "me:600", "b:500"]);
  assert.deepEqual([...last().pending!], [last().rows[1]]);
  assert.equal(last().stats.optimistic?.pending, 1);

  const insert = ws.sent.at(-1);
  assert.equal(insert.type, "insert");
  ws.deliver({ type: "ack", id: insert.id, offsets: [[0, 11]] });
  assert.deepEqual(await answer, { offsets: [[0, 11]] });
  // acknowledged, but the result isn't there yet: still pending
  assert.deepEqual(users(last()), ["a:900", "me:600", "b:500"]);
  assert.equal(last().pending?.size, 1);

  // an unrelated event first: placed again among the new rows
  ws.deliver({ type: "diff", id: "s1", changes: [{ op: "insert", row: { user_id: "z", latency_ms: 950 }, index: 0 }, { op: "delete", index: 3 }], at: at(10) });
  assert.deepEqual(users(last()), ["z:950", "a:900", "me:600"]);

  // the server's diff with it, and its cursor past the insert: no longer pending
  ws.deliver({
    type: "diff",
    id: "s1",
    changes: [
      { op: "insert", row: { user_id: "me", latency_ms: 600 }, index: 2 },
      { op: "delete", index: 3 },
    ],
    at: at(11),
  });
  assert.deepEqual(users(last()), ["z:950", "a:900", "me:600"]);
  assert.equal(last().pending, undefined);
  assert.equal(last().stats.optimistic?.settled, 1);
  client.close();
});

test("a failed insert is taken back", async () => {
  const { client, ws, last } = await subscribed({ plan: plan() });
  const answer = client.insert("events", [
    { user_id: "x", latency_ms: 2000 },
    { user_id: "y", latency_ms: 1000 },
  ]);
  assert.deepEqual(users(last()), ["x:2000", "y:1000", "a:900"]);
  ws.deliver({ type: "error", id: ws.sent.at(-1).id, code: "PERMISSION_DENIED", message: "no" });
  await assert.rejects(answer, (e) => e instanceof InsertError && e.code === "PERMISSION_DENIED");
  assert.deepEqual(users(last()), ["a:900", "b:500", "c:100"]);
  assert.equal(last().pending, undefined);
  assert.equal(last().stats.optimistic?.rolledBack, 2);

  // one that doesn't fit the table (as the server checks inserts) isn't shown at all
  void client.insert("events", { user_id: "w", latency_ms: "fast" }).catch(() => {});
  assert.deepEqual(users(last()), ["a:900", "b:500", "c:100"]);
  // nor one that ranks past the LIMIT
  void client.insert("events", { user_id: "v", latency_ms: 1 }).catch(() => {});
  assert.deepEqual(users(last()), ["a:900", "b:500", "c:100"]);
  // nor with { optimistic: false }
  void client.insert("events", { user_id: "u", latency_ms: 3000 }, { optimistic: false }).catch(() => {});
  assert.deepEqual(users(last()), ["a:900", "b:500", "c:100"]);
  client.close();
});

test("rows are stamped with the user, and filtered as the server filters them", async () => {
  const filter = {
    where: {
      expr: { compare: { op: "=", left: { column: 0 }, right: { literal: { s: "alice" } } } },
      text: "user_id = 'alice'",
    },
  };
  const { client, last } = await subscribed({ plan: plan({ filter, userColumn: "user_id" }) }, "alice");
  void client.insert("events", { latency_ms: 700 }).catch(() => {});
  assert.deepEqual(users(last()), ["a:900", "alice:700", "b:500"]);
  client.close();

  // another user's rows don't match the row filter
  const bob = await subscribed({ plan: plan({ filter, userColumn: "user_id" }) }, "bob");
  void bob.client.insert("events", { latency_ms: 700 }).catch(() => {});
  assert.deepEqual(users(bob.last()), ["a:900", "b:500", "c:100"]);
  bob.client.close();
});

test("a query without a plan shows no predictions, and says why", async () => {
  const { client, last } = await subscribed({ reason: "OFFSET isn't predicted" });
  void client.insert("events", { user_id: "me", latency_ms: 600 }).catch(() => {});
  assert.deepEqual(users(last()), ["a:900", "b:500", "c:100"]);
  assert.deepEqual(last().stats.optimistic, {
    predicted: false,
    reason: "OFFSET isn't predicted",
    plan: undefined,
    pending: 0,
    settled: 0,
    rolledBack: 0,
  });
  // other tables' inserts don't touch it either
  client.close();
});
