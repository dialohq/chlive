import { test } from "bun:test";
import assert from "node:assert/strict";

import { LiveClient, type LiveState } from "../src/index.ts";

/** Minimal in-memory WebSocket double. */
class FakeSocket {
  static all: FakeSocket[] = [];
  readyState = 0;
  sent: any[] = [];
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  url: string;
  constructor(url: string) {
    this.url = url;
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

const tick = (ms: number) => new Promise((r) => setTimeout(r, ms));

const SQL = "select user_id from events where latency_ms > 100";
const COLUMNS = [{ name: "user_id", type: "String" }];

test("subscribes, applies messages, resubscribes after reconnect", async () => {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", minBackoffMs: 5, WebSocket: FakeSocket as any });
  const states: LiveState[] = [];
  const sub = client.subscribe(SQL, (s) => states.push(s));
  assert.equal(states.at(-1)!.status, "loading");

  const ws1 = FakeSocket.all[0];
  ws1.open();
  assert.deepEqual(ws1.sent, [{ type: "subscribe", id: "s1", sql: SQL }]);

  ws1.deliver({ type: "snapshot", id: "s1", columns: COLUMNS, rows: [{ user_id: "h1" }] });
  assert.equal(states.at(-1)!.status, "live");
  assert.deepEqual(states.at(-1)!.columns, COLUMNS);
  ws1.deliver({ type: "diff", id: "s1", changes: [{ op: "insert", row: { user_id: "e1" } }, { op: "insert", row: { user_id: "e2" } }] });
  assert.deepEqual(states.at(-1)!.rows, [{ user_id: "h1" }, { user_id: "e1" }, { user_id: "e2" }]);
  // each change gives React a new array
  assert.notEqual(states.at(-1)!.rows, states.at(-2)!.rows);
  // messages for other ids are ignored
  ws1.deliver({ type: "diff", id: "s9", changes: [{ op: "insert", row: { user_id: "x" } }] });
  assert.equal(states.at(-1)!.rows.length, 3);

  ws1.close();
  assert.equal(states.at(-1)!.status, "loading");
  await tick(20);
  const ws2 = FakeSocket.all[1];
  assert.ok(ws2, "reconnected");
  ws2.open();
  assert.deepEqual(ws2.sent, [{ type: "subscribe", id: "s1", sql: SQL }]);
  // the new snapshot is the whole result: nothing is duplicated
  ws2.deliver({ type: "snapshot", id: "s1", columns: COLUMNS, rows: [{ user_id: "h1" }, { user_id: "e1" }, { user_id: "e2" }, { user_id: "e3" }] });
  assert.equal(states.at(-1)!.rows.length, 4);

  sub.unsubscribe();
  assert.deepEqual(ws2.sent.at(-1), { type: "unsubscribe", id: "s1" });
  client.close();
});

test("errors are reported per subscription", () => {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any });
  let a: LiveState | undefined;
  let b: LiveState | undefined;
  client.subscribe("select * form events", (s) => (a = s));
  client.subscribe(SQL, (s) => (b = s));
  FakeSocket.all[0].open();
  FakeSocket.all[0].deliver({
    type: "error",
    id: "s1",
    code: "SYNTAX_ERROR",
    message: "syntax error: expected FROM, found identifier `form` at 1:10",
  });
  FakeSocket.all[0].deliver({ type: "snapshot", id: "s2", columns: COLUMNS, rows: [] });
  assert.equal(a!.status, "error");
  assert.equal(a!.errorCode, "SYNTAX_ERROR");
  assert.match(a!.error!, /at 1:10$/);
  assert.equal(b!.status, "live");
  client.close();
});

test("an unknown change op fails the subscription instead of being skipped", () => {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any });
  let last: LiveState | undefined;
  client.subscribe(SQL, (s) => (last = s));
  FakeSocket.all[0].open();
  FakeSocket.all[0].deliver({ type: "snapshot", id: "s1", columns: COLUMNS, rows: [{ user_id: "h1" }] });
  FakeSocket.all[0].deliver({ type: "diff", id: "s1", changes: [{ op: "upsert", row: { user_id: "x" } }] });
  assert.equal(last!.status, "error");
  assert.equal(last!.errorCode, "CLIENT_ERROR");
  assert.deepEqual(last!.rows, [{ user_id: "h1" }]);
  client.close();
});
