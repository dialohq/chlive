import { test } from "node:test";
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

test("subscribes, applies messages, resubscribes after reconnect", async () => {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", minBackoffMs: 5, WebSocket: FakeSocket as any });
  const states: LiveState[] = [];
  const sub = client.subscribe("SELECT 1", { a: 1 }, (s) => states.push(s));
  assert.equal(states.at(-1)!.status, "loading");

  const ws1 = FakeSocket.all[0];
  ws1.open();
  assert.deepEqual(ws1.sent, [{ type: "subscribe", id: "s1", sql: "SELECT 1", params: { a: 1 } }]);

  ws1.deliver({ type: "snapshot", id: "s1", columns: [], keyColumns: [], rows: [{ __key: "[]", n: 1 }] });
  assert.equal(states.at(-1)!.status, "live");
  ws1.deliver({ type: "patch", id: "s1", upserts: [{ __key: "[]", n: 2 }], deletes: [] });
  assert.deepEqual(states.at(-1)!.rows, [{ __key: "[]", n: 2 }]);

  ws1.close();
  assert.equal(states.at(-1)!.status, "loading");
  await tick(20);
  const ws2 = FakeSocket.all[1];
  assert.ok(ws2, "reconnected");
  ws2.open();
  assert.deepEqual(ws2.sent, [{ type: "subscribe", id: "s1", sql: "SELECT 1", params: { a: 1 } }]);
  ws2.deliver({ type: "snapshot", id: "s1", columns: [], keyColumns: [], rows: [{ __key: "[]", n: 7 }] });
  assert.deepEqual(states.at(-1)!.rows, [{ __key: "[]", n: 7 }]);

  sub.unsubscribe();
  assert.deepEqual(ws2.sent.at(-1), { type: "unsubscribe", id: "s1" });
  client.close();
});

test("errors are reported per subscription", () => {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any });
  let last: LiveState | undefined;
  client.subscribe("SELECT now()", {}, (s) => (last = s));
  FakeSocket.all[0].open();
  FakeSocket.all[0].deliver({ type: "error", id: "s1", code: "UNSUPPORTED_QUERY", message: "now() is not allowed" });
  assert.equal(last!.status, "error");
  assert.equal(last!.errorCode, "UNSUPPORTED_QUERY");
  client.close();
});
