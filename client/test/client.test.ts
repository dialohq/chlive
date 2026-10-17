import { test } from "bun:test";
import assert from "node:assert/strict";

import { type CachedResult, cacheKey, LiveClient, type LiveState, opfsAvailable, type ResultCache } from "../src/index.ts";

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
  const client = new LiveClient({ url: "ws://x/live", minBackoffMs: 5, keepAliveMs: 10, WebSocket: FakeSocket as any });
  const states: LiveState[] = [];
  const sub = client.subscribe(SQL, (s) => states.push(s));
  assert.equal(states.at(-1)!.status, "loading");

  const ws1 = FakeSocket.all[0];
  ws1.open();
  assert.deepEqual(ws1.sent, [{ type: "subscribe", id: "s1", sql: SQL }]);

  ws1.deliver({ type: "snapshot", id: "s1", columns: COLUMNS, rows: [{ user_id: "h1" }] });
  assert.equal(states.at(-1)!.status, "live");
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

  // the server subscription outlives the last subscriber by keepAliveMs
  sub.unsubscribe();
  assert.equal(ws2.sent.at(-1).type, "subscribe");
  await tick(20);
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
  assert.equal(a!.error?.code, "SYNTAX_ERROR");
  assert.match(a!.error!.message, /at 1:10$/);
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
  assert.equal(last!.error?.code, "CLIENT_ERROR");
  assert.deepEqual(last!.rows, [{ user_id: "h1" }]);
  client.close();
});

test("listeners never see more rows than the LIMIT", () => {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any });
  const sizes: number[] = [];
  client.subscribe("select user_id from events limit 2", (s) => sizes.push(s.rows.length));
  FakeSocket.all[0].open();
  FakeSocket.all[0].deliver({ type: "snapshot", id: "s1", columns: COLUMNS, rows: [{ user_id: "a" }, { user_id: "b" }], limit: 2 });
  for (const u of ["c", "d", "e"]) {
    FakeSocket.all[0].deliver({ type: "diff", id: "s1", changes: [{ op: "insert", row: { user_id: u } }, { op: "delete", index: 0 }] });
  }
  assert.deepEqual(sizes, [0, 2, 2, 2, 2]);
  client.close();
});

test("stats count what arrived, per subscription", async () => {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", minBackoffMs: 5, WebSocket: FakeSocket as any });
  let last: LiveState | undefined;
  client.subscribe(SQL, (s) => (last = s));
  const ws = FakeSocket.all[0];
  ws.open();
  ws.deliver({ type: "snapshot", id: "s1", columns: COLUMNS, rows: [{ user_id: "a" }], limit: 2 });
  const diff = { type: "diff", id: "s1", changes: [{ op: "insert", row: { user_id: "b" } }] };
  ws.deliver(diff);
  ws.deliver({ type: "diff", id: "s1", changes: [{ op: "insert", row: { user_id: "c" } }, { op: "delete", index: 0 }] });
  const s = last!.stats;
  assert.equal(s.id, "s1");
  assert.deepEqual([s.subscribes, s.snapshots, s.diffs, s.inserts, s.deletes], [1, 1, 2, 2, 1]);
  assert.equal(s.lastMessage?.type, "diff");
  assert.ok(s.bytes > JSON.stringify(diff).length);
  assert.ok(s.lastApplyMs! >= 0);
  // a reconnect resubscribes, and counts it
  ws.close();
  await tick(20);
  FakeSocket.all[1].open();
  FakeSocket.all[1].deliver({ type: "snapshot", id: "s1", columns: COLUMNS, rows: [] });
  assert.deepEqual([last!.stats.subscribes, last!.stats.snapshots], [2, 2]);
  client.close();
});

/** A ResultCache in memory, recording calls. */
function memoryCache(initial: Record<string, CachedResult> = {}) {
  const data = new Map(Object.entries(initial));
  const calls: string[] = [];
  const cache: ResultCache = {
    load: async (k) => (calls.push(`load ${k}`), data.get(k)),
    save: async (k, r) => void (calls.push(`save ${r.rows.length}@${r.at}`), data.set(k, r)),
    append: async (k, rows, at) => {
      calls.push(`append ${rows.length}@${at}`);
      const r = data.get(k)!;
      data.set(k, { ...r, rows: [...r.rows, ...rows], at });
    },
    setCursor: async (k, at) => {
      calls.push(`cursor ${at}`);
      data.set(k, { ...data.get(k)!, at });
    },
    remove: async (k) => void data.delete(k),
    close: () => void calls.push("close"),
    reopen: () => void calls.push("reopen"),
  };
  return { cache, data, calls };
}

test("a cached result shows first, resumes from its cursor, and is kept up to date", async () => {
  FakeSocket.all = [];
  const { cache, data, calls } = memoryCache({
    [SQL]: { rows: [{ user_id: "a" }], at: "c1", savedAt: 42 },
  });
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any, cache, saveDelayMs: 5 });
  const states: LiveState[] = [];
  client.subscribe(SQL, (s) => states.push(s));
  const ws = FakeSocket.all[0];
  ws.open();
  assert.deepEqual(ws.sent, [], "not subscribed before the cache is read");
  await tick(1);
  assert.equal(states.at(-1)!.status, "cached");
  assert.equal(states.at(-1)!.cachedAt, 42);
  assert.deepEqual(states.at(-1)!.rows, [{ user_id: "a" }]);
  assert.deepEqual(ws.sent, [{ type: "subscribe", id: "s1", sql: SQL, since: "c1" }]);

  // resumed: the rows stay; the catch-up and later diffs apply on top, appends go straight in
  ws.deliver({ type: "resume", id: "s1", columns: COLUMNS });
  assert.equal(states.at(-1)!.status, "live");
  assert.deepEqual(states.at(-1)!.rows, [{ user_id: "a" }]);
  ws.deliver({ type: "diff", id: "s1", changes: [{ op: "insert", row: { user_id: "b" } }], at: "c2" });
  ws.deliver({ type: "progress", id: "s1", at: "c3" });
  await tick(10);
  assert.deepEqual(states.at(-1)!.rows, [{ user_id: "a" }, { user_id: "b" }]);
  const cs = states.at(-1)!.stats.cache!;
  assert.deepEqual([cs.hit, cs.loadedRows, cs.resumed, cs.catchUp], [true, 1, true, 1]);
  assert.deepEqual(calls, [`load ${SQL}`, "append 1@c2", "cursor c3"]);
  assert.deepEqual(data.get(SQL)!.rows.length, 2);

  // a reconnect resumes from where it is now
  ws.close();
  await tick(600);
  FakeSocket.all[1].open();
  assert.deepEqual(FakeSocket.all[1].sent, [{ type: "subscribe", id: "s1", sql: SQL, since: "c3" }]);
  client.close();
});

test("a snapshot instead of a resume replaces the cached rows, and is saved in full", async () => {
  FakeSocket.all = [];
  const { cache, data, calls } = memoryCache({
    [SQL]: { rows: [{ user_id: "old" }], at: "stale", savedAt: 1 },
  });
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any, cache, saveDelayMs: 5 });
  let last: LiveState | undefined;
  client.subscribe(SQL, (s) => (last = s));
  FakeSocket.all[0].open();
  await tick(1);
  FakeSocket.all[0].deliver({ type: "snapshot", id: "s1", columns: COLUMNS, rows: [{ user_id: "x" }], at: "c9" });
  assert.deepEqual(last!.rows, [{ user_id: "x" }]);
  assert.equal(last!.stats.cache!.resumed, false);
  // ordered changes mean a full save (batched)
  FakeSocket.all[0].deliver({ type: "diff", id: "s1", changes: [{ op: "insert", row: { user_id: "w" }, index: 0 }], at: "c10" });
  await tick(20);
  assert.deepEqual(calls, [`load ${SQL}`, "save 2@c10"]);
  assert.deepEqual(data.get(SQL)!.rows, [{ user_id: "w" }, { user_id: "x" }]);
  client.close();
});

test("inserts with ids are saved in full, keeping their ids", async () => {
  FakeSocket.all = [];
  const { cache, data, calls } = memoryCache();
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any, cache, saveDelayMs: 5 });
  client.subscribe(SQL, () => {});
  FakeSocket.all[0].open();
  await tick(1);
  const ws = FakeSocket.all[0];
  ws.deliver({ type: "snapshot", id: "s1", columns: COLUMNS, rows: [{ user_id: "a" }], ids: ['["a"]'], at: "c1" });
  await tick(20);
  ws.deliver({ type: "diff", id: "s1", changes: [{ op: "insert", row: { user_id: "b" }, id: '["b"]' }], at: "c2" });
  await tick(20);
  assert.deepEqual(calls, [`load ${SQL}`, "save 1@c1", "save 2@c2"]);
  assert.deepEqual(data.get(SQL)!.ids, ['["a"]', '["b"]']);
  client.close();
});

test("a cache miss subscribes without since, then saves the snapshot", async () => {
  FakeSocket.all = [];
  const { cache, calls } = memoryCache();
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any, cache, saveDelayMs: 5 });
  let last: LiveState | undefined;
  client.subscribe(SQL, (s) => (last = s));
  FakeSocket.all[0].open();
  await tick(1);
  assert.deepEqual(FakeSocket.all[0].sent, [{ type: "subscribe", id: "s1", sql: SQL }]);
  assert.equal(last!.status, "loading");
  FakeSocket.all[0].deliver({ type: "snapshot", id: "s1", columns: COLUMNS, rows: [{ user_id: "a" }], at: "c1" });
  await tick(20);
  assert.equal(last!.stats.cache!.hit, false);
  assert.deepEqual(calls, [`load ${SQL}`, "save 1@c1"]);
  client.close();
});

test("the client saves results when the page is hidden, and manages the cache's lifecycle", async () => {
  FakeSocket.all = [];
  const { cache, data, calls } = memoryCache();
  // a long save delay: only the page being hidden saves it
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any, cache, saveDelayMs: 60_000 });
  client.subscribe(SQL, () => {});
  FakeSocket.all[0].open();
  await tick(1);
  FakeSocket.all[0].deliver({ type: "snapshot", id: "s1", columns: COLUMNS, rows: [{ user_id: "a" }], at: "c1" });
  dispatchEvent(new Event("pagehide"));
  await tick(5);
  assert.deepEqual(calls, [`load ${SQL}`, "save 1@c1", "close"]);
  assert.deepEqual(data.get(SQL)!.rows, [{ user_id: "a" }]);
  // restored from the back/forward cache: opened again; a normal pageshow does nothing
  dispatchEvent(Object.assign(new Event("pageshow"), { persisted: true }));
  dispatchEvent(Object.assign(new Event("pageshow"), { persisted: false }));
  assert.deepEqual(calls.slice(3), ["reopen"]);
  // closing the client closes the cache and stops listening to the page
  client.close();
  await tick(1);
  dispatchEvent(new Event("pagehide"));
  await tick(5);
  assert.deepEqual(calls.slice(4), ["close"]);
});

// --- value parsers ---------------------------------------------------------------------------

const TS_SQL = "select user_id, ts from events order by ts desc";
const TS_COLUMNS = [
  { name: "user_id", type: "String" },
  { name: "ts", type: "DateTime64(3)" },
];

test("date columns reach listeners as Dates; the message log keeps the wire strings", async () => {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any });
  let last: LiveState | undefined;
  client.subscribe(TS_SQL, (s) => (last = s));
  FakeSocket.all[0].open();
  FakeSocket.all[0].deliver({
    type: "snapshot",
    id: "s1",
    columns: TS_COLUMNS,
    rows: [{ user_id: "a", ts: "2026-10-01 12:00:00.250" }],
    at: "c1",
  });
  assert.deepEqual(last!.rows, [{ user_id: "a", ts: new Date("2026-10-01T12:00:00.250Z") }]);
  assert.deepEqual((last!.stats.lastMessage as any).rows, [{ user_id: "a", ts: "2026-10-01 12:00:00.250" }]);
  client.close();
});

test("the cache gets wire rows and columns, and shows cached rows parsed", async () => {
  FakeSocket.all = [];
  const { cache, data } = memoryCache({
    [TS_SQL]: { rows: [{ user_id: "a", ts: "2026-10-01 00:00:00.000" }], columns: TS_COLUMNS, at: "c1", savedAt: 1 },
  });
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any, cache, saveDelayMs: 5 });
  const states: LiveState[] = [];
  client.subscribe(TS_SQL, (s) => states.push(s));
  const ws = FakeSocket.all[0];
  ws.open();
  await tick(1);
  assert.equal(states.at(-1)!.status, "cached");
  assert.ok(states.at(-1)!.rows[0].ts instanceof Date);
  ws.deliver({ type: "resume", id: "s1", columns: TS_COLUMNS });
  // appended as they arrived
  ws.deliver({ type: "diff", id: "s1", changes: [{ op: "insert", row: { user_id: "b", ts: "2026-10-02 00:00:00.000" } }], at: "c2" });
  await tick(10);
  assert.deepEqual(data.get(TS_SQL)!.rows[1], { user_id: "b", ts: "2026-10-02 00:00:00.000" });
  // a full save too
  ws.deliver({
    type: "diff",
    id: "s1",
    changes: [{ op: "insert", row: { user_id: "c", ts: "2026-10-03 00:00:00.000" }, index: 0 }],
    at: "c3",
  });
  await tick(20);
  assert.deepEqual(data.get(TS_SQL)!.rows[0], { user_id: "c", ts: "2026-10-03 00:00:00.000" });
  assert.deepEqual(data.get(TS_SQL)!.columns, TS_COLUMNS);
  assert.ok(states.at(-1)!.rows.every((r) => r.ts instanceof Date));
  client.close();
});

test("custom parsers apply, and one that throws fails the subscription", () => {
  FakeSocket.all = [];
  const client = new LiveClient({
    url: "ws://x/live",
    WebSocket: FakeSocket as any,
    parsers: {
      DateTime64: (v) => {
        if (v === "bad") throw new Error("cannot read bad");
        return `parsed ${v}`;
      },
    },
  });
  let last: LiveState | undefined;
  client.subscribe(TS_SQL, (s) => (last = s));
  FakeSocket.all[0].open();
  FakeSocket.all[0].deliver({ type: "snapshot", id: "s1", columns: TS_COLUMNS, rows: [{ user_id: "a", ts: "t" }], at: "c1" });
  assert.equal(last!.rows[0].ts, "parsed t");
  FakeSocket.all[0].deliver({ type: "diff", id: "s1", changes: [{ op: "insert", row: { user_id: "b", ts: "bad" } }], at: "c2" });
  assert.equal(last!.status, "error");
  assert.equal(last!.error!.code, "CLIENT_ERROR");
  assert.match(last!.error!.message, /cannot read bad/);
  assert.equal(last!.rows.length, 1);
  client.close();
});

// --- query parameters -------------------------------------------------------------------------

test("params are sent with every (re)subscribe, Dates and bigints serialized", async () => {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", minBackoffMs: 5, WebSocket: FakeSocket as any });
  const sql = "select * from events where ts > {t:DateTime64(3)} and n = {n:UInt64} and s = {s:String}";
  client.subscribe(sql, () => {}, { params: { t: new Date("2026-10-01T12:00:00.250Z"), n: 2n ** 63n, s: "x" } });
  const params = { t: "2026-10-01 12:00:00.250", n: "9223372036854775808", s: "x" };
  FakeSocket.all[0].open();
  assert.deepEqual(FakeSocket.all[0].sent, [{ type: "subscribe", id: "s1", sql, params }]);
  FakeSocket.all[0].deliver({ type: "snapshot", id: "s1", columns: COLUMNS, rows: [], at: "c1" });
  FakeSocket.all[0].close();
  await tick(20);
  FakeSocket.all[1].open();
  assert.deepEqual(FakeSocket.all[1].sent, [{ type: "subscribe", id: "s1", sql, params, since: "c1" }]);
  client.close();
});

test("results are cached per params; without params, under the query alone", async () => {
  FakeSocket.all = [];
  const { cache, calls } = memoryCache();
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any, cache, saveDelayMs: 1 });
  const sql = "select user_id from events where user_id = {u:String}";
  client.subscribe(sql, () => {}, { params: { u: "a" } });
  client.subscribe(sql, () => {}, { params: { u: "b" } });
  client.subscribe(SQL, () => {});
  client.subscribe(sql, () => {}, { params: {} });
  await tick(5);
  assert.deepEqual(calls, [`load ${sql}\u0000{"u":"a"}`, `load ${sql}\u0000{"u":"b"}`, `load ${SQL}`, `load ${sql}`]);
  client.close();
});

test("the built-in cache is on by default only where OPFS and workers are", () => {
  // Bun, like Node and SSR, has no OPFS: the cache stays off without a word
  assert.equal(opfsAvailable(), false);
  const nav = globalThis.navigator as { storage?: unknown };
  const had = Object.getOwnPropertyDescriptor(nav, "storage");
  Object.defineProperty(nav, "storage", { value: { getDirectory: async () => ({}) }, configurable: true });
  try {
    assert.equal(opfsAvailable(), typeof Worker === "function");
  } finally {
    if (had) Object.defineProperty(nav, "storage", had);
    else delete nav.storage;
  }
  assert.equal(opfsAvailable(), false);
});

// --- results shared in memory ----------------------------------------------------------------

const snapshot = (id: string, users: string[], at = "c1") => ({
  type: "snapshot",
  id,
  columns: COLUMNS,
  rows: users.map((user_id) => ({ user_id })),
  at,
});

test("subscribers to the same query and params share one server subscription and its rows", () => {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any });
  const a: LiveState[] = [];
  client.subscribe(SQL, (s) => a.push(s));
  const ws = FakeSocket.all[0];
  ws.open();
  ws.deliver(snapshot("s1", ["h1"]));

  const b: LiveState[] = [];
  client.subscribe(SQL, (s) => b.push(s));
  assert.equal(b.length, 1, "called synchronously");
  assert.equal(b[0].status, "live");
  assert.equal(b[0].rows, a.at(-1)!.rows);
  assert.equal(client.peek(SQL), a.at(-1));
  assert.deepEqual(
    ws.sent.map((m) => m.id),
    ["s1"],
    "one server subscription",
  );

  // other params: another result
  const c: LiveState[] = [];
  client.subscribe(SQL, (s) => c.push(s), { params: { n: 1 } });
  assert.equal(c[0].status, "loading");
  assert.deepEqual(ws.sent.at(-1), { type: "subscribe", id: "s2", sql: SQL, params: { n: 1 } });
  assert.equal(client.peek(SQL, { params: { n: 1 } }), c[0]);
  assert.equal(client.peek("select 1"), undefined);

  // a diff reaches both
  ws.deliver({ type: "diff", id: "s1", changes: [{ op: "insert", row: { user_id: "e1" } }], at: "c2" });
  assert.equal(a.at(-1)!.rows.length, 2);
  assert.equal(b.at(-1), a.at(-1));
  client.close();
});

test("after the last subscriber: live for keepAliveMs, then rows for gcTimeMs, resuming on return", async () => {
  FakeSocket.all = [];
  const { cache, calls } = memoryCache();
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any, cache, keepAliveMs: 20, gcTimeMs: 60 });
  const sub = client.subscribe(SQL, () => {});
  const ws = FakeSocket.all[0];
  ws.open();
  await tick(1);
  ws.deliver(snapshot("s1", ["h1"], "c1"));
  sub.unsubscribe();

  // within keepAliveMs: still live, a new subscriber sends nothing
  const again = client.subscribe(SQL, () => {});
  assert.equal(client.peek(SQL)!.status, "live");
  again.unsubscribe();
  await tick(5);
  assert.deepEqual(ws.sent.map((m) => m.type), ["subscribe"]);

  // then the server subscription ends; the rows stay, cached
  await tick(30);
  assert.deepEqual(ws.sent.at(-1), { type: "unsubscribe", id: "s1" });
  assert.equal(client.peek(SQL)!.status, "cached");

  // coming back: the rows at once, and a resume from their cursor (no cache read)
  const loads = calls.filter((c) => c.startsWith("load")).length;
  const states: LiveState[] = [];
  const back = client.subscribe(SQL, (s) => states.push(s));
  assert.equal(states[0].status, "cached");
  assert.deepEqual(states[0].rows, [{ user_id: "h1" }]);
  assert.deepEqual(ws.sent.at(-1), { type: "subscribe", id: "s2", sql: SQL, since: "c1" });
  assert.equal(calls.filter((c) => c.startsWith("load")).length, loads);
  ws.deliver({ type: "resume", id: "s2", columns: COLUMNS });
  assert.equal(states.at(-1)!.status, "live");
  back.unsubscribe();

  // after keepAliveMs + gcTimeMs, gone
  await tick(100);
  assert.equal(client.peek(SQL), undefined);
  client.close();
});

test("results nobody subscribes to are evicted beyond memoryRows, oldest first", async () => {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any, memoryRows: 3 });
  const subs = ["q1", "q2", "q3"].map((q) => client.subscribe(`select '${q}'`, () => {}));
  const ws = FakeSocket.all[0];
  ws.open();
  ws.deliver(snapshot("s1", ["a", "b"]));
  ws.deliver(snapshot("s2", ["c"]));
  ws.deliver(snapshot("s3", ["d"]));
  subs[0].unsubscribe();
  await tick(2);
  subs[1].unsubscribe();
  // idle: q1 (2 rows) and q2 (1 row) fit; q3 is still subscribed
  assert.ok(client.peek("select 'q1'") && client.peek("select 'q2'"));
  subs[2].unsubscribe();
  // 4 idle rows: the oldest, q1, goes
  assert.equal(client.peek("select 'q1'"), undefined);
  assert.ok(client.peek("select 'q2'") && client.peek("select 'q3'"));
  client.close();
});

test("a failed query is tried again once nobody is looking at the error", () => {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any });
  const first: LiveState[] = [];
  const sub = client.subscribe(SQL, (s) => first.push(s));
  const ws = FakeSocket.all[0];
  ws.open();
  ws.deliver({ type: "error", id: "s1", code: "STREAM_FAILED", message: "gone" });
  // shared while someone sees it
  const second: LiveState[] = [];
  const other = client.subscribe(SQL, (s) => second.push(s));
  assert.equal(second[0].status, "error");
  sub.unsubscribe();
  other.unsubscribe();
  const third: LiveState[] = [];
  client.subscribe(SQL, (s) => third.push(s));
  assert.equal(third[0].status, "loading");
  assert.deepEqual(ws.sent.at(-1), { type: "subscribe", id: "s2", sql: SQL });
  client.close();
});

test("preload subscribes ahead of time; later subscribers get the rows synchronously", async () => {
  FakeSocket.all = [];
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any, keepAliveMs: 50 });
  let resolved = false;
  const done = client.preload(SQL).then(() => (resolved = true));
  const ws = FakeSocket.all[0];
  ws.open();
  assert.deepEqual(ws.sent, [{ type: "subscribe", id: "s1", sql: SQL }]);
  await tick(1);
  assert.equal(resolved, false);
  ws.deliver(snapshot("s1", ["h1"]));
  await done;
  const states: LiveState[] = [];
  client.subscribe(SQL, (s) => states.push(s));
  assert.equal(states[0].status, "live");
  assert.deepEqual(states[0].rows, [{ user_id: "h1" }]);
  // preloading what's already there resolves at once
  await client.preload(SQL);
  assert.equal(ws.sent.length, 1);
  client.close();
});

test("the cache's most recent results are warmed into memory; subscribing resumes without a read", async () => {
  FakeSocket.all = [];
  const KEY = cacheKey(SQL, { n: 1 });
  const { cache, calls } = memoryCache();
  let asked: number | undefined;
  cache.recent = async (maxRows) => {
    asked = maxRows;
    return [
      { key: KEY, result: { rows: [{ user_id: "w" }], at: "c9", savedAt: 7 } },
      { key: "select 2", result: { rows: [], at: "c1", savedAt: 1 } },
    ];
  };
  const client = new LiveClient({ url: "ws://x/live", WebSocket: FakeSocket as any, cache });
  await tick(1);
  assert.equal(asked, 10_000);
  const warm = client.peek(SQL, { params: { n: 1 } })!;
  assert.equal(warm.status, "cached");
  assert.equal(warm.cachedAt, 7);
  assert.deepEqual(warm.rows, [{ user_id: "w" }]);
  assert.equal(FakeSocket.all.length, 0, "warming doesn't connect");

  const states: LiveState[] = [];
  client.subscribe(SQL, (s) => states.push(s), { params: { n: 1 } });
  assert.equal(states[0], warm);
  const ws = FakeSocket.all[0];
  ws.open();
  assert.deepEqual(ws.sent, [{ type: "subscribe", id: "s1", sql: SQL, since: "c9", params: { n: 1 } }]);
  assert.deepEqual(calls.filter((c) => c.startsWith("load")), []);
  client.close();
});
