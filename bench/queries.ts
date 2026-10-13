// Query benchmark through @chlive/client: a ladder of queries from trivial to the most complex
// the dialect has, each measured three ways against the running stack (`dnvr up`):
//
// - snapshot: subscribe → first result (`live`), cold (the first subscription) and warm;
// - latency: one event at a time, produce → the diff it causes reaching the client;
// - throughput: how fast a subscription goes through a backlog. Right after the table's snapshot
//   is refreshed (every 10 s), a burst of realistic events is produced, then the query is
//   subscribed: its snapshot is the one from before the burst, so the engine replays the whole
//   burst from Kafka as fast as it can. Measured from the snapshot reaching the client until
//   the subscription's cursor covers the burst's last offsets, with the server's CPU time.
//   A snapshot only holds what the ClickHouse sink has ingested, so each run first waits for
//   the sink to catch up (it's much slower than the engine), and the events replayed are
//   counted from the snapshot's cursor.
//
// Every query comes with `hit(i)`: an event guaranteed to change its result, used for the
// latency runs and to keep the cursor moving at the end of a burst.
//
//   bun bench/queries.ts [--seed 1000000] [--burst 50000] [--repeat 3] [--hits 200] [--snapshots 5]
//                        [--only q05,q08] [--phases snapshot,latency,throughput] [--json out.json]

import { LiveClient, type LiveState, type SubscriptionStats } from "../client/src/index.ts";

const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i].replace(/^--/, ""), process.argv[i + 1]);
const SEED = Number(args.get("seed") ?? 1_000_000);
const BURST = Number(args.get("burst") ?? 50_000);
const REPEAT = Number(args.get("repeat") ?? 3);
const HITS = Number(args.get("hits") ?? 200);
const SNAPSHOT_RUNS = Number(args.get("snapshots") ?? 5);
const ONLY = args.get("only")?.split(",");
const PHASES = (args.get("phases") ?? "snapshot,latency,throughput").split(",");
const JSON_OUT = args.get("json");

const WS_URL = process.env.CHLIVE_URL ?? "ws://127.0.0.1:8080/live";
const CH_URL = process.env.CH_URL ?? "http://127.0.0.1:18123";
const PROXY_URL = process.env.PROXY_URL ?? "http://127.0.0.1:8082";
const PARTITIONS = 3;
/** Tags this run's own rows, where a query must not see earlier runs'. */
const RUN = Date.now().toString(36);

// --- the stack ---------------------------------------------------------------------------------

async function ch(sql: string): Promise<string> {
  const r = await fetch(`${CH_URL}/?user=default`, { method: "POST", body: sql });
  const text = await r.text();
  if (!r.ok) throw new Error(`ClickHouse: ${text}`);
  return text.trim();
}

/** Last produced offset per partition. */
type Offsets = Map<number, number>;

let nextPartition = 0;
/** The last offset produced to each partition of `events`, by this process. */
const produced: Offsets = new Map();

/** Produce to `topic`, round-robin over the partitions; the last offset written to each. */
async function produce(topic: string, values: object[]): Promise<Offsets> {
  const records = values.map((value) => ({ value, partition: nextPartition++ % PARTITIONS }));
  const r = await fetch(`${PROXY_URL}/topics/${topic}`, {
    method: "POST",
    headers: { "Content-Type": "application/vnd.kafka.json.v2+json" },
    body: JSON.stringify({ records }),
  });
  if (!r.ok) throw new Error(`produce: ${r.status} ${await r.text()}`);
  const { offsets } = (await r.json()) as { offsets: { partition: number; offset: number; error_code?: number }[] };
  const out: Offsets = new Map();
  for (const o of offsets) {
    if (o.error_code) throw new Error(`produce: error ${o.error_code}`);
    out.set(o.partition, Math.max(out.get(o.partition) ?? -1, o.offset));
  }
  if (topic === "events") mergeOffsets(produced, out);
  return out;
}

function mergeOffsets(into: Offsets, more: Offsets): void {
  for (const [p, o] of more) into.set(p, Math.max(into.get(p) ?? -1, o));
}

/** Wait until the ClickHouse sink has ingested everything produced to `events`. */
async function waitIngested(): Promise<void> {
  for (;;) {
    const state = new Map(
      (await ch("SELECT key, maxOffset FROM default.chlive_sink_events_state FORMAT TSV"))
        .split("\n")
        .map((l) => l.split("\t"))
        .map(([k, o]) => [Number(k.split("-").pop()), Number(o)] as const),
    );
    if ([...produced].every(([p, o]) => (state.get(p) ?? -1) >= o)) return;
    await Bun.sleep(500);
  }
}

/** A cursor's offsets: per partition, the last offset the result includes. */
function cursorOffsets(at: string): Record<string, number> {
  return (JSON.parse(at) as { o: Record<string, number> }).o;
}

/** The events after a cursor, up to the last offsets `end`. */
function between(at: string, end: Offsets): number {
  const o = cursorOffsets(at);
  let n = 0;
  for (const [p, off] of end) n += Math.max(0, off - (o[p] ?? -1));
  return n;
}

/** Whether a cursor includes every offset in `want`. */
function covers(at: string | undefined, want: Offsets): boolean {
  if (!at) return false;
  const o = cursorOffsets(at);
  for (const [p, off] of want) if ((o[p] ?? -1) < off) return false;
  return true;
}

/** The chlive-server process's CPU seconds so far (user + system). */
async function serverCpu(): Promise<number> {
  const pid = (await Bun.$`pgrep -f "target/release/chlive( |$)"`.text()).trim().split("\n")[0];
  const t = (await Bun.$`ps -o time= -p ${pid}`.text()).trim(); // [[dd-]hh:]mm:ss.cc
  return t.split(":").reduce((acc, part) => acc * 60 + Number(part), 0);
}

// --- data ----------------------------------------------------------------------------------------

const COUNTRIES = ["US", "DE", "GB", "PL", "FR", "IN", "BR", "JP", "CA", "ES", "IT", "NL", "SE", "AU", "MX"];
const WEIGHTS = [30, 12, 10, 8, 8, 7, 5, 5, 4, 3, 3, 2, 1, 1, 1];
const MIX = COUNTRIES.flatMap((c, i) => Array(WEIGHTS[i]).fill(c));
const PATHS = ["/", "/search", "/product", "/cart", "/checkout", "/login", "/account", "/api/orders"];
const pick = <T>(xs: T[]) => xs[Math.floor(Math.random() * xs.length)];
const iso = (ms: number) => new Date(ms).toISOString().replace("T", " ").replace("Z", "");

/** A random event, as `scripts/seed.sql` makes them, at `ts`. */
function randomEvent(ts: number) {
  return {
    ts: iso(ts),
    user_id: `u${Math.floor(Math.random() * 5000)}`,
    country: pick(MIX),
    path: pick(PATHS),
    latency_ms: Math.floor(200 * Math.pow(-Math.log(1 - Math.random()), 1.6)) + 1,
    amount: Math.random() < 0.6 ? 0 : (100 + Math.floor(Math.random() * 49900)) / 100,
  };
}

async function count(table: string): Promise<number> {
  return Number(await ch(`SELECT count() FROM default.${table}`));
}

async function waitCount(table: string, n: number): Promise<void> {
  while ((await count(table)) < n) await Bun.sleep(1000);
}

/** Top up `events` to SEED rows (seed.sql's distribution) and `customers` to 5000. */
async function seed(): Promise<void> {
  const have = await count("events");
  if (have < SEED) {
    const n = SEED - have;
    console.error(`seeding ${n} events (have ${have})`);
    const now = Date.now();
    for (let done = 0; done < n; ) {
      const batch = Math.min(20_000, n - done);
      await Promise.all(
        Array.from({ length: 4 }, (_, k) => {
          const part = Math.ceil(batch / 4);
          const m = Math.min(part, batch - k * part);
          if (m <= 0) return undefined;
          return produce(
            "events",
            Array.from({ length: m }, () => randomEvent(now - Math.floor(Math.random() * 7 * 86_400_000))),
          );
        }),
      );
      done += batch;
    }
    await waitCount("events", SEED);
  }
  const customers = await count("customers");
  if (customers < 5000) {
    console.error(`seeding ${5000 - customers} customers`);
    const rows = [];
    for (let i = customers; i < 5000; i++)
      rows.push({ ts: iso(Date.now()), customer_id: `u${i}`, name: `Customer ${i}`, country: pick(MIX) });
    for (let i = 0; i < rows.length; i += 5000) await produce("customers", rows.slice(i, i + 5000));
    await waitCount("customers", 5000);
  }
}

// --- the queries -----------------------------------------------------------------------------------

/** What the hit events build on, read once from ClickHouse. */
interface Ctx {
  maxLatency: number;
  maxAmount: number;
  /** Past every event's ts. */
  future: number;
  /** Customers in PL (some customer ids repeat; any row will do). */
  pl: string[];
}

interface Query {
  id: string;
  what: string;
  sql: string;
  /** The i-th event that changes the result (i counts up across the whole run). */
  hit: (i: number, ctx: Ctx) => object;
}

const base = (ctx: Ctx, i: number) => ({ ...randomEvent(ctx.future + i), user_id: `bench-${i}` });

const QUERIES: Query[] = [
  {
    id: "q01",
    what: "one column, equality filter (nearly empty result)",
    sql: `select user_id from events where user_id = 'bench-${RUN}'`,
    hit: (i, ctx) => ({ ...base(ctx, i), user_id: `bench-${RUN}` }),
  },
  {
    id: "q02",
    what: "all columns, range filter (~1.5% of rows)",
    sql: "select * from events where latency_ms > 2000",
    hit: (i, ctx) => ({ ...base(ctx, i), latency_ms: 2001 + i }),
  },
  {
    id: "q03",
    what: "compound filter: IN, OR, !=, AND",
    sql:
      "select ts, user_id, path, latency_ms from events " +
      "where (country in ('PL', 'DE', 'FR') or latency_ms >= 1000) and path != '/' and amount > 250",
    hit: (i, ctx) => ({ ...base(ctx, i), country: "PL", path: "/cart", amount: 300 }),
  },
  {
    id: "q04",
    what: "LIMIT without ORDER BY: a sliding window of 1000",
    sql: "select * from events limit 1000",
    hit: (i, ctx) => base(ctx, i),
  },
  {
    id: "q05",
    what: "ORDER BY one key, LIMIT 100 (top-N)",
    sql: "select ts, user_id, latency_ms from events order by latency_ms desc limit 100",
    hit: (i, ctx) => ({ ...base(ctx, i), latency_ms: ctx.maxLatency + 1 + i }),
  },
  {
    id: "q06",
    what: "filter + ORDER BY three keys + LIMIT/OFFSET",
    sql:
      "select ts, user_id, country, latency_ms from events where country != 'US' " +
      "order by country asc, latency_ms desc, ts desc limit 50 offset 100",
    // sorts above every row: the result shifts by one
    hit: (i, ctx) => ({ ...base(ctx, i), country: "AA", latency_ms: ctx.maxLatency + 1 + i }),
  },
  {
    id: "q07",
    what: "global aggregate: count()",
    sql: "select count() from events",
    hit: (i, ctx) => base(ctx, i),
  },
  {
    id: "q08",
    what: "GROUP BY low cardinality (15), native aggregates, ORDER BY + LIMIT",
    sql:
      "select country, count() as n, avg(latency_ms) as avg_latency, max(latency_ms) as max_latency " +
      "from events group by country order by n desc limit 5",
    hit: (i, ctx) => ({ ...base(ctx, i), country: "US" }),
  },
  {
    id: "q09",
    what: "GROUP BY two keys (120 groups), ORDER BY two terms",
    sql:
      "select country, path, count() as n, avg(latency_ms) as avg_latency, max(amount) as max_amount " +
      "from events group by country, path order by n desc, country asc limit 20",
    hit: (i, ctx) => ({ ...base(ctx, i), country: "US", path: "/" }),
  },
  {
    id: "q10",
    what: "GROUP BY high cardinality (~5000 users), filter, ORDER BY sum, LIMIT/OFFSET",
    sql:
      "select user_id, count() as n, sum(amount) as spent from events where amount > 0 " +
      "group by user_id order by spent desc limit 10 offset 5",
    // a new user spending more than anyone: everything shifts by one
    hit: (i, ctx) => ({ ...base(ctx, i), amount: ctx.maxAmount * 1000 * (i + 1) }),
  },
  {
    id: "q11",
    what: "GROUP BY with chDB aggregates (uniqExact, median) next to count()",
    sql:
      "select country, uniqExact(user_id) as users, median(latency_ms) as p50, count() as n " +
      "from events group by country order by users desc",
    hit: (i, ctx) => ({ ...base(ctx, i), country: "US" }),
  },
  {
    id: "q12",
    what: "the most complex aggregate: filter with OR, 2 keys, mixed chDB/native, ORDER BY an unselected aggregate, LIMIT/OFFSET",
    sql:
      "select country, path, count() as n, uniq(user_id) as users, avg(latency_ms) as avg_latency " +
      "from events where latency_ms > 50 and (amount > 0 or path = '/checkout') " +
      "group by country, path order by max(latency_ms) desc, n desc limit 10 offset 2",
    // three groups take turns at the top: each hit moves the one at rank 2 to rank 0
    hit: (i, ctx) => ({
      ...base(ctx, i),
      country: ["SE", "AU", "MX"][i % 3],
      path: "/checkout",
      latency_ms: ctx.maxLatency + 1 + i,
    }),
  },
  {
    id: "q13",
    what: "IN (sub-query) on customers, filter, ORDER BY ts, LIMIT",
    sql:
      "select ts, user_id, path, latency_ms from events " +
      "where user_id in (select customer_id from customers where country = 'PL') and latency_ms > 100 " +
      "order by ts desc limit 100",
    hit: (i, ctx) => ({ ...base(ctx, i), user_id: ctx.pl[i % ctx.pl.length], latency_ms: 500 }),
  },
  {
    id: "q14",
    what: "the most complex plain query: correlated EXISTS + compound filter + 2-key ORDER BY + LIMIT/OFFSET",
    sql:
      "select e.ts, e.user_id, e.country, e.amount from events e " +
      "where exists (select 1 from customers c where c.customer_id = e.user_id " +
      "and c.country in ('PL', 'DE') and c.name != '') " +
      "and (e.amount > 50 or e.latency_ms > 1000) and e.path != '/login' " +
      "order by e.amount desc, e.ts desc limit 25 offset 10",
    hit: (i, ctx) => ({
      ...base(ctx, i),
      user_id: ctx.pl[i % ctx.pl.length],
      path: "/cart",
      amount: ctx.maxAmount * 1000 * (i + 1),
    }),
  },
];

// --- measuring -------------------------------------------------------------------------------------

/**
 * A subscription whose updates can be awaited. Times are taken in the listener, when the client
 * applies a message: Bun can deliver several messages before an `await` resumes.
 */
class Watch {
  state: LiveState | undefined;
  /** When the snapshot was applied, and the stats then. */
  liveAt: number | undefined;
  atSnapshot: SubscriptionStats | undefined;
  snapshotApplyMs = 0;
  /** Over the diffs since the snapshot: time applying them, the longest one, their changes. */
  applyMs = 0;
  maxApplyMs = 0;
  changes = 0;
  private waiters: { pred: (s: LiveState) => boolean; resolve: (m: Match) => void }[] = [];
  readonly unsubscribe: () => void;

  constructor(client: LiveClient, sql: string) {
    let seen: unknown;
    this.unsubscribe = client.subscribe(sql, (s) => {
      const now = performance.now();
      this.state = s;
      const msg = s.stats.lastMessage;
      if (msg !== seen && s.stats.lastApplyMs !== undefined) {
        seen = msg;
        if (msg?.type === "snapshot" && this.liveAt === undefined) {
          this.liveAt = now;
          this.atSnapshot = s.stats;
          this.snapshotApplyMs = s.stats.lastApplyMs;
        } else if (msg?.type === "diff") {
          this.applyMs += s.stats.lastApplyMs;
          this.maxApplyMs = Math.max(this.maxApplyMs, s.stats.lastApplyMs);
          this.changes += msg.changes.length;
        }
      }
      this.waiters = this.waiters.filter((w) => {
        if (!w.pred(s)) return true;
        w.resolve({ s, t: now });
        return false;
      });
    }).unsubscribe;
  }

  /** The first state matching `pred`, and when it was applied. */
  until(pred: (s: LiveState) => boolean, timeoutMs = 120_000): Promise<Match> {
    if (this.state && pred(this.state)) return Promise.resolve({ s: this.state, t: performance.now() });
    return new Promise<Match>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`timed out after ${timeoutMs} ms`)), timeoutMs);
      this.waiters.push({
        pred,
        resolve: (m) => {
          clearTimeout(timer);
          resolve(m);
        },
      });
    }).then((m) => {
      if (m.s.status === "error") throw new Error(JSON.stringify(m.s.error));
      return m;
    });
  }

  live(): Promise<Match> {
    return this.until((s) => s.status === "live" || s.status === "error");
  }
}

interface Match {
  s: LiveState;
  t: number;
}

const pct = (xs: number[], p: number) => {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
};
const median = (xs: number[]) => pct(xs, 50);

interface Result {
  id: string;
  what: string;
  sql: string;
  snapshot?: { rows: number; bytes: number; coldMs: number; warmMs: number; applyMs: number; clickhouseMs: number };
  latency?: { p50: number; p95: number; p99: number; max: number; produceMs: number; missed: number };
  throughput?: Sample[];
}

/** One throughput run. */
interface Sample {
  /** Events between the snapshot's cursor and the burst's end. */
  events: number;
  produceMs: number;
  /** Subscribe → snapshot applied, and → caught up with the burst. */
  snapshotMs: number;
  caughtUpMs: number;
  serverCpuUsPerEvent: number;
  diffs: number;
  changes: number;
  bytes: number;
  clientApplyMs: number;
  maxApplyMs: number;
}

async function snapshotRuns(client: LiveClient, q: Query, r: Result): Promise<void> {
  const times: number[] = [];
  let last: LiveState | undefined;
  let applyMs = 0;
  for (let k = 0; k < SNAPSHOT_RUNS; k++) {
    const t0 = performance.now();
    const w = new Watch(client, q.sql);
    const m = await w.live();
    last = m.s;
    times.push(m.t - t0);
    applyMs = w.snapshotApplyMs;
    w.unsubscribe();
  }
  // the same SQL straight to ClickHouse (it's valid ClickHouse SQL), rows as JSON over HTTP
  const direct: number[] = [];
  for (let k = 0; k < SNAPSHOT_RUNS; k++) {
    const t0 = performance.now();
    await ch(`${q.sql} FORMAT JSONEachRow`);
    direct.push(performance.now() - t0);
  }
  r.snapshot = {
    clickhouseMs: median(direct),
    rows: last!.rows.length,
    bytes: last!.stats.bytes,
    coldMs: times[0],
    warmMs: median(times.slice(1)),
    applyMs,
  };
  console.error(`  snapshot: ${r.snapshot.rows} rows, cold ${times[0].toFixed(0)} ms, warm ${r.snapshot.warmMs.toFixed(0)} ms, ClickHouse ${r.snapshot.clickhouseMs.toFixed(0)} ms`);
}

let hitNo = 0;

async function latencyRuns(client: LiveClient, q: Query, ctx: Ctx, r: Result): Promise<void> {
  await waitIngested();
  const w = new Watch(client, q.sql);
  await w.live();
  // the snapshot can be up to a refresh interval old: untimed hits until the stream has
  // caught up with everything produced
  for (;;) {
    const diffs = w.state!.stats.diffs;
    const arrived = w.until((s) => s.stats.diffs > diffs, 60_000);
    await produce("events", [q.hit(hitNo++, ctx)]);
    if (covers((await arrived).s.stats.at, produced)) break;
  }
  await Bun.sleep(300);
  const lat: number[] = [];
  const produceMs: number[] = [];
  let missed = 0;
  for (let k = 0; k < HITS; k++) {
    const diffs = w.state!.stats.diffs;
    const t0 = performance.now();
    const arrived = w.until((s) => s.stats.diffs > diffs, 5000);
    await produce("events", [q.hit(hitNo++, ctx)]);
    produceMs.push(performance.now() - t0);
    try {
      lat.push((await arrived).t - t0);
    } catch {
      missed++;
    }
  }
  w.unsubscribe();
  r.latency = {
    p50: median(lat),
    p95: pct(lat, 95),
    p99: pct(lat, 99),
    max: Math.max(...lat),
    produceMs: median(produceMs),
    missed,
  };
  console.error(`  latency: p50 ${r.latency.p50.toFixed(1)} ms, p99 ${r.latency.p99.toFixed(1)} ms, missed ${missed}`);
}

/** The newest snapshot clone of `events`. */
async function newestClone(): Promise<string> {
  return ch(
    "SELECT name FROM system.tables WHERE database = 'default' AND name LIKE '\\_\\_chlive\\_snap\\_events\\_%' " +
      "ORDER BY metadata_modification_time DESC, name DESC LIMIT 1",
  );
}

/** Produce `n` random events, 4 requests of 5000 in flight; the last offset per partition. */
async function burst(n: number): Promise<Offsets> {
  const end: Offsets = new Map();
  let sent = 0;
  const now = Date.now();
  async function lane() {
    while (sent < n) {
      const m = Math.min(5000, n - sent);
      sent += m;
      mergeOffsets(end, await produce("events", Array.from({ length: m }, (_, k) => randomEvent(now - k))));
    }
  }
  await Promise.all([lane(), lane(), lane(), lane()]);
  return end;
}

async function throughputRun(client: LiveClient, q: Query, ctx: Ctx): Promise<Sample> {
  // a subscription keeps the table in use, so its snapshot is refreshed every 10 s
  const keep = new Watch(client, "select user_id from events where user_id = 'bench-none'");
  await keep.live();
  await waitIngested();
  const old = await newestClone();
  while ((await newestClone()) === old) await Bun.sleep(20);
  const t0 = performance.now();
  const end = await burst(BURST);
  const produced = performance.now() - t0;
  if (produced > 8000) throw new Error(`producing took ${produced} ms: the snapshot may have moved past it`);
  const cpu0 = await serverCpu();
  const tSub = performance.now();
  const w = new Watch(client, q.sql);
  // the cursor only moves with messages: a hit every 50 ms keeps them coming at the end
  let done = false;
  const caught = w.until((s) => covers(s.stats.at, end), 300_000).finally(() => (done = true));
  await w.live();
  const tLive = w.liveAt!;
  const before = w.atSnapshot!;
  if (covers(before.at, end)) throw new Error("the snapshot already holds the burst");
  const replayed = between(before.at!, end);
  (async () => {
    while (!done) {
      await produce("events", [q.hit(hitNo++, ctx)]);
      await Bun.sleep(50);
    }
  })();
  const { s, t: tCaught } = await caught;
  const cpu = (await serverCpu()) - cpu0;
  w.unsubscribe();
  keep.unsubscribe();
  const sample: Sample = {
    events: replayed,
    produceMs: produced,
    snapshotMs: tLive - tSub,
    caughtUpMs: tCaught - tSub,
    serverCpuUsPerEvent: (cpu * 1e6) / replayed,
    diffs: s.stats.diffs - before.diffs,
    changes: w.changes,
    bytes: s.stats.bytes - before.bytes,
    clientApplyMs: w.applyMs,
    maxApplyMs: w.maxApplyMs,
  };
  console.error(
    `  throughput: ${replayed} events, caught up ${sample.caughtUpMs.toFixed(0)} ms after subscribing ` +
      `(snapshot at ${sample.snapshotMs.toFixed(0)} ms), server ${sample.serverCpuUsPerEvent.toFixed(2)} µs CPU/event`,
  );
  await Bun.sleep(1000);
  return sample;
}

// --- main ------------------------------------------------------------------------------------------

async function context(): Promise<Ctx> {
  const [maxLatency, maxAmount, maxTs] = (
    await ch("SELECT max(latency_ms), max(amount), toUnixTimestamp64Milli(max(ts)) FROM default.events FORMAT TSV")
  )
    .split("\t")
    .map(Number);
  const pl = (await ch("SELECT DISTINCT customer_id FROM default.customers WHERE country = 'PL' FORMAT TSV")).split("\n");
  return { maxLatency, maxAmount: Math.max(maxAmount, 1), future: Math.max(maxTs, Date.now()) + 86_400_000, pl };
}

await seed();
const ctx = await context();
const queries = QUERIES.filter((q) => !ONLY || ONLY.includes(q.id));
const results: Result[] = queries.map((q) => ({ id: q.id, what: q.what, sql: q.sql }));
const client = new LiveClient({ url: WS_URL });
console.error(`events: ${await count("events")}, customers: ${await count("customers")}, PL customers: ${ctx.pl.length}`);

// The phases run over all the queries in turn, so every snapshot sees the same data (the
// later phases add events).
if (PHASES.includes("snapshot")) console.error("\n# snapshots");
for (const [k, q] of queries.entries()) {
  if (!PHASES.includes("snapshot")) break;
  console.error(`${q.id}: ${q.sql}`);
  await snapshotRuns(client, q, results[k]);
}
if (PHASES.includes("latency")) console.error("\n# latency");
for (const [k, q] of queries.entries()) {
  if (!PHASES.includes("latency")) break;
  console.error(q.id);
  await latencyRuns(client, q, ctx, results[k]);
}
if (PHASES.includes("throughput")) console.error("\n# throughput");
for (const [k, q] of queries.entries()) {
  if (!PHASES.includes("throughput")) break;
  console.error(q.id);
  results[k].throughput = [];
  for (let n = 0; n < REPEAT; n++) results[k].throughput!.push(await throughputRun(client, q, ctx));
}
client.close();

const f = (x: number, d = 1) => x.toFixed(d);
const size = (b: number) => (b > 1e6 ? `${f(b / 1e6)} MB` : `${f(b / 1e3)} kB`);
console.log(`events at the start: ${SEED}+, burst ${BURST}, ${HITS} hits per query`);
if (PHASES.includes("snapshot")) {
  console.log("\n| query | rows | size | cold | warm | ClickHouse alone | client apply |");
  console.log("|---|---|---|---|---|---|---|");
  for (const r of results) {
    const t = r.snapshot!;
    console.log(
      `| ${r.id} | ${t.rows} | ${size(t.bytes)} | ${f(t.coldMs, 0)} ms | ${f(t.warmMs, 0)} ms | ` +
        `${f(t.clickhouseMs, 0)} ms | ${f(t.applyMs, 2)} ms |`,
    );
  }
}
if (PHASES.includes("latency")) {
  console.log("\n| query | p50 | p95 | p99 | max | missed |");
  console.log("|---|---|---|---|---|---|");
  for (const r of results) {
    const l = r.latency!;
    console.log(`| ${r.id} | ${f(l.p50)} ms | ${f(l.p95)} ms | ${f(l.p99)} ms | ${f(l.max)} ms | ${l.missed} |`);
  }
}
if (PHASES.includes("throughput")) {
  console.log(`\nthroughput: ${REPEAT} runs each, median (min–max)`);
  console.log("\n| query | events | snapshot | caught up | after the snapshot | server CPU/event | diffs | changes | bytes | client apply |");
  console.log("|---|---|---|---|---|---|---|---|---|---|");
  const range = (xs: number[]) => `${f(median(xs), 0)} ms (${f(Math.min(...xs), 0)}–${f(Math.max(...xs), 0)})`;
  for (const r of results) {
    const t = r.throughput!;
    const m = (g: (x: Sample) => number) => median(t.map(g));
    console.log(
      `| ${r.id} | ${f(m((x) => x.events) / 1000, 0)}k | ${f(m((x) => x.snapshotMs), 0)} ms | ` +
        `${range(t.map((x) => x.caughtUpMs))} | ${range(t.map((x) => x.caughtUpMs - x.snapshotMs))} | ` +
        `${f(m((x) => x.serverCpuUsPerEvent), 1)} µs | ${m((x) => x.diffs)} | ${m((x) => x.changes)} | ` +
        `${size(m((x) => x.bytes))} | ${f(m((x) => x.clientApplyMs), 0)} ms |`,
    );
  }
}
if (JSON_OUT) await Bun.write(JSON_OUT, JSON.stringify(results, null, 2));
process.exit(0);
