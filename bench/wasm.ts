// chlive-core as WebAssembly (`wasm`, into client/wasm/) against the TS client's ResultStore,
// on the same messages: size, load, applying snapshots and diffs (with what listeners need: a
// new rows array each time), and placing rows as the server would (Predictor).
//
//   wasm && bun bench/wasm.ts

import { gzipSync } from "node:zlib";

import { ResultStore } from "../client/src/state.ts";
import { initSync, Predictor, Store } from "../client/wasm/chlive_wasm.js";

type Row = Record<string, unknown>;

const WASM = new URL("../client/wasm/chlive_wasm_bg.wasm", import.meta.url);

const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];
const p99 = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length * 0.99)];
const ms = (x: number) => (x < 0.1 ? x.toFixed(3) : x < 10 ? x.toFixed(2) : x.toFixed(1));

function time<T>(f: () => T): [T, number] {
  const t = performance.now();
  const out = f();
  return [out, performance.now() - t];
}

// a deterministic stream of rows, like the dev stack's events
let seed = 42;
const random = () => ((seed = (seed * 1103515245 + 12345) % 2 ** 31) / 2 ** 31);
const row = (i: number): Row => ({
  ts: `2026-11-01 12:${String(i % 60).padStart(2, "0")}:00.${String(i % 1000).padStart(3, "0")}`,
  user_id: `u${Math.floor(random() * 5000)}`,
  latency_ms: Math.floor(random() * 2000),
});
const COLUMNS = [
  { name: "ts", type: "DateTime64(3)" },
  { name: "user_id", type: "String" },
  { name: "latency_ms", type: "UInt32" },
];
const snapshotText = (rows: Row[], ids?: string[]) =>
  JSON.stringify({ type: "snapshot", id: "s1", columns: COLUMNS, rows, at: "c", ...(ids ? { ids } : {}) });

/** The rows listeners get after a diff, from the changes as the WASM store applied them. */
function mirror(rows: Row[], applied: [number, number | null, Row?][]): Row[] {
  const next = rows.slice();
  for (const [op, index, r] of applied) {
    if (op === 0) index === null ? next.push(r!) : next.splice(index, 0, r!);
    else if (op === 1) next.splice(index!, 1);
  }
  return next;
}

const report: string[] = [];
const line = (s: string) => {
  console.log(s);
  report.push(s);
};

// --- size and load ---------------------------------------------------------------------------
const bytes = new Uint8Array(await Bun.file(WASM).arrayBuffer());
line(`## Size\n\nraw ${bytes.length} B, gzip -9 ${gzipSync(bytes, { level: 9 }).length} B\n`);
const [module, compileMs] = time(() => new WebAssembly.Module(bytes));
const [, instantiateMs] = time(() => initSync({ module }));
line(`## Load\n\ncompile ${ms(compileMs)} ms, instantiate ${ms(instantiateMs)} ms\n`);

// --- snapshots -------------------------------------------------------------------------------
line("## Snapshot (ms; median of 5)\n");
line("| rows | TS: JSON.parse + applySnapshot (dates parsed) | TS, no parsers | WASM: apply(text) | WASM: apply_value(parsed) | WASM: apply(text) + rows() |");
line("|---|---|---|---|---|---|");
for (const n of [1_000, 57_000, 500_000]) {
  const rows = Array.from({ length: n }, (_, i) => row(i));
  const text = snapshotText(rows);
  const runs = n >= 500_000 ? 3 : 5;
  const ts: number[] = [], tn: number[] = [], wt: number[] = [], wv: number[] = [], wm: number[] = [];
  for (let r = 0; r < runs; r++) {
    ts.push(time(() => new ResultStore().applySnapshot(JSON.parse(text)))[1]);
    tn.push(time(() => new ResultStore({ parsers: false }).applySnapshot(JSON.parse(text)))[1]);
    wt.push(time(() => new Store().apply(text))[1]);
    wv.push(time(() => new Store().apply_value(JSON.parse(text)))[1]);
    wm.push(
      time(() => {
        const s = new Store();
        s.apply(text);
        return JSON.parse(s.rows());
      })[1],
    );
  }
  line(`| ${n} | ${ms(median(ts))} | ${ms(median(tn))} | ${ms(median(wt))} | ${ms(median(wv))} | ${ms(median(wm))} |`);
}

// --- diffs -----------------------------------------------------------------------------------
line("\n## Diffs at 57k rows, ordered with LIMIT-like churn: an insert at an index and a delete (ms per diff)\n");
{
  const n = 57_000;
  const rows = Array.from({ length: n }, (_, i) => row(i));
  const text = snapshotText(rows);
  // the diffs, generated against a model of the result
  let len = n;
  const diffs: string[] = [];
  for (let i = 0; i < 1000; i++) {
    const at = Math.floor(random() * (len + 1));
    const out = Math.floor(random() * (len + 1));
    diffs.push(
      JSON.stringify({ type: "diff", id: "s1", at: `c${i}`, changes: [{ op: "insert", row: row(n + i), index: at }, { op: "delete", index: out }] }),
    );

  }
  const ts = new ResultStore();
  ts.applySnapshot(JSON.parse(text));
  const tsTimes = diffs.map((d) => time(() => (ts.applyDiff(JSON.parse(d)), ts.rows()))[1]);
  const tn = new ResultStore({ parsers: false });
  tn.applySnapshot(JSON.parse(text));
  const tnTimes = diffs.map((d) => time(() => (tn.applyDiff(JSON.parse(d)), tn.rows()))[1]);

  const w = new Store();
  w.apply(text);
  let view: Row[] = JSON.parse(w.rows());
  const wTimes = diffs.map((d) => time(() => (view = mirror(view, JSON.parse(w.apply(d)))))[1]);

  const wv = new Store();
  wv.apply(text);
  let view2: Row[] = JSON.parse(wv.rows());
  const wvTimes = diffs.map((d) => time(() => (view2 = mirror(view2, wv.apply_value(JSON.parse(d)))))[1]);

  const same = JSON.stringify(ts.wireRows()) === JSON.stringify(view) && JSON.stringify(view) === wv.rows();
  line("| | median | p99 |");
  line("|---|---|---|");
  line(`| TS: JSON.parse + applyDiff + rows() (dates parsed) | ${ms(median(tsTimes))} | ${ms(p99(tsTimes))} |`);
  line(`| TS, no parsers | ${ms(median(tnTimes))} | ${ms(p99(tnTimes))} |`);
  line(`| WASM: apply(text) + mirror the changes into a new array | ${ms(median(wTimes))} | ${ms(p99(wTimes))} |`);
  line(`| WASM: apply_value(parsed) + mirror | ${ms(median(wvTimes))} | ${ms(p99(wvTimes))} |`);
  const wm = new Store();
  wm.apply(text);
  const wmTimes = diffs.slice(0, 50).map((d) => time(() => (wm.apply(d), JSON.parse(wm.rows())))[1]);
  line(`| WASM: apply(text) + every row out (rows()) | ${ms(median(wmTimes))} | ${ms(p99(wmTimes))} |`);
  line(`\nsame rows at the end: ${same}\n`);
}

// --- updates by id ---------------------------------------------------------------------------
line("## Updates by id at 5k groups (an aggregate), moving (ms per diff)\n");
{
  const n = 5_000;
  const rows = Array.from({ length: n }, (_, i) => ({ country: `c${i}`, n: n - i }));
  const ids = rows.map((r) => JSON.stringify([r.country]));
  const text = JSON.stringify({ type: "snapshot", id: "s1", columns: [], rows, ids, at: "c" });
  const diffs = Array.from({ length: 1000 }, (_, i) => {
    const id = ids[Math.floor(random() * n)];
    return JSON.stringify({ type: "diff", id: "s1", at: `c${i}`, changes: [{ op: "update", id, set: { n: n + i }, index: 0 }] });
  });
  const ts = new ResultStore();
  ts.applySnapshot(JSON.parse(text));
  const tsTimes = diffs.map((d) => time(() => (ts.applyDiff(JSON.parse(d)), ts.rows()))[1]);
  const w = new Store();
  w.apply(text);
  const wTimes = diffs.map((d) => time(() => JSON.parse(w.apply(d)))[1]);
  line("| | median | p99 |");
  line("|---|---|---|");
  line(`| TS: JSON.parse + applyDiff + rows() | ${ms(median(tsTimes))} | ${ms(p99(tsTimes))} |`);
  line(`| WASM: apply(text) (changes out, no mirror) | ${ms(median(wTimes))} | ${ms(p99(wTimes))} |`);
  line(`\nsame rows at the end: ${JSON.stringify(ts.rows()) === w.rows()}\n`);
}

// --- predicting --------------------------------------------------------------------------------
// the plan of `select ts, user_id, latency_ms from events order by latency_ms desc [limit n]`,
// as an optimistic subscription gets it
const plan = (limit: number | null) => {
  const col = (id: number, name: string, ty: string) => ({ id, name, source: { json: name }, ty });
  return JSON.stringify({
    v: 1,
    table: "events",
    stream: {
      topics: [],
      columns: [col(0, "ts", "DateTime64(3)"), col(1, "user_id", "String"), col(2, "latency_ms", "UInt32")],
      width: 3,
      output: [["ts", 0], ["user_id", 1], ["latency_ms", 2]],
      filter: "all",
      order: {
        keys: [{ column: 2, desc: true, nulls_first: false }],
        columns: [{ id: 2, name: "latency_ms", history_field: "latency_ms", ty: "UInt32" }],
        hidden: [],
        text: "latency_ms DESC NULLS LAST",
      },
      limit,
      offset: null,
      keyed: null,
      windows: [],
      history_fields: [],
    },
  });
};
line("## Showing a pending row as the server would place it (`order by latency_ms desc`), 57k rows\n");
line("| | seed (ms) | view with 1 pending, median (ms) | p99 (ms) | view with 10 pending, median (ms) | server diff, then view, median (ms) |");
line("|---|---|---|---|---|---|");
for (const limit of [null, 200]) {
  const rows = Array.from({ length: 57_000 }, (_, i) => row(i)).sort(
    (a, b) => (b.latency_ms as number) - (a.latency_ms as number),
  );
  const shown = limit ? rows.slice(0, limit) : rows;
  const p = new Predictor(plan(limit), undefined);
  const [, seedMs] = time(() => p.seed(JSON.stringify(shown), undefined));
  const one = Array.from({ length: 200 }, (_, i) => JSON.stringify([{ row: row(100_000 + i) }]));
  const t = one.map((pending) => time(() => JSON.parse(p.view(pending, Date.now())))[1]);
  const ten = JSON.stringify(Array.from({ length: 10 }, (_, i) => ({ row: row(200_000 + i) })));
  const t10 = Array.from({ length: 50 }, () => time(() => JSON.parse(p.view(ten, Date.now())))[1]);
  // a server diff in between: applied to the predictor's rows, whose state is seeded again
  const diff = (i: number) =>
    JSON.stringify({ type: "diff", id: "s1", at: `c${i}`, changes: [{ op: "insert", row: { ts: "2026-11-01 12:00:00.000", user_id: "x", latency_ms: 0 }, index: shown.length + i }] });
  const td = Array.from({ length: 50 }, (_, i) =>
    time(() => {
      p.apply(diff(i));
      return JSON.parse(p.view(one[0], Date.now()));
    })[1],
  );
  line(
    `| ${limit ? `limit ${limit}` : "no limit"} | ${ms(seedMs)} | ${ms(median(t))} | ${ms(p99(t))} | ${ms(median(t10))} | ${ms(median(td))} |`,
  );
}

if (process.argv.includes("--write")) {
  await Bun.write(new URL("../docs/bench/wasm.raw.md", import.meta.url), report.join("\n") + "\n");
}
