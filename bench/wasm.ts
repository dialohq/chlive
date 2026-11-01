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
line("## Placing a pending row as the server would (`order by latency_ms desc`), 57k rows\n");
line("| | seed (ms) | place, median (ms) | place, p99 (ms) |");
line("|---|---|---|---|");
for (const limit of [0, 200]) {
  const rows = Array.from({ length: 57_000 }, (_, i) => row(i)).sort(
    (a, b) => (b.latency_ms as number) - (a.latency_ms as number),
  );
  const shown = limit ? rows.slice(0, limit) : rows;
  const p = new Predictor(limit);
  const [, seedMs] = time(() => p.seed(JSON.stringify(shown)));
  const places = Array.from({ length: 1000 }, (_, i) => JSON.stringify(row(100_000 + i)));
  const t = places.map((r) => time(() => JSON.parse(p.place(r)))[1]);
  line(`| ${limit ? `limit ${limit}` : "no limit"} | ${ms(seedMs)} | ${ms(median(t))} | ${ms(p99(t))} |`);
}

if (process.argv.includes("--write")) {
  await Bun.write(new URL("../docs/bench/wasm.raw.md", import.meta.url), report.join("\n") + "\n");
}
