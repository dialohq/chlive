import { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { useLiveQuery } from "@chlive/client/react";

// Queries in the chlive dialect:
// SELECT * | columns FROM table [WHERE condition] [ORDER BY ALL | column [ASC|DESC] [NULLS FIRST|LAST], …] [LIMIT n]
const QUERY_PRESETS = [
  "select ts, user_id, country, path, latency_ms from events where latency_ms > 500",
  "select ts, user_id, country, latency_ms from events order by latency_ms desc",
  "select ts, user_id, country, path from events where country in ('PL', 'DE') order by country, ts desc",
  "select user_id, path, amount from events where amount > 0 order by all",
  "select ts, user_id, country, latency_ms from events order by latency_ms desc limit 10",
  "select * from events where country = 'PL' order by ts desc limit 15",
  "select ts, user_id, path, latency_ms from events limit 20",
  "select * from events where country = 'PL'",
  "select ts, user_id, path, amount from events where amount > 0 and country in ('DE', 'FR')",
  "select ts, order_id, customer_id, amount from orders where amount > 100",
  "select * from customers",
];
const SHOWN = 200;

const now = () => new Date().toISOString().replace("T", " ").replace("Z", "");
const pick = (xs) => xs[Math.floor(Math.random() * xs.length)];
const id = (prefix) => prefix + Math.random().toString(36).slice(2, 7);
const customer = () => "c" + (1 + Math.floor(Math.random() * 20));

// Random rows for each topic, shaped like its ClickHouse table.
const RANDOM = {
  events: () => ({
    ts: now(),
    user_id: id("u"),
    country: pick(["PL", "DE", "FR", "US", "GB"]),
    path: pick(["/", "/cart", "/checkout", "/search"]),
    latency_ms: Math.floor(Math.random() * 1000),
    amount: pick([0, 0, Math.round(Math.random() * 20000) / 100]),
  }),
  orders: () => ({ ts: now(), order_id: id("o"), customer_id: customer(), amount: Math.round(Math.random() * 20000) / 100 }),
  customers: () => ({ ts: now(), customer_id: customer(), name: pick(["Ada", "Linus", "Grace", "Alan", "Barbara"]), country: pick(["PL", "DE", "US"]) }),
};
const TOPICS = Object.keys(RANDOM);
const rand = (lo, hi) => lo + Math.floor(Math.random() * (hi - lo));
const money = (lo, hi) => Math.round((lo + Math.random() * (hi - lo)) * 100) / 100;

// How the generator draws rows, per topic.
const GENERATORS = {
  events: {
    Uniform: RANDOM.events,
    "Spiky latency (2% are 2–20 s)": () => ({
      ...RANDOM.events(),
      latency_ms: Math.random() < 0.02 ? rand(2000, 20000) : rand(20, 200),
    }),
    "Poland only": () => ({ ...RANDOM.events(), country: "PL" }),
    "Lots of ties (latency 100/200/300)": () => ({
      ...RANDOM.events(),
      country: pick(["PL", "DE"]),
      latency_ms: pick([100, 200, 300]),
    }),
    "Big spenders (amount 50–500)": () => ({ ...RANDOM.events(), amount: money(50, 500) }),
    "Random past timestamps (last 30 days)": () => ({
      ...RANDOM.events(),
      ts: new Date(Date.now() - Math.random() * 30 * 86400e3).toISOString().replace("T", " ").replace("Z", ""),
    }),
    "Rising latency (each one slower)": (() => {
      let n = 0;
      return () => ({ ...RANDOM.events(), latency_ms: 1000 + n++ });
    })(),
  },
  orders: {
    Uniform: RANDOM.orders,
    "Big orders (500–5000)": () => ({ ...RANDOM.orders(), amount: money(500, 5000) }),
  },
  customers: {
    Uniform: RANDOM.customers,
  },
};

// Messages to write by hand, per topic. Functions, so `ts` is the time the preset is picked.
const WRITE_PRESETS = {
  events: [
    ["Slow checkout", () => ({ ts: now(), user_id: "u-slow", country: "PL", path: "/checkout", latency_ms: 1200, amount: 0 })],
    ["Slowest ever (top of latency desc)", () => ({ ts: now(), user_id: "u-slowest", country: "DE", path: "/checkout", latency_ms: 99999, amount: 0 })],
    ["Mid latency (lands mid-list)", () => ({ ts: now(), user_id: "u-mid", country: "PL", path: "/search", latency_ms: 250, amount: 0 })],
    ["Old timestamp (bottom of ts desc)", () => ({ ts: "2020-01-01 00:00:00.000", user_id: "u-old", country: "DE", path: "/", latency_ms: 10, amount: 0 })],
    ["Paid order in DE", () => ({ ts: now(), user_id: "u-buyer", country: "DE", path: "/checkout", latency_ms: 140, amount: 59.9 })],
    ["Numbers as strings", () => ({ ts: now(), user_id: "u-text", country: "FR", path: "/", latency_ms: "750", amount: "19.99" })],
    ["Missing keys (zero values)", () => ({ user_id: "u-partial" })],
    ["Unreadable latency (skipped)", () => ({ ts: now(), user_id: "u-broken", country: "PL", path: "/", latency_ms: "fast", amount: 0 })],
    [
      "Batch of 3",
      () => [
        { ts: now(), user_id: "u-a", country: "PL", path: "/", latency_ms: 600, amount: 0 },
        { ts: now(), user_id: "u-b", country: "US", path: "/cart", latency_ms: 30, amount: 0 },
        { ts: now(), user_id: "u-c", country: "FR", path: "/checkout", latency_ms: 900, amount: 12.5 },
      ],
    ],
  ],
  orders: [
    ["Big order", () => ({ ts: now(), order_id: id("o"), customer_id: "c1", amount: 999.5 })],
    ["Small order", () => ({ ts: now(), order_id: id("o"), customer_id: "c2", amount: 4.2 })],
  ],
  customers: [
    ["New customer", () => ({ ts: now(), customer_id: "c42", name: "Margaret", country: "GB" })],
    ["Rename c1 (a new row)", () => ({ ts: now(), customer_id: "c1", name: "Ada Lovelace", country: "PL" })],
  ],
};

const pretty = (v) => JSON.stringify(v, null, 2);

/** One object, an array of objects, or one object per line (JSON lines). */
function parseMessages(text) {
  const trimmed = text.trim();
  if (!trimmed) throw new Error("nothing to write");
  let values;
  try {
    const v = JSON.parse(trimmed);
    values = Array.isArray(v) ? v : [v];
  } catch (whole) {
    const lines = trimmed.split("\n").filter((l) => l.trim());
    if (lines.length < 2) throw new Error(`invalid JSON: ${whole.message}`);
    values = lines.map((l, i) => {
      try {
        return JSON.parse(l);
      } catch (e) {
        throw new Error(`line ${i + 1}: invalid JSON: ${e.message}`);
      }
    });
  }
  values.forEach((v, i) => {
    if (v === null || typeof v !== "object" || Array.isArray(v)) throw new Error(`message ${i + 1} is not a JSON object`);
  });
  return values;
}

// Produce messages to a Kafka topic through Redpanda's HTTP proxy; returns where they landed.
async function write(topic, values) {
  const res = await fetch(`/kafka/topics/${topic}`, {
    method: "POST",
    headers: { "Content-Type": "application/vnd.kafka.json.v2+json" },
    body: JSON.stringify({ records: values.map((value) => ({ value })) }),
  });
  if (!res.ok) throw new Error(`write to ${topic} failed: ${await res.text()}`);
  const { offsets } = await res.json();
  return offsets.map((o) => `${o.partition}:${o.offset}`);
}

function Writer() {
  const [topic, setTopic] = useState("events");
  const [text, setText] = useState(() => pretty(WRITE_PRESETS.events[0][1]()));
  const [result, setResult] = useState(null); // { ok, message }

  const run = async (values, label) => {
    try {
      const at = await write(topic, values);
      setResult({ ok: true, message: `wrote ${label} to ${topic} (partition:offset ${at.join(", ")})` });
    } catch (e) {
      setResult({ ok: false, message: e.message });
    }
  };
  const writeText = () => {
    let values;
    try {
      values = parseMessages(text);
    } catch (e) {
      setResult({ ok: false, message: e.message });
      return;
    }
    run(values, values.length === 1 ? "1 message" : `${values.length} messages`);
  };
  const switchTopic = (t) => {
    setTopic(t);
    setText(pretty(WRITE_PRESETS[t][0][1]()));
    setResult(null);
  };

  return (
    <section className="writer">
      <h2>Write to Kafka</h2>
      <p>
        {TOPICS.map((t) => (
          <label key={t}>
            <input type="radio" name="topic" checked={topic === t} onChange={() => switchTopic(t)} /> {t}{" "}
          </label>
        ))}
      </p>
      <p>
        {WRITE_PRESETS[topic].map(([label, make]) => (
          <button key={label} onClick={() => setText(pretty(make()))}>
            {label}
          </button>
        ))}
      </p>
      <textarea
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={(e) => (e.metaKey || e.ctrlKey) && e.key === "Enter" && writeText()}
        rows={9}
        spellCheck={false}
      />
      <p>
        <button onClick={writeText}>Write</button> <small>(⌘/Ctrl+Enter; one object, an array, or JSON lines)</small>{" "}
        <button onClick={() => run([RANDOM[topic]()], "1 random message")}>Random</button>{" "}
        <button onClick={() => run(Array.from({ length: 50 }, RANDOM[topic]), "50 random messages")}>50 random</button>
      </p>
      {result && <p className={result.ok ? "ok" : "error"}>{result.message}</p>}
    </section>
  );
}

/** A loop: write a batch of generated rows, sleep, repeat — until stopped. */
function Generator() {
  const [topic, setTopic] = useState("events");
  const [kind, setKind] = useState("Uniform");
  const [batch, setBatch] = useState(1);
  const [intervalMs, setIntervalMs] = useState(500);
  const [running, setRunning] = useState(false);
  const [stats, setStats] = useState({ written: 0, ticks: 0, errors: 0, lastError: null });
  // the loop reads the latest settings through a ref, so they can change while it runs
  const settings = useRef({ topic, kind, batch, intervalMs });
  settings.current = { topic, kind, batch, intervalMs };

  useEffect(() => {
    if (!running) return;
    let stopped = false;
    let timer;
    const tick = async () => {
      const { topic, kind, batch, intervalMs } = settings.current;
      const make = GENERATORS[topic][kind] ?? GENERATORS[topic].Uniform;
      const n = Math.max(1, Number(batch) || 1);
      try {
        await write(topic, Array.from({ length: n }, make));
        setStats((s) => ({ ...s, written: s.written + n, ticks: s.ticks + 1 }));
      } catch (e) {
        setStats((s) => ({ ...s, ticks: s.ticks + 1, errors: s.errors + 1, lastError: e.message }));
      }
      if (!stopped) timer = setTimeout(tick, Math.max(0, Number(intervalMs) || 0));
    };
    tick();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [running]);

  const perSecond = (Math.max(1, Number(batch) || 1) * 1000) / Math.max(10, Number(intervalMs) || 0);
  return (
    <section className="generator">
      <h2>Generator</h2>
      <p>
        <select
          value={topic}
          onChange={(e) => {
            setTopic(e.target.value);
            setKind("Uniform");
          }}
        >
          {TOPICS.map((t) => (
            <option key={t}>{t}</option>
          ))}
        </select>{" "}
        <select value={kind} onChange={(e) => setKind(e.target.value)}>
          {Object.keys(GENERATORS[topic]).map((k) => (
            <option key={k}>{k}</option>
          ))}
        </select>
      </p>
      <p>
        <label>
          rows per write <input type="number" min={1} value={batch} onChange={(e) => setBatch(e.target.value)} />
        </label>{" "}
        <label>
          every <input type="number" min={0} step={50} value={intervalMs} onChange={(e) => setIntervalMs(e.target.value)} /> ms
        </label>{" "}
        <small>(≈ {perSecond.toFixed(perSecond < 10 ? 1 : 0)} rows/s)</small>
      </p>
      <p>
        <button className={running ? "stop" : "start"} onClick={() => setRunning((r) => !r)}>
          {running ? "■ Stop" : "▶ Start"}
        </button>{" "}
        <small>
          {stats.written} written in {stats.ticks} writes
          {stats.errors > 0 && <span className="error"> · {stats.errors} failed: {stats.lastError}</span>}
        </small>
      </p>
    </section>
  );
}

const ago = (t, now) => (t === undefined ? "—" : `${(Math.max(0, now - t) / 1000).toFixed(1)} s ago`);
const kb = (n) => (n < 10_000 ? `${n} B` : n < 10_000_000 ? `${(n / 1024).toFixed(1)} KB` : `${(n / 1048576).toFixed(1)} MB`);

/** One line about a message: its type, and for a diff its changes, e.g. `+@3 -@10`. */
function summarize(msg) {
  if (msg.type === "snapshot") return `snapshot: ${msg.rows.length} rows`;
  if (msg.type === "error") return `error ${msg.code}`;
  const ops = msg.changes.map((c) =>
    c.op === "insert" ? (c.index === undefined ? "+" : `+@${c.index}`) : c.op === "delete" ? `-@${c.index}` : c.op,
  );
  const shown = ops.length > 12 ? [...ops.slice(0, 12), `… ${ops.length - 12} more`] : ops;
  return `diff: ${msg.changes.length} change${msg.changes.length === 1 ? "" : "s"} · ${shown.join(" ")}`;
}

const LOG = 30;
const RATE_WINDOW_MS = 5000;

/** What the client holds for this subscription, and what arrived lately. */
function Debug({ state, sql, shown }) {
  const { stats, rows, status, limit, order, columns } = state;
  const [log, setLog] = useState([]); // newest first
  const [open, setOpen] = useState(null); // index into log
  const [now, setNow] = useState(Date.now());
  const samples = useRef([]); // [time, inserts total]

  useEffect(() => {
    if (!stats.lastMessage) return;
    const at = stats.lastMessageAt;
    setLog((l) => [{ at, msg: stats.lastMessage, applyMs: stats.lastApplyMs, rows: rows.length }, ...l].slice(0, LOG));
    samples.current = [...samples.current, [at, stats.inserts]].filter(([t]) => at - t <= RATE_WINDOW_MS);
  }, [stats.lastMessage]);

  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 500);
    return () => clearInterval(t);
  }, []);

  const recent = samples.current.filter(([t]) => now - t <= RATE_WINDOW_MS);
  const rate =
    recent.length > 1 ? ((recent.at(-1)[1] - recent[0][1]) * 1000) / Math.max(1, now - recent[0][0]) : 0;
  const changesPerDiff = stats.diffs ? ((stats.inserts + stats.deletes) / stats.diffs).toFixed(1) : "—";
  const opened = open !== null ? log[open] : null;

  return (
    <details className="debug" open>
      <summary>Client state</summary>
      <table className="kv">
        <tbody>
          <tr>
            <th>subscription</th>
            <td>
              {stats.id} · {status} · subscribed {stats.subscribes}× ({stats.snapshots} snapshot{stats.snapshots === 1 ? "" : "s"})
            </td>
          </tr>
          <tr>
            <th>rows in state</th>
            <td>
              <b>{rows.length}</b>
              {limit !== undefined && ` / limit ${limit}`} · {shown} rendered · {columns.length} columns
              {order ? ` · ordered by ${order.map((o) => `${o.column} ${o.direction}`).join(", ")}` : " · unordered"}
            </td>
          </tr>
          <tr>
            <th>received</th>
            <td>
              {stats.diffs} diffs · {stats.inserts} inserts · {stats.deletes} deletes · {changesPerDiff} changes/diff ·{" "}
              {kb(stats.bytes)}
            </td>
          </tr>
          <tr>
            <th>live rate</th>
            <td>{rate.toFixed(1)} inserts/s (last 5 s)</td>
          </tr>
          <tr>
            <th>last message</th>
            <td>
              {ago(stats.lastMessageAt, now)}
              {stats.lastApplyMs !== undefined && ` · applied in ${stats.lastApplyMs.toFixed(2)} ms`}
            </td>
          </tr>
        </tbody>
      </table>
      <p>
        <small>Last {LOG} messages, newest first; click one for its JSON.</small>
      </p>
      <ol className="log">
        {log.map((e, i) => (
          <li key={e.at + ":" + i} className={open === i ? "open" : ""} onClick={() => setOpen(open === i ? null : i)}>
            <code>{new Date(e.at).toISOString().slice(11, 23)}</code> {summarize(e.msg)}{" "}
            <small>
              → {e.rows} rows{e.applyMs !== undefined && `, ${e.applyMs.toFixed(2)} ms`}
            </small>
          </li>
        ))}
      </ol>
      {opened && <pre className="json">{truncate(JSON.stringify(opened.msg, null, 2), 20000)}</pre>}
      <p>
        <small>
          sql: <code>{sql}</code>
        </small>
      </p>
    </details>
  );
}

const truncate = (s, n) => (s.length > n ? s.slice(0, n) + `\n… ${s.length - n} more characters` : s);

function Result({ sql }) {
  const state = useLiveQuery(sql);
  const { rows, columns, order, limit, status, error, errorCode } = state;
  // ordered or limited results as they are (a LIMIT window has its newest row last); unordered
  // unlimited ones newest first
  const asIs = order || limit !== undefined;
  const shown = asIs ? rows.slice(0, SHOWN) : rows.slice(-SHOWN).reverse();
  return (
    <section>
      <p>
        <span className={`status ${status}`}>{status}</span> {rows.length} rows
        {rows.length > SHOWN && (asIs ? ` (first ${SHOWN} shown)` : ` (newest ${SHOWN} shown)`)}
        {limit !== undefined && <small> · limit {limit}{order ? " (top)" : " (latest)"}</small>}
        {order && (
          <small>
            {" "}
            · ordered by {order.map((o) => `${o.column} ${o.direction}${o.nulls === "first" ? " nulls first" : ""}`).join(", ")}
          </small>
        )}
      </p>
      {error && (
        <pre className="error">
          {errorCode}: {error}
        </pre>
      )}
      <Debug state={state} sql={sql} shown={shown.length} />
      <table>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.name} title={c.type}>
                {c.name}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {shown.map((r, i) => (
            <tr key={i}>
              {columns.map((c) => (
                <td key={c.name}>{String(r[c.name] ?? "")}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}

function Query() {
  const [draft, setDraft] = useState(QUERY_PRESETS[0]);
  const [sql, setSql] = useState(QUERY_PRESETS[0]);
  const run = (q) => {
    setDraft(q);
    setSql(q);
  };
  return (
    <section className="query">
      <h2>Live query</h2>
      <p>
        {QUERY_PRESETS.map((q) => (
          <button key={q} className="preset" onClick={() => run(q)}>
            {q}
          </button>
        ))}
      </p>
      <textarea
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onKeyDown={(e) => (e.metaKey || e.ctrlKey) && e.key === "Enter" && run(draft)}
        rows={3}
        spellCheck={false}
      />
      <p>
        <button onClick={() => run(draft)}>Run</button> <small>(⌘/Ctrl+Enter)</small>
      </p>
      {/* a new key per query: its result starts from a fresh snapshot */}
      <Result key={sql} sql={sql} />
    </section>
  );
}

createRoot(document.getElementById("root")).render(
  <>
    <h1>chlive playground</h1>
    <div className="panes">
      <Query />
      <div className="side">
        <Writer />
        <Generator />
      </div>
    </div>
  </>,
);
