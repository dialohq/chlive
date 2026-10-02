import { useState } from "react";
import { createRoot } from "react-dom/client";
import { useLiveQuery } from "@chlive/client/react";

// Queries in the chlive dialect: SELECT * | columns FROM table [WHERE condition].
const QUERY_PRESETS = [
  "select ts, user_id, country, path, latency_ms from events where latency_ms > 500",
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

// Messages to write by hand, per topic. Functions, so `ts` is the time the preset is picked.
const WRITE_PRESETS = {
  events: [
    ["Slow checkout", () => ({ ts: now(), user_id: "u-slow", country: "PL", path: "/checkout", latency_ms: 1200, amount: 0 })],
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

function Result({ sql }) {
  const { rows, columns, status, error, errorCode } = useLiveQuery(sql);
  const newest = rows.slice(-SHOWN).reverse();
  return (
    <section>
      <p>
        <span className={`status ${status}`}>{status}</span> {rows.length} rows
        {rows.length > SHOWN && ` (newest ${SHOWN} shown)`}
      </p>
      {error && (
        <pre className="error">
          {errorCode}: {error}
        </pre>
      )}
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
          {newest.map((r, i) => (
            <tr key={rows.length - i}>
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
      <Writer />
    </div>
  </>,
);
