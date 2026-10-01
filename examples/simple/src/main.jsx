import { useState } from "react";
import { createRoot } from "react-dom/client";
import { useLiveQuery } from "@chlive/client/react";

// Queries in the chlive dialect: SELECT * | columns FROM table [WHERE condition].
const PRESETS = [
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
const WRITERS = {
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

// Produce rows to a Kafka topic through Redpanda's HTTP proxy.
async function write(topic, n) {
  const records = Array.from({ length: n }, () => ({ value: WRITERS[topic]() }));
  const res = await fetch(`/kafka/topics/${topic}`, {
    method: "POST",
    headers: { "Content-Type": "application/vnd.kafka.json.v2+json" },
    body: JSON.stringify({ records }),
  });
  return res.ok ? null : `write to ${topic} failed: ${await res.text()}`;
}

function Writers() {
  const [error, setError] = useState(null);
  return (
    <p>
      {Object.keys(WRITERS).map((topic) => (
        <span key={topic}>
          <button onClick={async () => setError(await write(topic, 1))}>Write 1 to {topic}</button>{" "}
          <button onClick={async () => setError(await write(topic, 50))}>50</button>{"  "}
        </span>
      ))}
      {error && <span className="error">{error}</span>}
    </p>
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

function Playground() {
  const [draft, setDraft] = useState(PRESETS[0]);
  const [sql, setSql] = useState(PRESETS[0]);
  const run = (q) => {
    setDraft(q);
    setSql(q);
  };
  return (
    <>
      <p>
        {PRESETS.map((q) => (
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
      <Writers />
      {/* a new key per query: its result starts from a fresh snapshot */}
      <Result key={sql} sql={sql} />
    </>
  );
}

createRoot(document.getElementById("root")).render(
  <>
    <h1>chlive playground</h1>
    <Playground />
  </>,
);
