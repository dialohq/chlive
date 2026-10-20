// The chat demo: one group chat, two users side by side, every list live through chlive. Each
// pane is its own connection, as its user: chlive shows it the messages of the authors that user
// is permitted to see (the `permissions` table in Postgres, followed live). Messages go straight
// to Kafka through Redpanda's HTTP proxy.
import { StrictMode, useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { LiveClient, defaultUrl } from "@chlive/client";
import { LiveClientProvider, useLiveQuery } from "@chlive/client/react";

// the users list is public: read anonymously
const client = new LiveClient({ url: defaultUrl() });

/** A connection as `user` (one per pane; they share the page, so no offline cache), closed
 * when the pane switches user or goes. */
function useUserClient(user) {
  const [c, setC] = useState(null);
  useEffect(() => {
    const c = new LiveClient({ url: defaultUrl(), user, cache: false });
    setC(c);
    return () => c.close();
  }, [user]);
  return c;
}

/** Write messages (JSON values) to a Kafka topic. */
async function produce(topic, values) {
  const res = await fetch(`/kafka/topics/${topic}`, {
    method: "POST",
    headers: { "Content-Type": "application/vnd.kafka.json.v2+json" },
    body: JSON.stringify({ records: values.map((value) => ({ value })) }),
  });
  if (!res.ok) throw new Error(`${topic}: HTTP ${res.status}`);
  const { offsets } = await res.json();
  // the proxy answers 200 even when a record fails
  const failed = offsets.find((o) => o.error_code);
  if (failed) throw new Error(`${topic}: Kafka error ${failed.error_code}`);
}

/** Now, as ClickHouse reads a DateTime64(3) (UTC). */
const utcNow = () => new Date().toISOString().replace("T", " ").replace("Z", "");

const time = (d) => d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

function StatusDot({ query }) {
  return <span className={`dot ${query.status}`} title={query.error?.message ?? query.status} />;
}

/** One user's view of the chat, through a connection as that user. */
function Pane(props) {
  const userClient = useUserClient(props.userId);
  if (!userClient) return <section className="pane" />;
  return (
    <LiveClientProvider client={userClient}>
      <Chat {...props} />
    </LiveClientProvider>
  );
}

/** What the user is permitted to see of the chat, and a composer. */
function Chat({ userId, setUserId, users }) {
  const me = users.find((u) => u.user_id === userId);
  const messages = useLiveQuery(
    "select ts, user_id, body, message_id from messages order by ts desc limit 200",
  );
  const names = Object.fromEntries(users.map((u) => [u.user_id, u]));

  // keep the newest message in view
  const list = useRef(null);
  useEffect(() => {
    list.current?.scrollTo({ top: list.current.scrollHeight });
  }, [messages.rows]);

  const [draft, setDraft] = useState("");
  const [error, setError] = useState("");
  async function send(e) {
    e.preventDefault();
    const body = draft.trim();
    if (!body) return;
    setDraft("");
    try {
      await produce("messages", [{ ts: utcNow(), message_id: crypto.randomUUID(), user_id: userId, body }]);
      setError("");
    } catch (err) {
      setError(String(err.message ?? err));
    }
  }

  return (
    <section className="pane">
      <div className="pane-top">
        <StatusDot query={messages} />
        <select value={userId} onChange={(e) => setUserId(e.target.value)}>
          {users.map((u) => (
            <option key={u.user_id} value={u.user_id}>
              {u.name}
            </option>
          ))}
        </select>
      </div>
      <div className="messages" ref={list}>
        {[...messages.rows].reverse().map((m) => (
          <div key={m.message_id} className="msg">
            <span className="when">{time(m.ts)}</span>
            <span className="who" style={{ color: names[m.user_id]?.color }}>
              {names[m.user_id]?.name ?? m.user_id}
            </span>
            {m.body}
          </div>
        ))}
      </div>
      {error && <div className="error">{error}</div>}
      <form className="compose" onSubmit={send}>
        <input value={draft} onChange={(e) => setDraft(e.target.value)} placeholder={`Message as ${me?.name ?? ""}`} />
        <button type="submit">Send</button>
      </form>
    </section>
  );
}

function App() {
  const users = useLiveQuery("select user_id, name, color from users final order by user_id");
  const [panes, setPanes] = useState(["alice", "bob"]);
  const setPane = (i) => (id) => setPanes((p) => p.map((x, j) => (j === i ? id : x)));
  return (
    <>
      <header>
        <h1>chlive chat</h1>
        <p>
          Every list is a live query. Each user sees the messages of the authors they're permitted to see: the{" "}
          <code>permissions</code> table in Postgres (change it with <code>pg</code>).
        </p>
      </header>
      <main className="panes">
        {users.rows.length > 0 &&
          panes.map((id, i) => <Pane key={i} userId={id} setUserId={setPane(i)} users={users.rows} />)}
      </main>
    </>
  );
}

createRoot(document.getElementById("root")).render(
  <StrictMode>
    <LiveClientProvider client={client}>
      <App />
    </LiveClientProvider>
  </StrictMode>,
);
