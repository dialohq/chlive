// The chat demo: one group chat, two users side by side, every list live through chlive. Each
// pane is its own connection, as its user: chlive shows it what that user may read, per WorkOS
// (polled) or Postgres (followed live): whether they may read the messages, whose, and (Postgres
// only) which columns of the users. Messages go straight to Kafka through Redpanda's HTTP proxy.
import { StrictMode, useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { LiveClient, defaultUrl } from "@chlive/client";
import { LiveClientProvider, useLiveQuery } from "@chlive/client/react";

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
  // a subscription that lost access is over: mounting the chat again subscribes again
  const [attempt, setAttempt] = useState(0);
  if (!userClient) return <section className="pane" />;
  return (
    <LiveClientProvider client={userClient}>
      <Chat key={attempt} {...props} retry={() => setAttempt((n) => n + 1)} />
    </LiveClientProvider>
  );
}

/**
 * What the user is permitted to see: the users, with the columns they may read (`color`, or
 * names in plain text), the messages of the authors they may see (if they may read messages at
 * all), and a composer.
 */
function Chat({ userId, setUserId, retry }) {
  const users = useLiveQuery("select * from users final order by user_id");
  const messages = useLiveQuery(
    "select ts, user_id, body, message_id from messages order by ts desc limit 200",
  );
  const names = Object.fromEntries(users.rows.map((u) => [u.user_id, u]));

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
          {(users.rows.length ? users.rows : [{ user_id: userId, name: userId }]).map((u) => (
            <option key={u.user_id} value={u.user_id}>
              {u.name}
            </option>
          ))}
        </select>
      </div>
      {messages.status === "error" ? (
        <div className="messages denied">
          No access to messages: {messages.error?.message}{" "}
          <button type="button" onClick={retry}>
            Retry
          </button>
        </div>
      ) : (
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
      )}
      {error && <div className="error">{error}</div>}
      <form className="compose" onSubmit={send}>
        <input
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder={`Message as ${names[userId]?.name ?? userId}`}
        />
        <button type="submit">Send</button>
      </form>
    </section>
  );
}

function App() {
  const [panes, setPanes] = useState(["alice", "bob"]);
  const setPane = (i) => (id) => setPanes((p) => p.map((x, j) => (j === i ? id : x)));
  return (
    <>
      <header>
        <h1>chlive chat</h1>
        <p>
          Every list is a live query, as the pane's user. WorkOS says what each user may read: the messages if their
          role in the organization has <code>messages:read</code>, and those of the feeds they hold{" "}
          <code>feed:read</code> on (with <code>CHAT_SECURITY=postgres</code>, Postgres tables say it instead).
        </p>
      </header>
      <main className="panes">
        {panes.map((id, i) => (
          <Pane key={i} userId={id} setUserId={setPane(i)} />
        ))}
      </main>
    </>
  );
}

createRoot(document.getElementById("root")).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
