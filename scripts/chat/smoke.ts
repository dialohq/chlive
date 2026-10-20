// The chat stack end to end, through chlive (`chat-smoke` in the chat shell, with `dnvr up`):
// each user sees the messages of the authors Postgres permits them to see, and a permission
// granted or revoked there shows live; the users are public; anonymous connections see no
// messages.
import { LiveClient, type LiveState, type Row } from "../../client/src/index.ts";

const url = process.env.CHLIVE_URL ?? "ws://127.0.0.1:8080/live";
const proxy = process.env.PROXY_URL ?? "http://127.0.0.1:8082";
const pg = process.env.PG_URL ?? "postgres://postgres@127.0.0.1:54329/chat";

const clients: LiveClient[] = [];
const as = (user?: string) => {
  const c = new LiveClient({ url, user, cache: false });
  clients.push(c);
  return c;
};

async function produce(topic: string, value: unknown) {
  const res = await fetch(`${proxy}/topics/${topic}`, {
    method: "POST",
    headers: { "Content-Type": "application/vnd.kafka.json.v2+json" },
    body: JSON.stringify({ records: [{ value }] }),
  });
  const { offsets } = (await res.json()) as { offsets: { error_code?: number }[] };
  if (!res.ok || offsets.some((o) => o.error_code)) throw new Error(`produce to ${topic} failed`);
}

function psql(sql: string) {
  const p = Bun.spawnSync(["psql", pg, "-v", "ON_ERROR_STOP=1", "-qc", sql]);
  if (p.exitCode !== 0) throw new Error(`psql: ${p.stderr.toString()}`);
}

/** A live query kept open: its latest state, and a wait for one that satisfies `ok`. */
function follow(client: LiveClient, sql: string) {
  let state: LiveState<Row> | undefined;
  const waiting = new Set<() => void>();
  const sub = client.subscribe(sql, (s) => {
    state = s;
    for (const w of waiting) w();
  });
  return {
    get state() {
      return state;
    },
    until(ok: (s: LiveState<Row>) => boolean, what: string, ms = 20_000) {
      return new Promise<LiveState<Row>>((resolve, reject) => {
        const check = () => {
          if (!state) return;
          if (state.status === "error") done(new Error(`${what}: ${state.error?.message}`));
          else if (state.status === "live" && ok(state)) done();
        };
        const timer = setTimeout(() => done(new Error(`timed out: ${what}`)), ms);
        const done = (e?: Error) => {
          clearTimeout(timer);
          waiting.delete(check);
          e ? reject(e) : resolve(state!);
        };
        waiting.add(check);
        check();
      });
    },
    close: () => sub.unsubscribe(),
  };
}

const MESSAGES = "select body, user_id from messages order by ts desc limit 200";
const has = (body: string) => (s: LiveState<Row>) => s.rows.some((r) => r.body === body);

// a fresh start: Alice may not see Carol (the seed's permissions)
psql("delete from permissions where viewer_id = 'alice' and author_id = 'carol'");

const alice = follow(as("alice"), MESSAGES);
const bob = follow(as("bob"), MESSAGES);
await alice.until(() => true, "alice's messages");
await bob.until(() => true, "bob's messages");

// a message from Carol: Bob is permitted to see her, Alice isn't
const body = `smoke ${crypto.randomUUID()}`;
const ts = new Date().toISOString().replace("T", " ").replace("Z", "");
await produce("messages", { ts, message_id: crypto.randomUUID(), user_id: "carol", body });
await bob.until(has(body), "Carol's message in Bob's chat");
await Bun.sleep(500);
if (has(body)(alice.state!)) throw new Error("Alice sees Carol's message without permission");
if (alice.state!.rows.some((r) => r.user_id === "carol")) throw new Error("Alice sees Carol's messages");
console.log("ok: Bob sees Carol's message, Alice doesn't");

// granted in Postgres: it comes into Alice's chat
psql("insert into permissions values ('alice', 'carol')");
await alice.until(has(body), "Carol's message in Alice's chat after the grant");
console.log("ok: granted, Alice sees it");

// revoked: it goes
psql("delete from permissions where viewer_id = 'alice' and author_id = 'carol'");
await alice.until((s) => !s.rows.some((r) => r.user_id === "carol"), "Carol's messages gone from Alice's chat");
console.log("ok: revoked, it's gone");

// the users are public; messages need a user
await follow(as(), "select user_id, name from users final").until(
  (s) => s.rows.length >= 4 && s.rows.some((r) => r.name === "Alice"),
  "the users, anonymously",
);
console.log("ok: the users are there");
const denied = follow(as(), MESSAGES);
await denied.until(() => true, "an anonymous chat").then(
  () => {
    throw new Error("an anonymous connection reads messages");
  },
  (e: Error) => {
    if (!e.message.includes("needs a user")) throw e;
  },
);
console.log("ok: anonymous connections see no messages");

for (const c of clients) c.close();
console.log("chat smoke: all ok");
