// The chat stack end to end, through chlive (`chat-smoke` in the chat shell, with `dnvr up`):
// a message produced to Kafka comes into the chat's live query, and the users are there.
import { LiveClient, type LiveState, type Row } from "../../client/src/index.ts";

const url = process.env.CHLIVE_URL ?? "ws://127.0.0.1:8080/live";
const proxy = process.env.PROXY_URL ?? "http://127.0.0.1:8082";
const client = new LiveClient({ url, cache: false });

async function produce(topic: string, value: unknown) {
  const res = await fetch(`${proxy}/topics/${topic}`, {
    method: "POST",
    headers: { "Content-Type": "application/vnd.kafka.json.v2+json" },
    body: JSON.stringify({ records: [{ value }] }),
  });
  const { offsets } = (await res.json()) as { offsets: { error_code?: number }[] };
  if (!res.ok || offsets.some((o) => o.error_code)) throw new Error(`produce to ${topic} failed`);
}

/** Resolves once a live query's state satisfies `ok`; fails after `ms`. */
function until(sql: string, params: Record<string, unknown>, ok: (s: LiveState<Row>) => boolean, what: string, ms = 20_000) {
  return new Promise<LiveState<Row>>((resolve, reject) => {
    const timer = setTimeout(() => {
      sub.unsubscribe();
      reject(new Error(`timed out: ${what}`));
    }, ms);
    const sub = client.subscribe(
      sql,
      (s) => {
        if (s.status === "error") {
          clearTimeout(timer);
          reject(new Error(`${what}: ${s.error?.message}`));
        } else if (s.status === "live" && ok(s)) {
          clearTimeout(timer);
          queueMicrotask(() => sub.unsubscribe());
          resolve(s);
        }
      },
      { params },
    );
  });
}

// a message
const body = `smoke ${crypto.randomUUID()}`;
const ts = new Date().toISOString().replace("T", " ").replace("Z", "");
await produce("messages", { ts, message_id: crypto.randomUUID(), user_id: "alice", body });
await until(
  "select body from messages order by ts desc limit 50",
  {},
  (s) => s.rows.some((r) => r.body === body),
  "the message in the chat",
);
console.log("ok: the message came into the chat");

// the users, with their names
await until(
  "select user_id, name from users final",
  {},
  (s) => s.rows.length >= 4 && s.rows.some((r) => r.name === "Alice"),
  "the users",
);
console.log("ok: the users are there");

client.close();
console.log("chat smoke: all ok");
