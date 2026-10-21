// The chat stack's smoke tests' helpers: clients as a user, live queries followed, messages
// produced to Kafka.
import { LiveClient, type LiveState, type Row } from "../../client/src/index.ts";

const url = process.env.CHLIVE_URL ?? "ws://127.0.0.1:8080/live";
const proxy = process.env.PROXY_URL ?? "http://127.0.0.1:8082";

const clients: LiveClient[] = [];
export const as = (user?: string) => {
  const c = new LiveClient({ url, user, cache: false });
  clients.push(c);
  return c;
};

export async function produce(topic: string, value: unknown) {
  const res = await fetch(`${proxy}/topics/${topic}`, {
    method: "POST",
    headers: { "Content-Type": "application/vnd.kafka.json.v2+json" },
    body: JSON.stringify({ records: [{ value }] }),
  });
  const { offsets } = (await res.json()) as { offsets: { error_code?: number }[] };
  if (!res.ok || offsets.some((o) => o.error_code)) throw new Error(`produce to ${topic} failed`);
}

/** A live query kept open: its latest state, and a wait for one that satisfies `ok`. */
export function follow(client: LiveClient, sql: string) {
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
    /** Resolves with the error once it fails (with a message containing `text`). */
    fails(text: string, what: string, ms = 20_000) {
      return new Promise<string>((resolve, reject) => {
        const check = () => {
          if (state?.status !== "error") return;
          const message = state.error?.message ?? "";
          done(message.includes(text) ? undefined : new Error(`${what}: ${message}`), message);
        };
        const timer = setTimeout(() => done(new Error(`timed out: ${what}`)), ms);
        const done = (e?: Error, message = "") => {
          clearTimeout(timer);
          waiting.delete(check);
          e ? reject(e) : resolve(message);
        };
        waiting.add(check);
        check();
      });
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

export const MESSAGES = "select body, user_id from messages order by ts desc limit 200";
export const has = (body: string) => (s: LiveState<Row>) => s.rows.some((r) => r.body === body);

/** A message from `user`, produced to Kafka; its body. */
export async function message(user: string) {
  const body = `smoke ${crypto.randomUUID()}`;
  const ts = new Date().toISOString().replace("T", " ").replace("Z", "");
  await produce("messages", { ts, message_id: crypto.randomUUID(), user_id: user, body });
  return body;
}

export function closeAll() {
  for (const c of clients) c.close();
}
