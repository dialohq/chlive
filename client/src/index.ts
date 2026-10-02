import type { ClientMessage, Column, OrderTerm, Row, ServerMessage } from "./protocol.ts";
import { ResultStore } from "./state.ts";

export type * from "./protocol.ts";
export { ResultStore } from "./state.ts";

export type Status = "loading" | "live" | "error";

export interface LiveState<R extends Row = Row> {
  status: Status;
  rows: R[];
  columns: Column[];
  /** The ORDER BY the rows are in; absent for unordered queries (rows in arrival order). */
  order?: OrderTerm[];
  /** The query's LIMIT, if any. `rows` never has more: listeners only see whole diffs applied. */
  limit?: number;
  error?: string;
  errorCode?: string;
  /** What this subscription has received so far; for debugging and dev tools. */
  stats: SubscriptionStats;
}

/** Counters and the latest message of one subscription. A new object whenever it changes. */
export interface SubscriptionStats {
  /** The id on the wire (`s1`, `s2`, …). */
  id: string;
  /** `subscribe` messages sent: 1, plus one per reconnect. */
  subscribes: number;
  snapshots: number;
  diffs: number;
  inserts: number;
  deletes: number;
  /** Bytes of JSON received for this subscription. */
  bytes: number;
  /** The last message received for it, as parsed. */
  lastMessage?: ServerMessage;
  /** `Date.now()` when it arrived. */
  lastMessageAt?: number;
  /** How long applying it to the rows took, in milliseconds. */
  lastApplyMs?: number;
}

export type Listener<R extends Row = Row> = (state: LiveState<R>) => void;

export interface Subscription {
  unsubscribe(): void;
}

export interface LiveClientOptions {
  /** e.g. `ws://localhost:8080/live` */
  url: string;
  /** Initial reconnect delay; doubles up to `maxBackoffMs`. */
  minBackoffMs?: number;
  maxBackoffMs?: number;
  /** WebSocket implementation (defaults to the global one). */
  WebSocket?: new (url: string) => WebSocket;
}

interface SubRecord {
  sql: string;
  store: ResultStore<any>;
  state: LiveState<any>;
  stats: SubscriptionStats;
  listener: Listener<any>;
}

const OPEN = 1;

/**
 * One WebSocket shared by any number of live queries. Reconnects with exponential backoff and
 * resubscribes every active query; the fresh snapshot replaces local state.
 */
export class LiveClient {
  private readonly opts: Required<Omit<LiveClientOptions, "WebSocket">> & Pick<LiveClientOptions, "WebSocket">;
  private ws: WebSocket | null = null;
  private subs = new Map<string, SubRecord>();
  private nextId = 1;
  private backoff: number;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  constructor(opts: LiveClientOptions) {
    this.opts = { minBackoffMs: 500, maxBackoffMs: 10_000, ...opts };
    this.backoff = this.opts.minBackoffMs;
  }

  subscribe<R extends Row = Row>(sql: string, listener: Listener<R>): Subscription {
    if (this.closed) throw new Error("LiveClient is closed");
    const id = `s${this.nextId++}`;
    const rec: SubRecord = {
      sql,
      store: new ResultStore<R>(),
      state: { status: "loading", rows: [], columns: [], stats: newStats(id) },
      stats: newStats(id),
      listener,
    };
    this.subs.set(id, rec);
    listener(rec.state);
    this.ensureConnected();
    if (this.send({ type: "subscribe", id, sql })) this.countSubscribe(rec);
    let active = true;
    return {
      unsubscribe: () => {
        if (!active) return;
        active = false;
        this.subs.delete(id);
        this.send({ type: "unsubscribe", id });
      },
    };
  }

  /** Close the socket and stop reconnecting. */
  close(): void {
    this.closed = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.subs.clear();
    this.ws?.close();
    this.ws = null;
  }

  /** Whether it was sent. While disconnected, subscriptions are (re)sent from `onopen`. */
  private send(msg: ClientMessage): boolean {
    if (this.ws?.readyState !== OPEN) return false;
    this.ws.send(JSON.stringify(msg));
    return true;
  }

  private countSubscribe(rec: SubRecord): void {
    rec.stats = { ...rec.stats, subscribes: rec.stats.subscribes + 1 };
  }

  private ensureConnected(): void {
    if (this.ws || this.closed || this.reconnectTimer) return;
    const Impl = this.opts.WebSocket ?? globalThis.WebSocket;
    const ws = new Impl(this.opts.url);
    this.ws = ws;
    ws.onopen = () => {
      this.backoff = this.opts.minBackoffMs;
      for (const [id, rec] of this.subs) {
        ws.send(JSON.stringify({ type: "subscribe", id, sql: rec.sql } satisfies ClientMessage));
        this.countSubscribe(rec);
      }
    };
    ws.onmessage = (ev: MessageEvent) => {
      const text = String(ev.data);
      this.onMessage(JSON.parse(text) as ServerMessage, text.length);
    };
    ws.onclose = () => {
      if (this.ws !== ws) return;
      this.ws = null;
      for (const rec of this.subs.values()) {
        if (rec.state.status === "live") this.emit(rec, { ...rec.state, status: "loading" });
      }
      this.scheduleReconnect();
    };
    ws.onerror = () => {
      // `onclose` follows and handles reconnection.
    };
  }

  private scheduleReconnect(): void {
    if (this.closed || this.reconnectTimer) return;
    const delay = this.backoff;
    this.backoff = Math.min(this.backoff * 2, this.opts.maxBackoffMs);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.subs.size > 0) this.ensureConnected();
    }, delay);
  }

  private onMessage(msg: ServerMessage, bytes: number): void {
    const rec = msg.id ? this.subs.get(msg.id) : undefined;
    if (!rec) return;
    const started = performance.now();
    const stats: SubscriptionStats = {
      ...rec.stats,
      bytes: rec.stats.bytes + bytes,
      lastMessage: msg,
      lastMessageAt: Date.now(),
    };
    try {
      switch (msg.type) {
        case "snapshot":
          rec.store.applySnapshot(msg);
          stats.snapshots++;
          break;
        case "diff":
          rec.store.applyDiff(msg);
          stats.diffs++;
          for (const c of msg.changes) {
            if (c.op === "insert") stats.inserts++;
            else if (c.op === "delete") stats.deletes++;
          }
          break;
        case "error":
          rec.stats = stats;
          this.emit(rec, { ...rec.state, status: "error", error: msg.message, errorCode: msg.code });
          return;
      }
    } catch (e) {
      rec.stats = stats;
      this.emit(rec, { ...rec.state, status: "error", error: String(e), errorCode: "CLIENT_ERROR" });
      return;
    }
    stats.lastApplyMs = performance.now() - started;
    rec.stats = stats;
    this.emit(rec, {
      status: "live",
      rows: rec.store.rows(),
      columns: rec.store.columns,
      order: rec.store.order,
      limit: rec.store.limit,
      stats,
    });
  }

  private emit(rec: SubRecord, state: LiveState<any>): void {
    rec.state = { ...state, stats: rec.stats };
    rec.listener(rec.state);
  }
}

function newStats(id: string): SubscriptionStats {
  return { id, subscribes: 0, snapshots: 0, diffs: 0, inserts: 0, deletes: 0, bytes: 0 };
}

/** `ws(s)://<current host>/live` in a browser. */
export function defaultUrl(path = "/live"): string {
  const loc = (globalThis as { location?: Location }).location;
  if (!loc) throw new Error("no location; pass an explicit url");
  return `${loc.protocol === "https:" ? "wss" : "ws"}://${loc.host}${path}`;
}
