import type { ClientMessage, Column, Row, ServerMessage } from "./protocol.ts";
import { ResultStore } from "./state.ts";

export type * from "./protocol.ts";
export { ResultStore } from "./state.ts";

export type Status = "loading" | "live" | "error";

export interface LiveState<R extends Row = Row> {
  status: Status;
  rows: R[];
  columns: Column[];
  error?: string;
  errorCode?: string;
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
      state: { status: "loading", rows: [], columns: [] },
      listener,
    };
    this.subs.set(id, rec);
    listener(rec.state);
    this.ensureConnected();
    this.send({ type: "subscribe", id, sql });
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

  private send(msg: ClientMessage): void {
    // While disconnected, subscriptions are (re)sent from `onopen`.
    if (this.ws?.readyState === OPEN) this.ws.send(JSON.stringify(msg));
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
      }
    };
    ws.onmessage = (ev: MessageEvent) => this.onMessage(JSON.parse(String(ev.data)) as ServerMessage);
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

  private onMessage(msg: ServerMessage): void {
    const rec = msg.id ? this.subs.get(msg.id) : undefined;
    if (!rec) return;
    switch (msg.type) {
      case "snapshot":
        rec.store.applySnapshot(msg);
        break;
      case "diff":
        try {
          rec.store.applyDiff(msg);
        } catch (e) {
          this.emit(rec, { ...rec.state, status: "error", error: String(e), errorCode: "CLIENT_ERROR" });
          return;
        }
        break;
      case "error":
        this.emit(rec, { ...rec.state, status: "error", error: msg.message, errorCode: msg.code });
        return;
    }
    this.emit(rec, { status: "live", rows: rec.store.rows(), columns: rec.store.columns });
  }

  private emit(rec: SubRecord, state: LiveState<any>): void {
    rec.state = state;
    rec.listener(state);
  }
}

/** `ws(s)://<current host>/live` in a browser. */
export function defaultUrl(path = "/live"): string {
  const loc = (globalThis as { location?: Location }).location;
  if (!loc) throw new Error("no location; pass an explicit url");
  return `${loc.protocol === "https:" ? "wss" : "ws"}://${loc.host}${path}`;
}
