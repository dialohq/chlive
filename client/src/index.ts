import type { ClientMessage, Column, DiffMessage, OrderTerm, Row, ServerMessage } from "./protocol.ts";
import { type CachedResult, ResultStore } from "./state.ts";

export type * from "./protocol.ts";
export { ResultStore, type CachedResult } from "./state.ts";

/**
 * - `loading`: nothing to show yet;
 * - `cached`: rows from the cache, while the server catches them up;
 * - `live`: up to date, and following changes;
 * - `error`: see `error` and `errorCode`.
 */
export type Status = "loading" | "cached" | "live" | "error";

export interface LiveState<R extends Row = Row> {
  status: Status;
  rows: R[];
  columns: Column[];
  /** The ORDER BY the rows are in; absent for unordered queries (rows in arrival order). */
  order?: OrderTerm[];
  /** The query's LIMIT, if any. `rows` never has more: listeners only see whole diffs applied. */
  limit?: number;
  /** When the cached rows were saved (while `status` is `cached`). */
  cachedAt?: number;
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
  /** The cursor the rows are complete to. */
  at?: string;
  /** With a cache: what it did for this subscription. */
  cache?: CacheStats;
}

export interface CacheStats {
  /** Whether a cached result was found. */
  hit: boolean;
  loadedRows: number;
  loadMs: number;
  /** When the cached rows were saved. */
  savedAt?: number;
  /** Whether the server resumed from the cache (rather than sending a full snapshot). */
  resumed?: boolean;
  /** Changes in the catch-up after the resume. */
  catchUp?: number;
  saves: number;
  lastSaveMs?: number;
}

/**
 * Where results are kept between sessions (e.g. `@chlive/client/cache`'s SQLite on OPFS).
 * Keyed by query. Every write keeps the rows and their cursor together.
 */
export interface ResultCache {
  /** The cached result, marking it as just used; `undefined` if there's none. */
  load(key: string): Promise<CachedResult | undefined>;
  /** Replace the whole result. */
  save(key: string, result: CachedResult): Promise<void>;
  /** Append rows to the end of the result (unordered queries' common case), with the new cursor. */
  append(key: string, rows: Row[], at: string): Promise<void>;
  /** The rows are unchanged; only the cursor moved. */
  setCursor(key: string, at: string): Promise<void>;
  remove(key: string): Promise<void>;
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
  /** Keep results between sessions: show them right away, then resume from their cursor. */
  cache?: ResultCache;
  /** Full saves of a result are batched for this long after a change (default 1000 ms). */
  saveDelayMs?: number;
}

interface SubRecord {
  id: string;
  sql: string;
  store: ResultStore<any>;
  state: LiveState<any>;
  stats: SubscriptionStats;
  listener: Listener<any>;
  /** False while the cache is being read: the subscription isn't sent until then. */
  ready: boolean;
  /** Writes to the cache, one after another. */
  persist: Promise<void>;
  /** A full save is scheduled (appends and cursor updates wait for it). */
  saveTimer: ReturnType<typeof setTimeout> | null;
  /** Whether the cache has a base to append to (a saved result for this cursor). */
  saved: boolean;
  /** Waiting for the first diff after a resume (the catch-up). */
  awaitingCatchUp: boolean;
}

const OPEN = 1;

/**
 * One WebSocket shared by any number of live queries. Reconnects with exponential backoff and
 * resubscribes every active query from the cursor it has.
 *
 * With a `cache`, a subscription first shows the cached result (`status: "cached"`) and asks
 * the server to resume from its cursor; the server sends only what changed since (or a full
 * snapshot if it can't resume). Results are saved back as they change.
 */
export class LiveClient {
  private readonly opts: Required<Omit<LiveClientOptions, "WebSocket" | "cache">> &
    Pick<LiveClientOptions, "WebSocket" | "cache">;
  private ws: WebSocket | null = null;
  private subs = new Map<string, SubRecord>();
  private nextId = 1;
  private backoff: number;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  constructor(opts: LiveClientOptions) {
    this.opts = { minBackoffMs: 500, maxBackoffMs: 10_000, saveDelayMs: 1000, ...opts };
    this.backoff = this.opts.minBackoffMs;
  }

  subscribe<R extends Row = Row>(sql: string, listener: Listener<R>): Subscription {
    if (this.closed) throw new Error("LiveClient is closed");
    const id = `s${this.nextId++}`;
    const rec: SubRecord = {
      id,
      sql,
      store: new ResultStore<R>(),
      state: { status: "loading", rows: [], columns: [], stats: newStats(id) },
      stats: newStats(id),
      listener,
      ready: !this.opts.cache,
      persist: Promise.resolve(),
      saveTimer: null,
      saved: false,
      awaitingCatchUp: false,
    };
    this.subs.set(id, rec);
    listener(rec.state);
    this.ensureConnected();
    if (this.opts.cache) {
      const started = performance.now();
      this.opts.cache
        .load(sql)
        .catch((e) => {
          console.warn("chlive: reading the cache failed", e);
          return undefined;
        })
        .then((cached) => {
          if (this.subs.get(id) !== rec) return; // unsubscribed meanwhile
          rec.stats = {
            ...rec.stats,
            at: cached?.at,
            cache: {
              hit: !!cached,
              loadedRows: cached?.rows.length ?? 0,
              loadMs: performance.now() - started,
              savedAt: cached?.savedAt,
              saves: 0,
            },
          };
          if (cached) {
            rec.store.applyCached(cached);
            rec.saved = true;
            this.emit(rec, { ...this.view(rec), status: "cached", cachedAt: cached.savedAt });
          }
          rec.ready = true;
          this.sendSubscribe(rec);
        });
    } else {
      this.sendSubscribe(rec);
    }
    let active = true;
    return {
      unsubscribe: () => {
        if (!active) return;
        active = false;
        this.flushSave(rec);
        this.subs.delete(id);
        this.send({ type: "unsubscribe", id });
      },
    };
  }

  /** Write every pending result to the cache now (e.g. when the page is being hidden). */
  flush(): void {
    for (const rec of this.subs.values()) this.flushSave(rec);
  }

  /** Close the socket and stop reconnecting. */
  close(): void {
    this.flush();
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

  /** Subscribe, resuming from the cursor of the rows we hold (cached, or before a reconnect). */
  private sendSubscribe(rec: SubRecord): void {
    if (!rec.ready) return;
    const msg: ClientMessage = { type: "subscribe", id: rec.id, sql: rec.sql };
    if (rec.store.at !== undefined) msg.since = rec.store.at;
    if (this.send(msg)) rec.stats = { ...rec.stats, subscribes: rec.stats.subscribes + 1 };
  }

  private ensureConnected(): void {
    if (this.ws || this.closed || this.reconnectTimer) return;
    const Impl = this.opts.WebSocket ?? globalThis.WebSocket;
    const ws = new Impl(this.opts.url);
    this.ws = ws;
    ws.onopen = () => {
      this.backoff = this.opts.minBackoffMs;
      for (const rec of this.subs.values()) this.sendSubscribe(rec);
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
          if (stats.cache) stats.cache = { ...stats.cache, resumed: stats.cache.hit ? false : undefined };
          this.scheduleSave(rec);
          break;
        case "resume":
          rec.store.applyResume(msg);
          if (stats.cache) stats.cache = { ...stats.cache, resumed: true };
          rec.awaitingCatchUp = true;
          break;
        case "diff":
          rec.store.applyDiff(msg);
          stats.diffs++;
          for (const c of msg.changes) {
            if (c.op === "insert") stats.inserts++;
            else if (c.op === "delete") stats.deletes++;
          }
          if (rec.awaitingCatchUp && stats.cache) stats.cache = { ...stats.cache, catchUp: msg.changes.length };
          rec.awaitingCatchUp = false;
          this.persistDiff(rec, msg);
          break;
        case "progress":
          rec.store.at = msg.at;
          if (rec.awaitingCatchUp && stats.cache) stats.cache = { ...stats.cache, catchUp: 0 };
          rec.awaitingCatchUp = false;
          this.persistCursor(rec);
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
    stats.at = rec.store.at;
    rec.stats = stats;
    this.emit(rec, { ...this.view(rec), status: "live" });
  }

  // --- persistence ---------------------------------------------------------------------------

  /** Queue a cache write after the previous ones; failures are logged, never fatal. */
  private queue(rec: SubRecord, write: (cache: ResultCache) => Promise<void>): void {
    const cache = this.opts.cache;
    if (!cache) return;
    rec.persist = rec.persist
      .then(async () => {
        const started = performance.now();
        await write(cache);
        const c = rec.stats.cache;
        if (c) rec.stats = { ...rec.stats, cache: { ...c, saves: c.saves + 1, lastSaveMs: performance.now() - started } };
      })
      .catch((e) => console.warn("chlive: writing the cache failed", e));
  }

  /** Save the whole result soon (the latest state then, not now). */
  private scheduleSave(rec: SubRecord): void {
    if (!this.opts.cache || rec.saveTimer) return;
    rec.saveTimer = setTimeout(() => this.flushSave(rec), this.opts.saveDelayMs);
  }

  private flushSave(rec: SubRecord): void {
    if (!rec.saveTimer) return;
    clearTimeout(rec.saveTimer);
    rec.saveTimer = null;
    const at = rec.store.at;
    if (at === undefined) return;
    // the rows array is never mutated (a new one per diff), so this is a consistent state
    const result: CachedResult = {
      rows: rec.store.rows(),
      columns: rec.store.columns,
      order: rec.store.order,
      limit: rec.store.limit,
      at,
      savedAt: Date.now(),
      ids: rec.store.ids().some((id) => id !== undefined) ? rec.store.ids() : undefined,
    };
    rec.saved = true;
    this.queue(rec, (cache) => cache.save(rec.sql, result));
  }

  /** Appends go straight to the cache; anything else (indexes, deletes) means a full save. */
  private persistDiff(rec: SubRecord, msg: DiffMessage): void {
    const appendOnly = msg.changes.every((c) => c.op === "insert" && c.index === undefined);
    if (appendOnly && rec.saved && !rec.saveTimer) {
      const rows = msg.changes.map((c) => (c as { row: Row }).row);
      this.queue(rec, (cache) => cache.append(rec.sql, rows, msg.at));
    } else {
      this.scheduleSave(rec);
    }
  }

  private persistCursor(rec: SubRecord): void {
    const at = rec.store.at;
    if (at === undefined || !rec.saved || rec.saveTimer) return; // a pending save carries it
    this.queue(rec, (cache) => cache.setCursor(rec.sql, at));
  }

  // -------------------------------------------------------------------------------------------

  private view(rec: SubRecord): LiveState<any> {
    return {
      status: rec.state.status,
      rows: rec.store.rows(),
      columns: rec.store.columns,
      order: rec.store.order,
      limit: rec.store.limit,
      stats: rec.stats,
    };
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
