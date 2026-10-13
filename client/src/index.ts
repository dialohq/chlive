import type { ValueParsers } from "./parsers.ts";
import type { ClientMessage, DiffMessage, Params, Row, ServerMessage } from "./protocol.ts";
import { type CachedResult, ResultStore } from "./state.ts";

export type * from "./protocol.ts";
export { ResultStore, type CachedResult, type ResultStoreOptions } from "./state.ts";
export {
  defaultParsers,
  parseClickHouseDate,
  parseClickHouseType,
  type ClickHouseType,
  type ValueParser,
  type ValueParsers,
} from "./parsers.ts";

/**
 * - `loading`: nothing to show yet;
 * - `cached`: rows from the cache, while the server catches them up;
 * - `live`: up to date, and following changes;
 * - `error`: see `error`;
 * - `idle` (the React hooks with `enabled: false`): not subscribed; the rows, if any, are the
 *   last ones it had, no longer updated.
 */
export type Status = "loading" | "cached" | "live" | "error" | "idle";

/** Why a subscription failed; after an error it gets no more updates. */
export interface LiveError {
  /** `SYNTAX_ERROR`, `INVALID_QUERY` (both with a line:column in the message), `HISTORY_FAILED`,
   * `STREAM_FAILED`, `AGGREGATE_FAILED`, `SNAPSHOT_FAILED`, or `CLIENT_ERROR` (a change the client couldn't apply). */
  code: string;
  message: string;
}

export interface LiveState<R extends Row = Row> {
  status: Status;
  /** Exactly the selected columns, in the query's order (and in its ORDER BY order, if any). */
  rows: R[];
  error?: LiveError;
  /** When the cached rows were saved (while `status` is `cached`). */
  cachedAt?: number;
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
 * Where results are kept between sessions. `cache: true` uses SQLite on OPFS
 * (`@chlive/client/cache`); this is for other storage, and for tests. Keyed by query. Every
 * write keeps the rows and their cursor together.
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
  /** Forget every result (e.g. on logout). */
  clear?(): Promise<void>;
  /** For dev tools: why the cache isn't available, if it isn't, and what it holds. */
  info?(): Promise<CacheInfo>;
  /** Let go of the storage (the page is being hidden), so the next page can open it. */
  close?(): void;
  /** Open it again (the page was restored from the back/forward cache). */
  reopen?(): void;
}

export interface CacheInfo {
  unavailable?: string;
  queries?: number;
  rows?: number;
  bytes?: number;
  budgetBytes?: number;
}

/** The built-in cache's settings. */
export interface CacheOptions {
  /** Total size of cached results before the least recently used go (default 2 GB). */
  budgetBytes?: number;
}

export type Listener<R extends Row = Row> = (state: LiveState<R>) => void;

export interface Subscription {
  unsubscribe(): void;
}

export interface SubscribeOptions {
  /**
   * Values for the query's parameters (`{name:Type}`), read by their declared types. A `Date`
   * is sent as ClickHouse's `YYYY-MM-DD hh:mm:ss.fff` (UTC), a `bigint` as its digits.
   */
  params?: Params;
}

export interface LiveClientOptions {
  /** e.g. `ws://localhost:8080/live` */
  url: string;
  /** Initial reconnect delay; doubles up to `maxBackoffMs`. */
  minBackoffMs?: number;
  maxBackoffMs?: number;
  /** WebSocket implementation (defaults to the global one). */
  WebSocket?: new (url: string) => WebSocket;
  /**
   * Keep results between sessions: show them right away, then resume from their cursor.
   * `true` (or settings) keeps them in SQLite on OPFS, in a worker; the client takes care of
   * saving them when the page is hidden. Off by default.
   */
  cache?: boolean | CacheOptions | ResultCache;
  /** Full saves of a result are batched for this long after a change (default 1000 ms). */
  saveDelayMs?: number;
  /**
   * How column values become row values, by ClickHouse type (the base name: `DateTime64`,
   * `UUID`, …), merged over `defaultParsers`, which make `Date`, `Date32`, `DateTime` and
   * `DateTime64` columns JS `Date`s (read as UTC; DateTime64 to the millisecond). `null` turns
   * a parser off; `false` keeps every value as it arrives. Cached results keep the values as
   * they arrived, so they don't depend on the parsers.
   */
  parsers?: ValueParsers | false;
}

interface SubRecord {
  id: string;
  sql: string;
  /** As sent (serialized); undefined without any. */
  params: Params | undefined;
  /** What the cache keeps the result under: the query, with its params if any. */
  key: string;
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
  private readonly opts: Required<Omit<LiveClientOptions, "WebSocket" | "cache" | "parsers">> &
    Pick<LiveClientOptions, "WebSocket" | "parsers">;
  /** Resolves to the cache once it's set up (`undefined` without one); loads wait for it. */
  private readonly cacheReady: Promise<ResultCache | undefined> | undefined;
  private cache: ResultCache | undefined;
  private removePageListeners: (() => void) | undefined;
  private ws: WebSocket | null = null;
  private subs = new Map<string, SubRecord>();
  private nextId = 1;
  private backoff: number;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  constructor(opts: LiveClientOptions) {
    const { cache, ...rest } = opts;
    this.opts = { minBackoffMs: 500, maxBackoffMs: 10_000, saveDelayMs: 1000, ...rest };
    this.backoff = this.opts.minBackoffMs;
    if (cache) {
      this.cacheReady = openCache(cache)
        .catch((e) => {
          console.warn("chlive: the offline cache is unavailable:", e);
          return undefined;
        })
        .then((c) => {
          if (c && this.closed) c.close?.();
          else this.cache = c;
          return this.cache;
        });
      this.removePageListeners = this.watchPage();
    }
  }

  /**
   * Save results when the tab goes to the background (on mobile it may be killed without a
   * `pagehide`) and when the page is hidden; then let go of the storage, so a reload can open
   * it at once, and open it again if the page is restored from the back/forward cache.
   */
  private watchPage(): (() => void) | undefined {
    const target = globalThis as { addEventListener?: typeof addEventListener; removeEventListener?: typeof removeEventListener };
    if (typeof target.addEventListener !== "function") return undefined;
    const hide = () => {
      this.flush();
      // after the saves just queued have been sent to the worker
      setTimeout(() => this.cache?.close?.(), 0);
    };
    const show = (e: Event) => {
      if ((e as PageTransitionEvent).persisted) this.cache?.reopen?.();
    };
    const doc = (globalThis as { document?: Document }).document;
    const background = () => doc?.visibilityState === "hidden" && this.flush();
    target.addEventListener("pagehide", hide);
    target.addEventListener("pageshow", show);
    doc?.addEventListener("visibilitychange", background);
    return () => {
      target.removeEventListener?.("pagehide", hide);
      target.removeEventListener?.("pageshow", show);
      doc?.removeEventListener("visibilitychange", background);
    };
  }

  subscribe<R extends Row = Row>(sql: string, listener: Listener<R>, opts: SubscribeOptions = {}): Subscription {
    if (this.closed) throw new Error("LiveClient is closed");
    const id = `s${this.nextId++}`;
    const params = serializeParams(opts.params);
    const rec: SubRecord = {
      id,
      sql,
      params,
      key: cacheKey(sql, params),
      store: new ResultStore<R>({ parsers: this.opts.parsers }),
      state: { status: "loading", rows: [], stats: newStats(id) },
      stats: newStats(id),
      listener,
      ready: !this.cacheReady,
      persist: Promise.resolve(),
      saveTimer: null,
      saved: false,
      awaitingCatchUp: false,
    };
    this.subs.set(id, rec);
    listener(rec.state);
    this.ensureConnected();
    if (this.cacheReady) {
      const started = performance.now();
      this.cacheReady
        .then((cache) => cache?.load(rec.key))
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

  /** Forget every cached result (e.g. on logout). Results being shown stay. */
  async clearCache(): Promise<void> {
    const cache = await this.cacheReady;
    await cache?.clear?.();
  }

  /** For dev tools: whether the cache is on and working, and what it holds. */
  async cacheInfo(): Promise<CacheInfo | undefined> {
    if (!this.cacheReady) return undefined;
    const cache = await this.cacheReady;
    if (!cache) return { unavailable: "it couldn't be set up" };
    return (await cache.info?.()) ?? {};
  }

  /** Write every pending result to the cache now. */
  private flush(): void {
    for (const rec of this.subs.values()) this.flushSave(rec);
  }

  /** Close the socket and stop reconnecting; save pending results and close the cache. */
  close(): void {
    this.flush();
    this.removePageListeners?.();
    const cache = this.cache;
    if (cache?.close) {
      void Promise.all([...this.subs.values()].map((r) => r.persist)).then(() => cache.close!());
    }
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
    if (rec.params) msg.params = rec.params;
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
          // the rows we hold stay; the diffs that follow bring them up to date
          rec.store.applyResume(msg.columns);
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
          this.emit(rec, { ...rec.state, status: "error", error: { code: msg.code, message: msg.message } });
          return;
      }
    } catch (e) {
      rec.stats = stats;
      this.emit(rec, { ...rec.state, status: "error", error: { code: "CLIENT_ERROR", message: String(e) } });
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
    const cache = this.cache;
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
    if (!this.cache || rec.saveTimer) return;
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
      rows: rec.store.wireRows(),
      columns: rec.store.columns(),
      at,
      savedAt: Date.now(),
      ids: rec.store.ids().some((id) => id !== undefined) ? rec.store.ids() : undefined,
    };
    rec.saved = true;
    this.queue(rec, (cache) => cache.save(rec.key, result));
  }

  /** Appends (their rows as they came over the wire) go straight to the cache; anything else
   * (indexes, deletes) means a full save. */
  private persistDiff(rec: SubRecord, msg: DiffMessage): void {
    const appendOnly = msg.changes.every((c) => c.op === "insert" && c.index === undefined);
    if (appendOnly && rec.saved && !rec.saveTimer) {
      const rows = msg.changes.map((c) => (c as { row: Row }).row);
      this.queue(rec, (cache) => cache.append(rec.key, rows, msg.at));
    } else {
      this.scheduleSave(rec);
    }
  }

  private persistCursor(rec: SubRecord): void {
    const at = rec.store.at;
    if (at === undefined || !rec.saved || rec.saveTimer) return; // a pending save carries it
    this.queue(rec, (cache) => cache.setCursor(rec.key, at));
  }

  // -------------------------------------------------------------------------------------------

  private view(rec: SubRecord): LiveState<any> {
    return { status: rec.state.status, rows: rec.store.rows(), stats: rec.stats };
  }

  private emit(rec: SubRecord, state: LiveState<any>): void {
    rec.state = { ...state, stats: rec.stats };
    rec.listener(rec.state);
  }
}

/** The cache for the `cache` option: the built-in one is loaded only when asked for. */
async function openCache(opt: true | CacheOptions | ResultCache): Promise<ResultCache> {
  if (opt !== true && typeof (opt as ResultCache).load === "function") return opt as ResultCache;
  const { createOpfsCache } = await import("./cache/index.ts");
  return createOpfsCache(opt === true ? {} : (opt as CacheOptions));
}

/** Params as sent: `Date`s in ClickHouse's form (UTC), `bigint`s as digits; `undefined` if none. */
export function serializeParams(params: Params | undefined): Params | undefined {
  if (!params || Object.keys(params).length === 0) return undefined;
  const out: Params = {};
  for (const [k, v] of Object.entries(params)) {
    out[k] =
      v instanceof Date
        ? v.toISOString().replace("T", " ").replace("Z", "")
        : typeof v === "bigint"
          ? v.toString()
          : v;
  }
  return out;
}

/** JSON with object keys sorted, so the same params always give the same text. */
export function stableJson(v: unknown): string {
  return JSON.stringify(v, (_, x) =>
    x && typeof x === "object" && !Array.isArray(x)
      ? Object.fromEntries(Object.entries(x).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      : x,
  );
}

/** What a result is cached under: the query alone without params (as before params existed). */
export function cacheKey(sql: string, params: Params | undefined): string {
  return params ? `${sql}\u0000${stableJson(params)}` : sql;
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
