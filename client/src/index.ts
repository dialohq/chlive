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

/** An insert went in: each row's Kafka `[partition, offset]`, in order. */
export interface Ack {
  offsets: [number, number][];
}

/**
 * Why an insert failed: `PERMISSION_DENIED`, `INVALID_INSERT` (nothing went in), `WRITE_FAILED`
 * (Kafka; the message says which rows went in), `DISCONNECTED` (the connection dropped before
 * the server answered: the rows may or may not have gone in), or `CLIENT_ERROR` (the client is
 * closed).
 */
export class InsertError extends Error implements LiveError {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "InsertError";
    this.code = code;
  }
}

/** An insert waiting for its answer; `sent` once it's on the current connection. */
interface PendingInsert {
  msg: ClientMessage & { type: "insert" };
  sent: boolean;
  resolve: (ack: Ack) => void;
  reject: (e: InsertError) => void;
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
 * Where results are kept between sessions. The default cache uses SQLite on OPFS
 * (`@chlive/client/cache`); this is for other storage, and for tests. Keyed by query and params. Every
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
  /** The most recently used results fitting in `maxRows` rows together, most recent first,
   * without marking them as used (warming the client's memory). */
  recent?(maxRows: number): Promise<{ key: string; result: CachedResult }[]>;
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
  /** Rows of the most recently used results to read into memory as soon as the cache opens,
   * so their first subscribers get them synchronously (default 10 000; 0: none). */
  warmRows?: number;
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
  /**
   * Who the client is, said to the server (`hello`) on every connection: tables it secures show
   * each user only the rows they may see. Results are cached per user.
   */
  user?: string;
  /** Initial reconnect delay; doubles up to `maxBackoffMs`. */
  minBackoffMs?: number;
  maxBackoffMs?: number;
  /** WebSocket implementation (defaults to the global one). */
  WebSocket?: new (url: string) => WebSocket;
  /**
   * Keep results between sessions: show them right away, then resume from their cursor.
   * The built-in cache keeps them in SQLite on OPFS, in a worker; the client takes care of
   * saving them when the page is hidden. On by default where it can run (a browser with OPFS
   * and workers) and off elsewhere; `false` turns it off, settings tune it.
   */
  cache?: boolean | CacheOptions | ResultCache;
  /** Full saves of a result are batched for this long after a change (default 1000 ms). */
  saveDelayMs?: number;
  /** How long a query's server subscription outlives its last subscriber (default 5000 ms), so
   * a remount (StrictMode, a route change) picks it up live. */
  keepAliveMs?: number;
  /** How long its rows (and cursor) then stay in memory, to show at once and resume from
   * (default 5 min). */
  gcTimeMs?: number;
  /** Rows that results nobody subscribes to may hold in memory in all; the oldest go first
   * (default 100 000). */
  memoryRows?: number;
  /**
   * How column values become row values, by ClickHouse type (the base name: `DateTime64`,
   * `UUID`, …), merged over `defaultParsers`, which make `Date`, `Date32`, `DateTime` and
   * `DateTime64` columns JS `Date`s (read as UTC; DateTime64 to the millisecond). `null` turns
   * a parser off; `false` keeps every value as it arrives. Cached results keep the values as
   * they arrived, so they don't depend on the parsers.
   */
  parsers?: ValueParsers | false;
}

/** A query's result, shared by everyone subscribed to the same query and params. */
interface Result {
  sql: string;
  /** As sent (serialized); undefined without any. */
  params: Params | undefined;
  /** The query with its params: what memory and the cache keep the result under. */
  key: string;
  store: ResultStore<any>;
  state: LiveState<any>;
  stats: SubscriptionStats;
  listeners: Set<Listener<any>>;
  /** The server subscription's id while subscribed (a new one each time). */
  wireId: string | undefined;
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
  /** Without subscribers: when the server subscription ends, then when the rows go. */
  keepAliveTimer: ReturnType<typeof setTimeout> | null;
  gcTimer: ReturnType<typeof setTimeout> | null;
  /** When the last subscriber left (for evicting the oldest first). */
  idleSince: number;
}

const OPEN = 1;
const DEFAULT_WARM_ROWS = 10_000;

/**
 * One WebSocket shared by any number of live queries. Reconnects with exponential backoff and
 * resubscribes every active query from the cursor it has.
 *
 * Results are kept in memory per query and params, and shared: subscribing to a query the
 * client already holds gets its rows at once (synchronously). After the last subscriber leaves,
 * the server subscription stays for `keepAliveMs`, then the rows (and cursor) for `gcTimeMs`;
 * coming back meanwhile shows them and resumes from the cursor.
 *
 * With a `cache`, a subscription first shows the cached result (`status: "cached"`) and asks
 * the server to resume from its cursor; the server sends only what changed since (or a full
 * snapshot if it can't resume). Results are saved back as they change.
 */
export class LiveClient {
  private readonly opts: Required<Omit<LiveClientOptions, "WebSocket" | "cache" | "parsers" | "user">> &
    Pick<LiveClientOptions, "WebSocket" | "parsers" | "user">;
  /** Resolves to the cache once it's set up and memory is warmed from it (`undefined` without
   * one); reads wait for it. */
  private readonly cacheReady: Promise<ResultCache | undefined> | undefined;
  private cache: ResultCache | undefined;
  private removePageListeners: (() => void) | undefined;
  private ws: WebSocket | null = null;
  private results = new Map<string, Result>();
  /** Results by their server subscription's id. */
  private byWire = new Map<string, Result>();
  /** Inserts waiting for their answer, by id. */
  private inserts = new Map<string, PendingInsert>();
  private nextId = 1;
  private backoff: number;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  constructor(opts: LiveClientOptions) {
    const { cache = opfsAvailable(), ...rest } = opts;
    this.opts = {
      minBackoffMs: 500,
      maxBackoffMs: 10_000,
      saveDelayMs: 1000,
      keepAliveMs: 5000,
      gcTimeMs: 300_000,
      memoryRows: 100_000,
      ...rest,
    };
    this.backoff = this.opts.minBackoffMs;
    if (cache) {
      const settings = cache === true || typeof (cache as ResultCache).load === "function" ? {} : (cache as CacheOptions);
      const warmRows = settings.warmRows ?? DEFAULT_WARM_ROWS;
      this.cacheReady = openCache(cache)
        .catch((e) => {
          console.warn("chlive: the offline cache is unavailable:", e);
          return undefined;
        })
        .then(async (c) => {
          if (c && this.closed) c.close?.();
          else this.cache = c;
          if (this.cache && warmRows > 0) await this.warm(this.cache, warmRows);
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

  /**
   * Subscribe to a live query. The listener is called at once with what the client has (the
   * rows, if it holds this query and params already), then on every change.
   */
  subscribe<R extends Row = Row>(sql: string, listener: Listener<R>, opts: SubscribeOptions = {}): Subscription {
    if (this.closed) throw new Error("LiveClient is closed");
    const params = serializeParams(opts.params);
    const key = this.keyOf(sql, params);
    let r = this.results.get(key);
    // a failed query is tried again once nobody is looking at the error
    if (r && r.state.status === "error" && r.listeners.size === 0) {
      this.drop(r);
      r = undefined;
    }
    r ??= this.create(sql, params, key);
    const result = r;
    // one entry per subscription, even if two pass the same function
    const entry: Listener<any> = (s) => listener(s);
    result.listeners.add(entry);
    this.retain(result);
    entry(result.state);
    let active = true;
    return {
      unsubscribe: () => {
        if (!active) return;
        active = false;
        result.listeners.delete(entry);
        if (result.listeners.size === 0) this.release(result);
      },
    };
  }

  /** The query's current state if the client holds it (synchronously), else `undefined`. The
   * same object until it changes, so it can be a `useSyncExternalStore` snapshot. */
  peek<R extends Row = Row>(sql: string, opts: SubscribeOptions = {}): LiveState<R> | undefined {
    return this.results.get(this.keyOf(sql, serializeParams(opts.params)))?.state;
  }

  /** What a query's result is kept under: its `cacheKey`, after the user's name if there's one
   * (a user's cached results are theirs). */
  private keyOf(sql: string, params: Params | undefined): string {
    const key = cacheKey(sql, params);
    return this.opts.user === undefined ? key : `${this.opts.user}\u0001${key}`;
  }

  /** A key's `cacheKey`, if it's this client's user's (or, without a user, nobody's). */
  private ownKey(key: string): string | undefined {
    const i = key.indexOf("\u0001");
    const user = i < 0 ? undefined : key.slice(0, i);
    return user === this.opts.user ? key.slice(i + 1) : undefined;
  }

  /**
   * Start a query ahead of the components that will show it (e.g. in a route loader): it's read
   * from the cache and subscribed, then kept like a query whose last subscriber just left.
   * Resolves once it has rows (cached or live) or failed.
   */
  preload(sql: string, opts: SubscribeOptions = {}): Promise<void> {
    return new Promise((resolve) => {
      let sub: Subscription | undefined;
      let done = false;
      sub = this.subscribe(
        sql,
        (s) => {
          if (done || s.status === "loading") return;
          done = true;
          resolve();
          // called during `subscribe` when the rows are already here
          queueMicrotask(() => sub?.unsubscribe());
        },
        opts,
      );
      if (done) queueMicrotask(() => sub?.unsubscribe());
    });
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
    for (const r of this.results.values()) this.flushSave(r);
  }

  /**
   * Insert rows into a table, each by column name (as queries name them): resolves with each
   * row's Kafka partition and offset once the server has produced it, or rejects with an
   * {@link InsertError}. The server checks them against the user's write permissions and the
   * table's columns. Sent once connected; one sent but unanswered when the connection drops
   * rejects with `DISCONNECTED` (it isn't sent again). An update or delete is an insert by the
   * table's own convention (e.g. a new version in a ReplacingMergeTree).
   */
  insert(table: string, rows: Row | Row[]): Promise<Ack> {
    if (this.closed) return Promise.reject(new InsertError("CLIENT_ERROR", "LiveClient is closed"));
    const msg = { type: "insert" as const, id: `w${this.nextId++}`, table, rows: Array.isArray(rows) ? rows : [rows] };
    return new Promise<Ack>((resolve, reject) => {
      const pending: PendingInsert = { msg, sent: false, resolve, reject };
      this.inserts.set(msg.id, pending);
      pending.sent = this.send(msg);
      if (!pending.sent) this.ensureConnected();
    });
  }

  /** Close the socket and stop reconnecting; save pending results and close the cache. */
  close(): void {
    this.flush();
    this.removePageListeners?.();
    const cache = this.cache;
    if (cache?.close) {
      void Promise.all([...this.results.values()].map((r) => r.persist)).then(() => cache.close!());
    }
    this.closed = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    for (const r of this.results.values()) this.clearTimers(r);
    this.results.clear();
    this.byWire.clear();
    for (const p of this.inserts.values()) p.reject(new InsertError("CLIENT_ERROR", "LiveClient is closed"));
    this.inserts.clear();
    this.ws?.close();
    this.ws = null;
  }

  // --- results in memory ---------------------------------------------------------------------

  /** A new result in memory; read from the cache unless `cached` gives it already. */
  private create(sql: string, params: Params | undefined, key: string, cached?: CachedResult): Result {
    const r: Result = {
      sql,
      params,
      key,
      store: new ResultStore({ parsers: this.opts.parsers }),
      state: { status: "loading", rows: [], stats: newStats("") },
      stats: newStats(""),
      listeners: new Set(),
      wireId: undefined,
      ready: !this.cacheReady || cached !== undefined,
      persist: Promise.resolve(),
      saveTimer: null,
      saved: false,
      awaitingCatchUp: false,
      keepAliveTimer: null,
      gcTimer: null,
      idleSince: 0,
    };
    this.results.set(key, r);
    if (!r.ready) void this.read(r);
    return r;
  }

  /** Show the cached result, if any, then let the subscription go out. */
  private async read(r: Result): Promise<void> {
    const started = performance.now();
    const cached = await this.cacheReady!
      .then((cache) => cache?.load(r.key))
      .catch((e) => {
        console.warn("chlive: reading the cache failed", e);
        return undefined;
      });
    if (this.results.get(r.key) !== r) return; // dropped meanwhile
    r.stats = {
      ...r.stats,
      at: cached?.at,
      cache: {
        hit: !!cached,
        loadedRows: cached?.rows.length ?? 0,
        loadMs: performance.now() - started,
        savedAt: cached?.savedAt,
        saves: 0,
      },
    };
    // the server may have answered already (no cache wait without a subscription, but a
    // snapshot could have come in through a previous server subscription)
    if (cached && r.store.at === undefined) {
      r.store.applyCached(cached);
      r.saved = true;
      this.emit(r, { ...this.view(r), status: "cached", cachedAt: cached.savedAt });
    } else {
      this.emit(r, r.state);
    }
    r.ready = true;
    this.sendSubscribe(r);
  }

  /** Put the cache's most recently used results in memory (`cached`, not subscribed), so the
   * first subscribers get them synchronously. */
  private async warm(cache: ResultCache, maxRows: number): Promise<void> {
    if (!cache.recent) return;
    let recent: { key: string; result: CachedResult }[];
    try {
      recent = await cache.recent(maxRows);
    } catch (e) {
      console.warn("chlive: reading the cache failed", e);
      return;
    }
    const now = Date.now();
    recent.forEach(({ key, result }, i) => {
      if (this.closed || this.results.has(key)) return;
      const own = this.ownKey(key);
      if (own === undefined) return;
      const [sql, json] = splitKey(own);
      const r = this.create(sql, json === undefined ? undefined : (JSON.parse(json) as Params), key, result);
      r.store.applyCached(result);
      r.saved = true;
      r.stats = {
        ...r.stats,
        at: result.at,
        cache: { hit: true, loadedRows: result.rows.length, loadMs: 0, savedAt: result.savedAt, saves: 0 },
      };
      r.state = { ...this.view(r), status: "cached", cachedAt: result.savedAt, stats: r.stats };
      // the most recent is the newest idle one
      r.idleSince = now - i;
      r.gcTimer = setTimeout(() => this.drop(r), this.opts.gcTimeMs);
    });
    this.evict();
  }

  /** Someone subscribed: keep it, and make sure it's subscribed on the server. */
  private retain(r: Result): void {
    if (r.keepAliveTimer) clearTimeout(r.keepAliveTimer);
    if (r.gcTimer) clearTimeout(r.gcTimer);
    r.keepAliveTimer = r.gcTimer = null;
    if (r.wireId) return;
    r.wireId = `s${this.nextId++}`;
    this.byWire.set(r.wireId, r);
    r.stats = { ...r.stats, id: r.wireId };
    this.ensureConnected();
    this.sendSubscribe(r);
  }

  /** The last subscriber left: the server subscription goes after `keepAliveMs`, the rows
   * after `gcTimeMs` more. */
  private release(r: Result): void {
    this.flushSave(r);
    r.idleSince = Date.now();
    r.keepAliveTimer = setTimeout(() => this.stop(r), this.opts.keepAliveMs);
    this.evict();
  }

  /** End the server subscription, keeping the rows (and their cursor) for `gcTimeMs`. */
  private stop(r: Result): void {
    r.keepAliveTimer = null;
    this.unsubscribeWire(r);
    this.flushSave(r);
    if (r.state.status === "live" || (r.state.status === "loading" && r.store.at !== undefined)) {
      this.emit(r, { ...r.state, status: "cached", cachedAt: Date.now() });
    }
    r.gcTimer = setTimeout(() => this.drop(r), this.opts.gcTimeMs);
  }

  private unsubscribeWire(r: Result): void {
    if (!r.wireId) return;
    this.send({ type: "unsubscribe", id: r.wireId });
    this.byWire.delete(r.wireId);
    r.wireId = undefined;
  }

  /** Forget a result (its pending save still goes to the cache). */
  private drop(r: Result): void {
    this.clearTimers(r);
    this.unsubscribeWire(r);
    this.flushSave(r);
    if (this.results.get(r.key) === r) this.results.delete(r.key);
  }

  private clearTimers(r: Result): void {
    if (r.keepAliveTimer) clearTimeout(r.keepAliveTimer);
    if (r.gcTimer) clearTimeout(r.gcTimer);
    r.keepAliveTimer = r.gcTimer = null;
  }

  /** Results nobody subscribes to may hold `memoryRows` rows in all; the oldest go first. */
  private evict(): void {
    const idle = [...this.results.values()].filter((r) => r.listeners.size === 0);
    let total = idle.reduce((n, r) => n + r.store.rows().length, 0);
    if (total <= this.opts.memoryRows) return;
    idle.sort((a, b) => a.idleSince - b.idleSince);
    for (const r of idle) {
      if (total <= this.opts.memoryRows) break;
      total -= r.store.rows().length;
      this.drop(r);
    }
  }

  // --- the connection ------------------------------------------------------------------------

  /** Whether it was sent. While disconnected, subscriptions are (re)sent from `onopen`. */
  private send(msg: ClientMessage): boolean {
    if (this.ws?.readyState !== OPEN) return false;
    this.ws.send(JSON.stringify(msg));
    return true;
  }

  /** Subscribe, resuming from the cursor of the rows we hold (cached, or before a reconnect). */
  private sendSubscribe(r: Result): void {
    if (!r.ready || !r.wireId) return;
    const msg: ClientMessage = { type: "subscribe", id: r.wireId, sql: r.sql };
    if (r.store.at !== undefined) msg.since = r.store.at;
    if (r.params) msg.params = r.params;
    if (this.send(msg)) r.stats = { ...r.stats, subscribes: r.stats.subscribes + 1 };
  }

  private ensureConnected(): void {
    if (this.ws || this.closed || this.reconnectTimer) return;
    const Impl = this.opts.WebSocket ?? globalThis.WebSocket;
    const ws = new Impl(this.opts.url);
    this.ws = ws;
    ws.onopen = () => {
      this.backoff = this.opts.minBackoffMs;
      if (this.opts.user !== undefined) this.send({ type: "hello", user: this.opts.user });
      for (const r of this.byWire.values()) this.sendSubscribe(r);
      for (const p of this.inserts.values()) if (!p.sent) p.sent = this.send(p.msg);
    };
    ws.onmessage = (ev: MessageEvent) => {
      const text = String(ev.data);
      this.onMessage(JSON.parse(text) as ServerMessage, text.length);
    };
    ws.onclose = () => {
      if (this.ws !== ws) return;
      this.ws = null;
      for (const r of this.byWire.values()) {
        if (r.state.status === "live") this.emit(r, { ...r.state, status: "loading" });
      }
      for (const [id, p] of this.inserts) {
        if (!p.sent) continue;
        this.inserts.delete(id);
        p.reject(
          new InsertError("DISCONNECTED", "the connection dropped before the server answered; the rows may or may not have gone in"),
        );
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
      if (this.byWire.size > 0 || this.inserts.size > 0) this.ensureConnected();
    }, delay);
  }

  private onMessage(msg: ServerMessage, bytes: number): void {
    const pending = msg.id ? this.inserts.get(msg.id) : undefined;
    if (pending) {
      this.inserts.delete(pending.msg.id);
      if (msg.type === "ack") pending.resolve({ offsets: msg.offsets });
      else if (msg.type === "error") pending.reject(new InsertError(msg.code, msg.message));
      else pending.reject(new InsertError("CLIENT_ERROR", `an unexpected answer to an insert: ${msg.type}`));
      return;
    }
    const rec = msg.id ? this.byWire.get(msg.id) : undefined;
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
        case "ack":
          // inserts' answers are theirs, never a subscription's
          return;
        case "plan":
          // never asked for yet
          return;
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
  private queue(rec: Result, write: (cache: ResultCache) => Promise<void>): void {
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
  private scheduleSave(rec: Result): void {
    if (!this.cache || rec.saveTimer) return;
    rec.saveTimer = setTimeout(() => this.flushSave(rec), this.opts.saveDelayMs);
  }

  private flushSave(rec: Result): void {
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
   * (indexes, deletes, ids, which appends don't keep) means a full save. */
  private persistDiff(rec: Result, msg: DiffMessage): void {
    const appendOnly = msg.changes.every(
      (c) => c.op === "insert" && c.index === undefined && c.id === undefined,
    );
    if (appendOnly && rec.saved && !rec.saveTimer) {
      const rows = msg.changes.map((c) => (c as { row: Row }).row);
      this.queue(rec, (cache) => cache.append(rec.key, rows, msg.at));
    } else {
      this.scheduleSave(rec);
    }
  }

  private persistCursor(rec: Result): void {
    const at = rec.store.at;
    if (at === undefined || !rec.saved || rec.saveTimer) return; // a pending save carries it
    this.queue(rec, (cache) => cache.setCursor(rec.key, at));
  }

  // -------------------------------------------------------------------------------------------

  private view(rec: Result): LiveState<any> {
    return { status: rec.state.status, rows: rec.store.rows(), stats: rec.stats };
  }

  private emit(rec: Result, state: LiveState<any>): void {
    rec.state = { ...state, stats: rec.stats };
    for (const listener of [...rec.listeners]) listener(rec.state);
  }
}

/** Whether the built-in cache can run here: a browser (or worker) with OPFS and workers. */
export function opfsAvailable(): boolean {
  const g = globalThis as { Worker?: unknown; navigator?: { storage?: { getDirectory?: unknown } } };
  return typeof g.Worker === "function" && typeof g.navigator?.storage?.getDirectory === "function";
}

/** The cache for the `cache` option: the built-in one is loaded only when used. */
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

/** A `cacheKey`'s query and params (JSON), back. */
function splitKey(key: string): [sql: string, params: string | undefined] {
  const i = key.indexOf("\u0000");
  return i < 0 ? [key, undefined] : [key.slice(0, i), key.slice(i + 1)];
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
