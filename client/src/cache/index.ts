// The offline cache: live results kept between sessions, in SQLite on OPFS (in a worker).
// `LiveClient` loads and manages it (by default, in browsers); nothing here is called directly.
import type { CacheInfo, ResultCache } from "../index.ts";
import type { Row } from "../protocol.ts";
import type { CachedResult } from "../state.ts";
import type { StoreStats } from "./store.ts";
import type { Request, Response } from "./worker.ts";

export { SqliteResultStore, DEFAULT_BUDGET_BYTES, type Db, type StoreOptions, type StoreStats } from "./store.ts";

export interface OpfsCacheOptions {
  /** Total size of cached results before the least recently used go (default 2 GB). */
  budgetBytes?: number;
  /** How long to keep retrying to open the database while another page holds it (default 5 s);
   * a reload often starts before the previous page has let go. */
  openTimeoutMs?: number;
  /** How long a `load` waits for the database to open before counting as a miss (default 1.5 s),
   * so a database held elsewhere never holds up a subscription for long. */
  loadWaitMs?: number;
}

/** A `ResultCache` with its whole lifecycle. If the database can't be opened (e.g. another tab
 * holds it), every call does nothing and loads find nothing. */
export interface OpfsCache extends ResultCache {
  clear(): Promise<void>;
  info(): Promise<CacheInfo>;
  /** Close the database (the page is being hidden), letting the next page open it right away. */
  close(): void;
  /** Open it again after `close` (the page was restored from the back/forward cache). */
  reopen(): void;
}

type Pending = { resolve: (v: unknown) => void; reject: (e: Error) => void };

/** A worker running the store; one per attempt to open the database. */
function startWorker() {
  const worker = new Worker(new URL("./worker.ts", import.meta.url), { type: "module" });
  let next = 1;
  const pending = new Map<number, Pending>();
  worker.onmessage = (ev: MessageEvent<Response>) => {
    const p = pending.get(ev.data.id);
    if (!p) return;
    pending.delete(ev.data.id);
    if (ev.data.error !== undefined) p.reject(new Error(ev.data.error));
    else p.resolve(ev.data.result);
  };
  const call = <T>(method: Request["method"], ...args: unknown[]): Promise<T> =>
    new Promise((resolve, reject) => {
      const id = next++;
      pending.set(id, { resolve: resolve as (v: unknown) => void, reject });
      worker.postMessage({ id, method, args });
    });
  return { worker, call };
}

/** The worker closes the database, then goes. */
function release(w: ReturnType<typeof startWorker>): void {
  void w
    .call("close")
    .catch(() => undefined)
    .finally(() => w.worker.terminate());
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** A cache of live results in SQLite on OPFS, run in a dedicated worker. */
export function createOpfsCache(opts: OpfsCacheOptions = {}): OpfsCache {
  const openTimeoutMs = opts.openTimeoutMs ?? 5000;
  const loadWaitMs = opts.loadWaitMs ?? 1500;
  let current: ReturnType<typeof startWorker> | undefined;
  let unavailable: string | undefined;
  /** Bumped by `close`: an open still in progress then lets go of the database. */
  let generation = 0;

  const open = async () => {
    const opening = generation;
    const deadline = Date.now() + openTimeoutMs;
    for (;;) {
      const w = startWorker();
      try {
        await w.call<void>("init", opts.budgetBytes);
        if (opening !== generation) {
          release(w);
          return false;
        }
        current = w;
        unavailable = undefined;
        return true;
      } catch (e) {
        w.worker.terminate();
        const message = (e as Error).message;
        // another page (often the one being reloaded) still holds the database: try again
        const held = /Access Handle|NoModificationAllowed/i.test(message);
        if (opening !== generation) return false;
        if (!held || Date.now() >= deadline) {
          unavailable = message;
          console.warn("chlive: the offline cache is unavailable:", message);
          return false;
        }
        await sleep(250);
      }
    }
  };
  let ready = open();
  // ask the browser not to evict the cache under storage pressure
  void navigator.storage?.persist?.().catch(() => undefined);

  const whenReady = async <T>(f: (call: ReturnType<typeof startWorker>["call"]) => Promise<T>, otherwise: T) =>
    (await ready) && current ? f(current.call) : otherwise;

  return {
    load: async (key) => {
      const isOpen = await Promise.race([ready, sleep(loadWaitMs).then(() => false)]);
      return isOpen && current ? current.call<CachedResult | undefined>("load", key) : undefined;
    },
    save: (key, result) => whenReady((call) => call<void>("save", key, result), undefined),
    append: (key: string, rows: Row[], at: string) =>
      whenReady((call) => call<void>("append", key, rows, at), undefined),
    setCursor: (key, at) => whenReady((call) => call<void>("setCursor", key, at), undefined),
    recent: (maxRows) => whenReady((call) => call<{ key: string; result: CachedResult }[]>("recent", maxRows), []),
    remove: (key) => whenReady((call) => call<void>("remove", key), undefined),
    clear: () => whenReady((call) => call<void>("clear"), undefined),
    info: async () => {
      const stats = await whenReady((call) => call<StoreStats>("stats"), undefined);
      return { unavailable, ...stats };
    },
    close: () => {
      generation++;
      if (current) release(current);
      current = undefined;
      ready = Promise.resolve(false);
    },
    reopen: () => {
      if (!current) ready = open();
    },
  };
}
