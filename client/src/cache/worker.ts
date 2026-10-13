// The cache's worker: SQLite (sqlite-wasm) on OPFS through the "opfs-sahpool" VFS, which needs
// no COOP/COEP headers but holds the database exclusively: a second tab can't open it and runs
// without a cache. Serves `SqliteResultStore` calls over postMessage.
import sqlite3InitModule from "@sqlite.org/sqlite-wasm";

import { type Db, SqliteResultStore } from "./store.ts";

export type Request =
  | { id: number; method: "init"; args: [budgetBytes?: number] }
  | { id: number; method: "load" | "remove"; args: [key: string] }
  | { id: number; method: "save"; args: [key: string, result: unknown] }
  | { id: number; method: "append"; args: [key: string, rows: unknown[], at: string] }
  | { id: number; method: "setCursor"; args: [key: string, at: string] }
  | { id: number; method: "recent"; args: [maxRows: number] }
  | { id: number; method: "stats" | "clear" | "close"; args: [] };

export type Response = { id: number; result?: unknown; error?: string };

let store: SqliteResultStore | undefined;
let release: (() => void) | undefined;

async function init(budgetBytes?: number): Promise<void> {
  const sqlite3 = await sqlite3InitModule();
  const pool = await sqlite3.installOpfsSAHPoolVfs({ name: "chlive-cache" });
  const db = new pool.OpfsSAHPoolDb("/chlive-cache.sqlite3");
  const adapter: Db = {
    exec: (sql, params) => void db.exec({ sql, bind: params as never }),
    all: (sql, params) => db.selectObjects(sql, params as never) as Record<string, unknown>[],
    transaction: (fn) => db.transaction(() => fn()),
  };
  store = new SqliteResultStore(adapter, { budgetBytes });
  // closing gives the OPFS access handles back, so a reloaded page can open the database
  release = () => {
    db.close();
    pool.pauseVfs();
  };
}

self.onmessage = async (ev: MessageEvent<Request>) => {
  const { id, method, args } = ev.data;
  try {
    let result: unknown;
    if (method === "init") {
      await init(args[0]);
    } else if (method === "close") {
      release?.();
      store = undefined;
      release = undefined;
    } else {
      if (!store) throw new Error("cache not initialized");
      result = (store[method] as (...a: unknown[]) => unknown)(...args);
    }
    self.postMessage({ id, result } satisfies Response);
  } catch (e) {
    self.postMessage({ id, error: String(e) } satisfies Response);
  }
};
