import type { Column, Row } from "../protocol.ts";
import type { CachedResult } from "../state.ts";

/** The little of SQLite the store needs; adapters exist for sqlite-wasm and `bun:sqlite`. */
export interface Db {
  exec(sql: string, params?: unknown[]): void;
  all(sql: string, params?: unknown[]): Record<string, unknown>[];
  transaction<T>(fn: () => T): T;
}

export interface StoreOptions {
  /** Total size of cached results (JSON bytes) before the least recently used go (default 2 GB). */
  budgetBytes?: number;
  /** Rows per stored chunk (default 1000). */
  chunkRows?: number;
}

export interface StoreStats {
  queries: number;
  rows: number;
  bytes: number;
  budgetBytes: number;
}

export const DEFAULT_BUDGET_BYTES = 2 * 1024 ** 3;

/** The tables' layout; a database with another one is emptied. */
const SCHEMA_VERSION = 3;

/**
 * Cached results in SQLite: one `results` row per query (its metadata and cursor), and its rows
 * as JSON chunks in `chunks`, in order. Every write is one transaction, so rows and cursor
 * always match; after each, the least recently used results are evicted down to the budget.
 */
export class SqliteResultStore {
  readonly budgetBytes: number;
  private readonly db: Db;
  private readonly chunkRows: number;

  constructor(db: Db, opts: StoreOptions = {}) {
    this.db = db;
    this.budgetBytes = opts.budgetBytes ?? DEFAULT_BUDGET_BYTES;
    this.chunkRows = opts.chunkRows ?? 1000;
    // an older layout is just a cache: start over
    const [{ user_version: version }] = db.all("PRAGMA user_version") as { user_version: number }[];
    if (Number(version) !== SCHEMA_VERSION) {
      db.exec("DROP TABLE IF EXISTS chunks");
      db.exec("DROP TABLE IF EXISTS results");
      db.exec(`PRAGMA user_version = ${SCHEMA_VERSION}`);
    }
    db.exec(`CREATE TABLE IF NOT EXISTS results (
      key TEXT PRIMARY KEY,
      cursor TEXT NOT NULL,
      columns TEXT,
      rows INTEGER NOT NULL,
      bytes INTEGER NOT NULL,
      saved_at INTEGER NOT NULL,
      used_at INTEGER NOT NULL
    )`);
    db.exec(`CREATE TABLE IF NOT EXISTS chunks (
      key TEXT NOT NULL,
      seq INTEGER NOT NULL,
      data TEXT NOT NULL,
      PRIMARY KEY (key, seq)
    )`);
    db.exec("CREATE INDEX IF NOT EXISTS results_used_at ON results (used_at)");
  }

  /** The cached result, marking it as just used. */
  load(key: string): CachedResult | undefined {
    const result = this.read(key);
    if (result) this.db.exec("UPDATE results SET used_at = ? WHERE key = ?", [Date.now(), key]);
    return result;
  }

  /**
   * The most recently used results that fit in `maxRows` rows together, most recent first (one
   * that doesn't fit is skipped), for warming a client's memory. Doesn't mark them as used.
   */
  recent(maxRows: number): { key: string; result: CachedResult }[] {
    const out: { key: string; result: CachedResult }[] = [];
    let total = 0;
    for (const r of this.db.all("SELECT key, rows FROM results ORDER BY used_at DESC, key")) {
      const n = Number(r.rows);
      if (total + n > maxRows) continue;
      const result = this.read(r.key as string);
      if (!result) continue;
      total += n;
      out.push({ key: r.key as string, result });
    }
    return out;
  }

  private read(key: string): CachedResult | undefined {
    const [r] = this.db.all("SELECT * FROM results WHERE key = ?", [key]);
    if (!r) return undefined;
    const rows: Row[] = [];
    for (const c of this.db.all("SELECT data FROM chunks WHERE key = ? ORDER BY seq", [key])) {
      for (const row of JSON.parse(c.data as string) as Row[]) rows.push(row);
    }
    return {
      rows,
      columns: r.columns ? (JSON.parse(r.columns as string) as Column[]) : undefined,
      at: r.cursor as string,
      savedAt: Number(r.saved_at),
    };
  }

  /** Replace a query's result. */
  save(key: string, result: CachedResult): void {
    this.db.transaction(() => {
      this.db.exec("DELETE FROM chunks WHERE key = ?", [key]);
      const bytes = this.writeChunks(key, 0, result.rows);
      this.db.exec(
        `INSERT INTO results (key, cursor, columns, rows, bytes, saved_at, used_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (key) DO UPDATE SET cursor = excluded.cursor, columns = excluded.columns,
           rows = excluded.rows, bytes = excluded.bytes, saved_at = excluded.saved_at,
           used_at = excluded.used_at`,
        [
          key,
          result.at,
          result.columns ? JSON.stringify(result.columns) : null,
          result.rows.length,
          bytes,
          result.savedAt,
          Date.now(),
        ],
      );
      this.gc(key);
    });
  }

  /** Append rows to a saved result, with its new cursor. No-op if there's no saved result. */
  append(key: string, rows: Row[], at: string): void {
    this.db.transaction(() => {
      const [r] = this.db.all("SELECT COALESCE(MAX(seq), -1) AS seq FROM chunks WHERE key = ?", [key]);
      const exists = this.db.all("SELECT 1 FROM results WHERE key = ?", [key]).length > 0;
      if (!exists) return;
      const bytes = this.writeChunks(key, Number(r.seq) + 1, rows);
      this.db.exec(
        "UPDATE results SET rows = rows + ?, bytes = bytes + ?, cursor = ?, saved_at = ?, used_at = ? WHERE key = ?",
        [rows.length, bytes, at, Date.now(), Date.now(), key],
      );
      this.gc(key);
    });
  }

  /** The rows are unchanged; only the cursor moved. */
  setCursor(key: string, at: string): void {
    this.db.exec("UPDATE results SET cursor = ?, saved_at = ? WHERE key = ?", [at, Date.now(), key]);
  }

  remove(key: string): void {
    this.db.transaction(() => {
      this.db.exec("DELETE FROM chunks WHERE key = ?", [key]);
      this.db.exec("DELETE FROM results WHERE key = ?", [key]);
    });
  }

  clear(): void {
    this.db.transaction(() => {
      this.db.exec("DELETE FROM chunks");
      this.db.exec("DELETE FROM results");
    });
  }

  stats(): StoreStats {
    const [s] = this.db.all(
      "SELECT COUNT(*) AS queries, COALESCE(SUM(rows), 0) AS rows, COALESCE(SUM(bytes), 0) AS bytes FROM results",
    );
    return {
      queries: Number(s.queries),
      rows: Number(s.rows),
      bytes: Number(s.bytes),
      budgetBytes: this.budgetBytes,
    };
  }

  /** Rows as JSON chunks from `seq` on; returns their size in bytes. */
  private writeChunks(key: string, seq: number, rows: Row[]): number {
    let bytes = 0;
    for (let i = 0; i < rows.length; i += this.chunkRows) {
      const data = JSON.stringify(rows.slice(i, i + this.chunkRows));
      bytes += data.length;
      this.db.exec("INSERT INTO chunks (key, seq, data) VALUES (?, ?, ?)", [key, seq++, data]);
    }
    return bytes;
  }

  /** Evict the least recently used results until under the budget; never `keep`. */
  private gc(keep: string): void {
    let [{ total }] = this.db.all("SELECT COALESCE(SUM(bytes), 0) AS total FROM results") as {
      total: number;
    }[];
    total = Number(total);
    if (total <= this.budgetBytes) return;
    const victims = this.db.all("SELECT key, bytes FROM results WHERE key != ? ORDER BY used_at, key", [keep]);
    for (const v of victims) {
      if (total <= this.budgetBytes) break;
      this.db.exec("DELETE FROM chunks WHERE key = ?", [v.key]);
      this.db.exec("DELETE FROM results WHERE key = ?", [v.key]);
      total -= Number(v.bytes);
    }
  }
}
