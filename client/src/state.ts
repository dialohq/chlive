import type { Column, PatchMessage, Row, SnapshotMessage } from "./protocol.ts";

/**
 * Client-side copy of one live result: a key → row map plus the server's ordering.
 * Rows come out in `order` when the query has ORDER BY, otherwise in insertion order.
 */
export class ResultStore<R extends Row = Row> {
  private rowsByKey = new Map<string, R>();
  private order: string[] | null = null;
  columns: Column[] = [];
  keyColumns: string[] = [];

  /** A snapshot replaces all local state. */
  applySnapshot(msg: SnapshotMessage): void {
    this.rowsByKey.clear();
    for (const row of msg.rows) this.rowsByKey.set(row.__key, row as R);
    this.order = msg.order ?? null;
    this.columns = msg.columns;
    this.keyColumns = msg.keyColumns;
  }

  applyPatch(msg: PatchMessage): void {
    for (const key of msg.deletes) this.rowsByKey.delete(key);
    for (const row of msg.upserts) this.rowsByKey.set(row.__key, row as R);
    if (msg.order) this.order = msg.order;
  }

  get size(): number {
    return this.rowsByKey.size;
  }

  rows(): R[] {
    if (!this.order) return [...this.rowsByKey.values()];
    const out: R[] = [];
    for (const key of this.order) {
      const row = this.rowsByKey.get(key);
      if (row) out.push(row);
    }
    return out;
  }
}
