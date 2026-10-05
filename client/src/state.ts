import type { DiffMessage, Row, SnapshotMessage } from "./protocol.ts";

/** A result as kept in a cache: the rows and the cursor they're complete to. */
export interface CachedResult {
  rows: Row[];
  /** The server's cursor for these rows (opaque). */
  at: string;
  /** `Date.now()` when they were saved. */
  savedAt: number;
  /** Aggregated results: each row's id, in row order. */
  ids?: (string | undefined)[];
}

/** A live result: the snapshot's rows, then each diff's changes applied in order. */
export class ResultStore<R extends Row = Row> {
  /** The cursor the rows are complete to (from the server), if any. */
  at: string | undefined;
  private data: R[] = [];
  /** Row ids (aggregated results), parallel to the rows; `undefined` for rows without one. */
  private rowIds: (string | undefined)[] = [];
  /** id → index, rebuilt when rows move. */
  private byId = new Map<string, number>();

  private setIds(ids: (string | undefined)[] | undefined, n: number): void {
    this.rowIds = ids ? ids.slice() : new Array(n).fill(undefined);
    this.reindex();
  }

  private reindex(): void {
    this.byId = new Map();
    this.rowIds.forEach((id, i) => id !== undefined && this.byId.set(id, i));
  }

  /** Each row's id, if the result has them (aggregates). */
  ids(): (string | undefined)[] {
    return this.rowIds;
  }

  /** Start from a cached result (until the server resumes or replaces it). */
  applyCached(c: CachedResult): void {
    this.at = c.at;
    this.data = c.rows as R[];
    this.setIds(c.ids, c.rows.length);
  }

  /** Replaces everything (also after a reconnect: the new snapshot is the whole result). */
  applySnapshot(msg: SnapshotMessage): void {
    this.at = msg.at;
    this.data = msg.rows as R[];
    this.setIds(msg.ids, msg.rows.length);
  }

  /**
   * Applies a whole diff, in sequence, to one copy of the rows, and only then makes it the
   * result: readers never see a state from the middle of a diff. Throws on a change it doesn't
   * understand (a newer server) or an index outside the list, leaving the rows untouched.
   */
  applyDiff(msg: DiffMessage): void {
    const next = this.data.slice();
    const ids = this.rowIds.slice();
    let byId = this.byId;
    let moved = false; // rows moved: ids → indexes need rebuilding
    const indexOf = (id: string) => {
      if (moved) {
        byId = new Map();
        ids.forEach((x, i) => x !== undefined && byId.set(x, i));
        moved = false;
      }
      return byId.get(id);
    };
    for (const change of msg.changes) {
      switch (change.op) {
        case "insert":
          if (change.index === undefined) {
            next.push(change.row as R);
            ids.push(change.id);
            if (change.id !== undefined) {
              if (!moved && byId === this.byId) byId = new Map(byId);
              byId.set(change.id, next.length - 1);
            }
          } else if (Number.isInteger(change.index) && change.index >= 0 && change.index <= next.length) {
            next.splice(change.index, 0, change.row as R);
            ids.splice(change.index, 0, change.id);
            moved = true;
          } else {
            throw new Error(`insert at index ${change.index} of a result with ${next.length} rows`);
          }
          break;
        case "delete":
          if (Number.isInteger(change.index) && change.index >= 0 && change.index < next.length) {
            next.splice(change.index, 1);
            ids.splice(change.index, 1);
            moved = true;
          } else {
            throw new Error(`delete at index ${change.index} of a result with ${next.length} rows`);
          }
          break;
        case "update": {
          const i = indexOf(change.id);
          if (i === undefined) throw new Error(`update of unknown row ${change.id}`);
          next[i] = { ...next[i], ...change.set } as R;
          break;
        }
        default:
          throw new Error(`unsupported change ${JSON.stringify((change as { op: unknown }).op)}`);
      }
    }
    this.data = next;
    this.rowIds = ids;
    if (moved || byId !== this.byId) {
      this.byId = byId;
      if (moved) this.reindex();
    }
    this.at = msg.at;
  }

  /** History first, then each change applied. A new array after each diff. */
  rows(): R[] {
    return this.data;
  }

  get size(): number {
    return this.data.length;
  }
}
