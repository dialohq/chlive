import type { Column, DiffMessage, OrderTerm, Row, SnapshotMessage } from "./protocol.ts";

/** A live result: the snapshot's rows, then each diff's changes applied in order. */
export class ResultStore<R extends Row = Row> {
  columns: Column[] = [];
  /** The ORDER BY the rows are kept in, if any. */
  order: OrderTerm[] | undefined;
  limit: number | undefined;
  private data: R[] = [];

  /** Replaces everything (also after a reconnect: the new snapshot is the whole result). */
  applySnapshot(msg: SnapshotMessage): void {
    this.columns = msg.columns;
    this.order = msg.order;
    this.limit = msg.limit;
    this.data = msg.rows as R[];
  }

  /**
   * Applies a whole diff, in sequence, to one copy of the rows, and only then makes it the
   * result: readers never see a state from the middle of a diff. Throws on a change it doesn't
   * understand (a newer server) or an index outside the list, leaving the rows untouched.
   */
  applyDiff(msg: DiffMessage): void {
    const next = this.data.slice();
    for (const change of msg.changes) {
      switch (change.op) {
        case "insert":
          if (change.index === undefined) {
            next.push(change.row as R);
          } else if (Number.isInteger(change.index) && change.index >= 0 && change.index <= next.length) {
            next.splice(change.index, 0, change.row as R);
          } else {
            throw new Error(`insert at index ${change.index} of a result with ${next.length} rows`);
          }
          break;
        case "delete":
          if (Number.isInteger(change.index) && change.index >= 0 && change.index < next.length) {
            next.splice(change.index, 1);
          } else {
            throw new Error(`delete at index ${change.index} of a result with ${next.length} rows`);
          }
          break;
        default:
          throw new Error(`unsupported change ${JSON.stringify((change as { op: unknown }).op)}`);
      }
    }
    this.data = next;
  }

  /** History first, then each change applied. A new array after each diff. */
  rows(): R[] {
    return this.data;
  }

  get size(): number {
    return this.data.length;
  }
}
