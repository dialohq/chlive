import type { Column, DiffMessage, Row, SnapshotMessage } from "./protocol.ts";

/** A live result: the snapshot's rows, then each diff's changes applied in order. */
export class ResultStore<R extends Row = Row> {
  columns: Column[] = [];
  private data: R[] = [];

  /** Replaces everything (also after a reconnect: the new snapshot is the whole result). */
  applySnapshot(msg: SnapshotMessage): void {
    this.columns = msg.columns;
    this.data = msg.rows as R[];
  }

  /** Throws on a change it doesn't understand (a newer server), leaving the rows untouched. */
  applyDiff(msg: DiffMessage): void {
    const next = this.data.slice();
    for (const change of msg.changes) {
      switch (change.op) {
        case "insert":
          next.push(change.row as R);
          break;
        default:
          throw new Error(`unsupported change ${JSON.stringify((change as { op: unknown }).op)}`);
      }
    }
    this.data = next;
  }

  /** In arrival order: history first, then inserts as they came. A new array after each change. */
  rows(): R[] {
    return this.data;
  }

  get size(): number {
    return this.data.length;
  }
}
