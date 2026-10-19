import { type ColumnParser, columnParsers, parseRow, resolveParsers, type ValueParsers } from "./parsers.ts";
import { applyArrayEdit, type Column, type DiffMessage, type Row, type SnapshotMessage } from "./protocol.ts";

/** A result as kept in a cache: the rows (as they came over the wire) and the cursor they're
 * complete to. */
export interface CachedResult {
  rows: Row[];
  /** The result's columns, to parse the rows with before the server answers. */
  columns?: Column[];
  /** The server's cursor for these rows (opaque). */
  at: string;
  /** `Date.now()` when they were saved. */
  savedAt: number;
  /** Aggregated results: each row's id, in row order. */
  ids?: (string | undefined)[];
}

export interface ResultStoreOptions {
  /** Merged over `defaultParsers` (`null` removes one); `false`: none. */
  parsers?: ValueParsers | false;
}

/**
 * A live result: the snapshot's rows, then each diff's changes applied in order.
 *
 * Rows are kept twice when a column has a parser (e.g. any date column, by default): as they
 * came over the wire (`wireRows()`, what a cache saves, so it doesn't depend on the parsers)
 * and parsed (`rows()`). Without parsed columns both are the same array.
 */
export class ResultStore<R extends Row = Row> {
  /** The cursor the rows are complete to (from the server), if any. */
  at: string | undefined;
  private readonly parsers: ValueParsers;
  private cols: Column[] = [];
  private parsed: ColumnParser[] = [];
  /** As received. */
  private raw: Row[] = [];
  /** Parsed; `raw` itself when no column has a parser. */
  private data: R[] = [];
  /** Row ids (aggregated results), parallel to the rows; `undefined` for rows without one. */
  private rowIds: (string | undefined)[] = [];
  /** id → index, rebuilt when rows move. */
  private byId = new Map<string, number>();

  constructor(opts: ResultStoreOptions = {}) {
    this.parsers = resolveParsers(opts.parsers);
  }

  private setColumns(columns: Column[]): void {
    this.cols = columns;
    this.parsed = columnParsers(columns, this.parsers);
  }

  private setRows(rows: Row[]): void {
    this.raw = rows;
    this.data = this.parsed.length ? rows.map((r) => parseRow<R>(r, this.parsed)) : (rows as R[]);
  }

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
    this.setColumns(c.columns ?? []);
    this.setRows(c.rows);
    this.at = c.at;
    this.setIds(c.ids, c.rows.length);
  }

  /** Replaces everything (also after a reconnect: the new snapshot is the whole result). */
  applySnapshot(msg: SnapshotMessage): void {
    this.setColumns(msg.columns);
    this.setRows(msg.rows);
    this.at = msg.at;
    this.setIds(msg.ids, msg.rows.length);
  }

  /** The server resumed from our cursor: the rows stay, (re)parsed by the columns it gives. */
  applyResume(columns: Column[]): void {
    this.setColumns(columns);
    this.setRows(this.raw);
  }

  /**
   * Applies a whole diff, in sequence, to one copy of the rows, and only then makes it the
   * result: readers never see a state from the middle of a diff. Throws on a change it doesn't
   * understand (a newer server) or an index outside the list, leaving the rows untouched.
   */
  applyDiff(msg: DiffMessage): void {
    const cols = this.parsed;
    const raw = this.raw.slice();
    // the parsed rows, kept in step with `raw`; the same array when nothing is parsed
    const next = cols.length ? this.data.slice() : (raw as R[]);
    const twin = next !== (raw as Row[]);
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
    const parse = (row: Row) => parseRow<R>(row, cols);
    for (const change of msg.changes) {
      switch (change.op) {
        case "insert":
          if (change.index === undefined) {
            raw.push(change.row);
            if (twin) next.push(parse(change.row));
            ids.push(change.id);
            if (change.id !== undefined) {
              if (!moved && byId === this.byId) byId = new Map(byId);
              byId.set(change.id, raw.length - 1);
            }
          } else if (Number.isInteger(change.index) && change.index >= 0 && change.index <= raw.length) {
            raw.splice(change.index, 0, change.row);
            if (twin) next.splice(change.index, 0, parse(change.row));
            ids.splice(change.index, 0, change.id);
            moved = true;
          } else {
            throw new Error(`insert at index ${change.index} of a result with ${raw.length} rows`);
          }
          break;
        case "delete": {
          let i = change.index;
          if ((i === undefined) === (change.id === undefined)) {
            throw new Error("a delete needs exactly one of index and id");
          }
          if (change.id !== undefined) {
            i = indexOf(change.id);
            if (i === undefined) throw new Error(`delete of unknown row ${change.id}`);
          }
          if (Number.isInteger(i) && i! >= 0 && i! < raw.length) {
            raw.splice(i!, 1);
            if (twin) next.splice(i!, 1);
            ids.splice(i!, 1);
            moved = true;
          } else {
            throw new Error(`delete at index ${i} of a result with ${raw.length} rows`);
          }
          break;
        }
        case "update": {
          const i = indexOf(change.id);
          if (i === undefined) throw new Error(`update of unknown row ${change.id}`);
          // array columns edited in place: their new values, as if set
          let set = change.set;
          if (change.edit) {
            set = { ...set };
            for (const [column, edit] of Object.entries(change.edit)) {
              set[column] = applyArrayEdit(raw[i][column], edit);
            }
          }
          const wire = { ...raw[i], ...set };
          const row = twin ? ({ ...next[i], ...parse(set) } as R) : (wire as R);
          if (change.index === undefined || change.index === i) {
            raw[i] = wire;
            if (twin) next[i] = row;
          } else if (Number.isInteger(change.index) && change.index >= 0 && change.index < raw.length) {
            raw.splice(i, 1);
            raw.splice(change.index, 0, wire);
            if (twin) {
              next.splice(i, 1);
              next.splice(change.index, 0, row);
            }
            ids.splice(i, 1);
            ids.splice(change.index, 0, change.id);
            moved = true;
          } else {
            throw new Error(`move to index ${change.index} of a result with ${raw.length} rows`);
          }
          break;
        }
        default:
          throw new Error(`unsupported change ${JSON.stringify((change as { op: unknown }).op)}`);
      }
    }
    this.raw = raw;
    this.data = next;
    this.rowIds = ids;
    if (moved || byId !== this.byId) {
      this.byId = byId;
      if (moved) this.reindex();
    }
    this.at = msg.at;
  }

  /** History first, then each change applied, parsed. A new array after each diff. */
  rows(): R[] {
    return this.data;
  }

  /** The rows as they came over the wire (what a cache keeps). */
  wireRows(): Row[] {
    return this.raw;
  }

  /** The result's columns, as the server last gave them. */
  columns(): Column[] {
    return this.cols;
  }

  get size(): number {
    return this.data.length;
  }
}
