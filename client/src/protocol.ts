/**
 * The chlive WebSocket protocol (v1), mirroring `server/src/protocol.rs`.
 *
 * The client subscribes with an id of its choosing (scoped to the connection) and a query in the
 * chlive dialect. The server answers with one `snapshot` (the result as of subscribing), then
 * `diff`s as events arrive — or an `error`, after which that subscription is over.
 */

/** A result row: exactly the selected columns, rendered the way ClickHouse renders JSON. */
export type Row = Record<string, unknown>;

/** An output column and its ClickHouse type, e.g. `{ name: "latency_ms", type: "UInt32" }`. */
export interface Column {
  name: string;
  type: string;
}

export type ClientMessage =
  | { type: "subscribe"; id: string; sql: string }
  | { type: "unsubscribe"; id: string };

/**
 * One change to a result. Only inserts exist so far; unknown ops must be rejected.
 * With ORDER BY, an insert has the `index` the row takes: the row there and every row after it
 * move one down. Without one, the row is appended.
 */
export type Change = { op: "insert"; row: Row; index?: number };

/** One ORDER BY key the result is kept in. */
export interface OrderTerm {
  column: string;
  direction: "asc" | "desc";
  nulls: "first" | "last";
}

export interface SnapshotMessage {
  type: "snapshot";
  id: string;
  columns: Column[];
  /** In order when `order` is present. */
  rows: Row[];
  order?: OrderTerm[];
}

export interface DiffMessage {
  type: "diff";
  id: string;
  /** Never empty. Applied in sequence: each index is into the list as the previous change left it. */
  changes: Change[];
}

/**
 * `SYNTAX_ERROR` and `INVALID_QUERY` are problems with the query (the message ends with
 * `at line:column`); `HISTORY_FAILED` / `STREAM_FAILED` are ClickHouse / Kafka problems;
 * `BAD_MESSAGE` (no id) and `DUPLICATE_ID` are protocol misuse.
 */
export interface ErrorMessage {
  type: "error";
  id?: string;
  code: string;
  message: string;
}

export type ServerMessage = SnapshotMessage | DiffMessage | ErrorMessage;
