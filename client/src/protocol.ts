/**
 * The chlive WebSocket protocol (v1), mirroring `protocol/src/lib.rs` (the `chlive-protocol` crate).
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

/** Values for a query's parameters (`{name:Type}`), by name; each is read by its declared type
 * (`"42"` or `42` for a UInt32, a UUID in either case, …). */
export type Params = Record<string, unknown>;

export type ClientMessage =
  /** `since`: the `at` of a result the client already holds (e.g. cached), to resume from it.
   * `params`: values for the query's parameters. */
  | { type: "subscribe"; id: string; sql: string; since?: string; params?: Params }
  | { type: "unsubscribe"; id: string };

/**
 * One change to a result; unknown ops must be rejected.
 * - `insert`: with ORDER BY, at `index` (the row there and every row after it move one down);
 *   without, appended. Rows of aggregated and FINAL results carry an `id` (their group's or
 *   row's key, e.g. `["PL"]`) that updates and deletes refer to.
 * - `delete`: a row leaves the result: the one at `index` (pushed out by LIMIT) or the one with
 *   this `id` (FINAL: deleted, or no longer matching); exactly one is set. The rows after it
 *   move up.
 * - `update`: the row with this `id` has these new values (the columns that changed, or all of
 *   them); with an `index` (ordered aggregates), it also moves: taken out, and put back so it's
 *   at `index`.
 */
export type Change =
  | { op: "insert"; row: Row; index?: number; id?: string }
  | { op: "delete"; index?: number; id?: string }
  | { op: "update"; id: string; set: Row; index?: number };

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
  /** The LIMIT: the result never has more rows at the end of a diff. */
  limit?: number;
  /** The OFFSET: the rows (or groups) the result starts after. Informational: the diffs
   * already account for it. */
  offset?: number;
  /** An opaque cursor: how far this result is complete. Resume from it with `since`. */
  at: string;
  /** Aggregated results: each row's id (its group's key), in row order. */
  ids?: string[];
}

/** Instead of a snapshot, when resuming from `since`: the client's rows are still valid; the
 * diffs that follow bring them up to date. */
export interface ResumeMessage {
  type: "resume";
  id: string;
  columns: Column[];
  order?: OrderTerm[];
  limit?: number;
}

/** The cursor moved without changes to the result (events that didn't match). */
export interface ProgressMessage {
  type: "progress";
  id: string;
  at: string;
}

export interface DiffMessage {
  type: "diff";
  id: string;
  /**
   * Never empty, and atomic: applied in sequence (each index is into the list as the previous
   * change left it), and only the state after all of them is meaningful. In between, an insert
   * and the delete it causes can briefly make one row more than the LIMIT.
   */
  changes: Change[];
  /** The cursor after these changes. */
  at: string;
}

/**
 * `SYNTAX_ERROR` and `INVALID_QUERY` are problems with the query (the message ends with
 * `at line:column`); `HISTORY_FAILED` / `STREAM_FAILED` are ClickHouse / Kafka problems;
 * `BAD_MESSAGE` (no id) and `DUPLICATE_ID` are protocol misuse; `TOO_MANY_SUBSCRIPTIONS`: the
 * connection is at the server's `max_subscriptions_per_connection`.
 */
export interface ErrorMessage {
  type: "error";
  id?: string;
  code: string;
  message: string;
}

export type ServerMessage = SnapshotMessage | ResumeMessage | DiffMessage | ProgressMessage | ErrorMessage;
