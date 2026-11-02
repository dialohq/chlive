/**
 * The chlive WebSocket protocol (v1), mirroring `protocol/src/lib.rs` (the `chlive-protocol` crate).
 *
 * The client may first say who it is (`hello`); tables the server secures show each user only
 * the rows they may see. It subscribes with an id of its choosing (scoped to the connection) and
 * a query in the chlive dialect. The server answers with a `snapshot` (the result as of
 * subscribing), then `diff`s as events arrive — or an `error`, after which that subscription is
 * over. When the user's permissions change, a new `snapshot` replaces the result; when they lose a
 * table the query reads, or a column it names, a `PERMISSION_DENIED` error ends it.
 *
 * A server verifying credentials takes the `hello`'s `token` (nothing comes before it) and
 * answers `authenticated` (the subject, and when the credential expires); `renew` brings a fresh
 * one before then, and the connection carries on. A connection without a valid credential gets
 * an `error` with no id (one of the `AUTH_*` codes) and is closed with code `AUTH_CLOSE` (4401),
 * the code as the reason.
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
  /** Who the client is: first, at most once per connection: a credential (`token`), or, on a
   * server trusting declared users (dev, tests), a `user`. */
  | { type: "hello"; user?: string; token?: string }
  /** A fresh credential for the connection's subject, before the current one expires. */
  | { type: "renew"; token: string }
  /** `since`: the `at` of a result the client already holds (e.g. cached), to resume from it.
   * `params`: values for the query's parameters. `complete`: the snapshot holds everything in
   * Kafka when subscribing. `optimistic`: a `plan` comes before each snapshot. */
  | {
      type: "subscribe";
      id: string;
      sql: string;
      since?: string;
      params?: Params;
      complete?: boolean;
      optimistic?: boolean;
    }
  | { type: "unsubscribe"; id: string }
  /** Rows to insert into `table`, each by column name (as queries name them); answered by an
   * `ack` or an `error` with this id (the client's own, apart from subscriptions'). */
  | { type: "insert"; id: string; table: string; rows: Row[] };

/**
 * One change to a result; unknown ops must be rejected.
 * - `insert`: with ORDER BY, at `index` (the row there and every row after it move one down);
 *   without, appended (LIMIT and OFFSET windows are newest first: at index 0). Rows of aggregated and FINAL results carry an `id` (their group's or
 *   row's key, e.g. `["PL"]`) that updates and deletes refer to.
 * - `delete`: a row leaves the result: the one at `index` (pushed out by LIMIT) or the one with
 *   this `id` (FINAL: deleted, or no longer matching); exactly one is set. The rows after it
 *   move up.
 * - `update`: the row with this `id` has these new values (the columns that changed, or all of
 *   them); with an `index` (ordered aggregates), it also moves: taken out, and put back so it's
 *   at `index`. Array columns (`groupArray`) can change in place instead, by an `edit` per
 *   column: the `remove` ranges (`[start, count]`, positions before the edit, ascending) go,
 *   then `append` is added at the end. A column is in `set` or in `edit`, never both.
 */
export type Change =
  | { op: "insert"; row: Row; index?: number; id?: string }
  | { op: "delete"; index?: number; id?: string }
  | { op: "update"; id: string; set: Row; index?: number; edit?: Record<string, ArrayEdit> };

/** An array column's change in place: ranges removed, then values appended. */
export interface ArrayEdit {
  remove?: [number, number][];
  append?: unknown[];
}

/** A new array: `array` with `edit` applied (it isn't changed); throws on a range outside it. */
export function applyArrayEdit(array: unknown, edit: ArrayEdit): unknown[] {
  if (!Array.isArray(array)) throw new Error(`an array edit of a non-array value ${JSON.stringify(array)}`);
  const out: unknown[] = [];
  let at = 0;
  for (const [start, count] of edit.remove ?? []) {
    if (!(Number.isInteger(start) && Number.isInteger(count)) || start < at || start + count > array.length) {
      throw new Error(`removing [${start}, ${count}] of an array of ${array.length} elements`);
    }
    for (let i = at; i < start; i++) out.push(array[i]);
    at = start + count;
  }
  for (let i = at; i < array.length; i++) out.push(array[i]);
  for (const v of edit.append ?? []) out.push(v);
  return out;
}

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
 * For an insert: `PERMISSION_DENIED` (the table takes no inserts, or not these from this user),
 * `INVALID_INSERT` (a row doesn't fit the table; nothing went in) or `WRITE_FAILED` (Kafka; the
 * message says which rows went in).
 */
export interface ErrorMessage {
  type: "error";
  id?: string;
  code: string;
  message: string;
}

/** An insert went in: each row's Kafka `[partition, offset]`, in order. */
export interface AckMessage {
  type: "ack";
  id: string;
  offsets: [number, number][];
}

/**
 * An `optimistic` subscription's plan, before each of its snapshots (and resumes): how the
 * server places rows in this result (chlive-core's `ClientPlan`, opaque here), for placing the
 * client's own pending rows the same way. Without one, `reason` says why the query isn't
 * predicted.
 */
export interface PlanMessage {
  type: "plan";
  id: string;
  plan?: unknown;
  reason?: string;
}

/** The `hello`'s (or a `renew`'s) credential is verified: the connection is `subject`'s until
 * `expires_at` (Unix ms), when the server closes it unless renewed. */
export interface AuthenticatedMessage {
  type: "authenticated";
  id?: undefined;
  subject: string;
  expires_at: number;
}

/** The credential's error codes: none (or a declared user), a signature that doesn't verify,
 * another issuer or audience, expired. Nothing more is said. */
export const AUTH_CODES = ["AUTH_MISSING", "AUTH_BAD_SIGNATURE", "AUTH_WRONG_AUDIENCE", "AUTH_EXPIRED"] as const;
/** The close code of a connection closed for its credential; the reason is the code. */
export const AUTH_CLOSE = 4401;

export type ServerMessage =
  | SnapshotMessage
  | ResumeMessage
  | DiffMessage
  | ProgressMessage
  | PlanMessage
  | AckMessage
  | AuthenticatedMessage
  | ErrorMessage;
