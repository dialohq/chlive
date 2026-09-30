/** Wire protocol shared with chlive-server (JSON over WebSocket at `/live`). */

export type Params = Record<string, string | number | boolean>;

export interface Column {
  name: string;
  type: string;
}

/** A result row. `__key` is the JSON-encoded array of key column values. */
export type Row = Record<string, unknown> & { __key: string };

export type ClientMessage =
  | { type: "subscribe"; id: string; sql: string; params: Params }
  | { type: "unsubscribe"; id: string };

export interface SnapshotMessage {
  type: "snapshot";
  id: string;
  columns: Column[];
  keyColumns: string[];
  rows: Row[];
  order?: string[];
}

export interface PatchMessage {
  type: "patch";
  id: string;
  upserts: Row[];
  deletes: string[];
  order?: string[];
}

export interface ErrorMessage {
  type: "error";
  id?: string;
  code: string;
  message: string;
}

export type ServerMessage = SnapshotMessage | PatchMessage | ErrorMessage;
