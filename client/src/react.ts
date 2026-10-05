import { createContext, createElement, useContext, useEffect, useState, type ReactNode } from "react";

import { defaultUrl, LiveClient, type LiveError, type LiveState, type Status } from "./index.ts";
import type { Row } from "./protocol.ts";

const LiveClientContext = createContext<LiveClient | null>(null);

export function LiveClientProvider(props: { client: LiveClient; children?: ReactNode }) {
  return createElement(LiveClientContext.Provider, { value: props.client }, props.children);
}

let fallbackClient: LiveClient | null = null;

function useClient(explicit?: LiveClient): LiveClient {
  const ctx = useContext(LiveClientContext);
  if (explicit) return explicit;
  if (ctx) return ctx;
  fallbackClient ??= new LiveClient({ url: defaultUrl() });
  return fallbackClient;
}

export interface LiveQuery<R extends Row = Row> {
  /** `loading` (nothing yet), `cached` (rows from the offline cache, being caught up), `live`
   * (up to date and following changes) or `error`. */
  status: Status;
  /** The result, kept up to date: exactly the selected columns, in the query's order. */
  rows: R[];
  /** Why the query failed; it gets no more updates. */
  error?: LiveError;
}

const EMPTY: LiveQuery<never> = { status: "loading", rows: [] };

/** Subscribe to a live query. Resubscribes when `sql` changes and unsubscribes on unmount. */
export function useLiveQuery<R extends Row = Row>(sql: string, opts: { client?: LiveClient } = {}): LiveQuery<R> {
  const client = useClient(opts.client);
  const [result, setResult] = useState<LiveQuery<R>>(EMPTY);
  useEffect(() => {
    // re-render only when the status, rows or error change (not on every message)
    const sub = client.subscribe<R>(sql, ({ status, rows, error }) =>
      setResult((prev) =>
        prev.status === status && prev.rows === rows && prev.error === error ? prev : { status, rows, error },
      ),
    );
    return () => sub.unsubscribe();
  }, [client, sql]);
  return result;
}

const LOADING: LiveState<never> = {
  status: "loading",
  rows: [],
  stats: { id: "", subscribes: 0, snapshots: 0, diffs: 0, inserts: 0, deletes: 0, bytes: 0 },
};

/** `useLiveQuery` with everything the client knows (cache and message stats too), for dev
 * tools. Re-renders on every message. */
export function useLiveQueryDebug<R extends Row = Row>(sql: string, opts: { client?: LiveClient } = {}): LiveState<R> {
  const client = useClient(opts.client);
  const [state, setState] = useState<LiveState<R>>(LOADING);
  useEffect(() => {
    const sub = client.subscribe<R>(sql, setState);
    return () => sub.unsubscribe();
  }, [client, sql]);
  return state;
}
