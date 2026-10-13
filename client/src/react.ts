import { createContext, createElement, useContext, useEffect, useState, type ReactNode } from "react";

import { defaultUrl, LiveClient, type LiveError, type LiveState, type Status, stableJson } from "./index.ts";
import type { Params, Row } from "./protocol.ts";

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

export interface LiveQueryOptions {
  /** The client; by default the one from `LiveClientProvider`, or one for this page's `/live`. */
  client?: LiveClient;
  /** Values for the query's parameters (`{name:Type}`). A new object with the same values
   * doesn't resubscribe. */
  params?: Params;
  /**
   * Whether to subscribe (default `true`). Hooks can't be called conditionally; with `false`
   * the hook subscribes to nothing and its status is `idle`: no rows if it never ran, or the
   * last rows it had (no longer updated) if it was turned off.
   */
  enabled?: boolean;
}

export interface LiveQuery<R extends Row = Row> {
  /** `loading` (nothing yet), `cached` (rows from the offline cache, being caught up), `live`
   * (up to date and following changes), `error`, or `idle` (`enabled: false`). */
  status: Status;
  /** The result, kept up to date: exactly the selected columns, in the query's order. */
  rows: R[];
  /** Why the query failed; it gets no more updates. */
  error?: LiveError;
}

const EMPTY: LiveQuery<never> = { status: "loading", rows: [] };

/** What a disabled hook shows: its last state, marked `idle` (no rows if it never ran). */
export function idle<S extends { status: Status }>(last: S): S {
  return last.status === "idle" ? last : { ...last, status: "idle" };
}

/** Changes exactly when the subscription must: the client, the query, its params' values, or
 * `enabled`. */
export function subscriptionKey(sql: string, params: Params | undefined, enabled: boolean): string {
  return enabled ? `${sql}\u0000${stableJson(params ?? {})}` : "";
}

/**
 * Subscribe to a live query. Resubscribes when `sql` or the params' values change, and
 * unsubscribes on unmount or when `enabled` turns false.
 */
export function useLiveQuery<R extends Row = Row>(sql: string, opts: LiveQueryOptions = {}): LiveQuery<R> {
  const client = useClient(opts.client);
  const enabled = opts.enabled ?? true;
  const key = subscriptionKey(sql, opts.params, enabled);
  const [result, setResult] = useState<LiveQuery<R>>(enabled ? EMPTY : idle(EMPTY));
  useEffect(() => {
    if (!enabled) {
      setResult(idle);
      return;
    }
    // re-render only when the status, rows or error change (not on every message)
    const sub = client.subscribe<R>(
      sql,
      ({ status, rows, error }) =>
        setResult((prev) =>
          prev.status === status && prev.rows === rows && prev.error === error ? prev : { status, rows, error },
        ),
      { params: opts.params },
    );
    return () => sub.unsubscribe();
    // `key` stands for sql, the params' values and enabled
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, key]);
  return result;
}

const LOADING: LiveState<never> = {
  status: "loading",
  rows: [],
  stats: { id: "", subscribes: 0, snapshots: 0, diffs: 0, inserts: 0, deletes: 0, bytes: 0 },
};

/** `useLiveQuery` with everything the client knows (cache and message stats too), for dev
 * tools. Re-renders on every message. */
export function useLiveQueryDebug<R extends Row = Row>(sql: string, opts: LiveQueryOptions = {}): LiveState<R> {
  const client = useClient(opts.client);
  const enabled = opts.enabled ?? true;
  const key = subscriptionKey(sql, opts.params, enabled);
  const [state, setState] = useState<LiveState<R>>(enabled ? LOADING : idle(LOADING));
  useEffect(() => {
    if (!enabled) {
      setState(idle);
      return;
    }
    const sub = client.subscribe<R>(sql, setState, { params: opts.params });
    return () => sub.unsubscribe();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, key]);
  return state;
}
