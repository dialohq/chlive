import { createContext, createElement, useCallback, useContext, useRef, useSyncExternalStore, type ReactNode } from "react";

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
  /** With an `optimistic` client: the rows that are its own inserts, shown before the server
   * has them. */
  pending?: ReadonlySet<R>;
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

/** The hook's view of a state: the same object while its status, rows, error and pending rows
 * are, so changes to stats alone don't re-render. */
export function project<R extends Row>(
  prev: LiveQuery<R>,
  s: { status: Status; rows: R[]; error?: LiveError; pending?: ReadonlySet<R> },
): LiveQuery<R> {
  if (prev.status === s.status && prev.rows === s.rows && prev.error === s.error && prev.pending === s.pending) return prev;
  return s.pending ? { status: s.status, rows: s.rows, error: s.error, pending: s.pending } : { status: s.status, rows: s.rows, error: s.error };
}

/**
 * The client's state for a query, kept in sync with `useSyncExternalStore`: rows the client
 * already holds (shared with other components, kept after unmounts, warmed from the cache) are
 * there on the first render. `view` maps a state to what the hook returns, keeping it the same
 * object while nothing it shows changed.
 */
function useClientState<V extends { status: Status }>(
  sql: string,
  opts: LiveQueryOptions,
  initial: V,
  view: (prev: V, s: LiveState<any>) => V,
): V {
  const client = useClient(opts.client);
  const enabled = opts.enabled ?? true;
  const key = subscriptionKey(sql, opts.params, enabled);
  const params = opts.params;
  const last = useRef<V>(initial);
  const subscribe = useCallback(
    (changed: () => void) => (enabled ? client.subscribe(sql, changed, { params }).unsubscribe : () => {}),
    // `key` stands for sql, the params' values and enabled
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [client, key],
  );
  const snapshot = () => {
    if (!enabled) return (last.current = idle(last.current));
    const s = client.peek(sql, { params });
    return (last.current = s ? view(last.current, s) : initial);
  };
  return useSyncExternalStore(subscribe, snapshot, () => initial);
}

/**
 * Subscribe to a live query. Resubscribes when `sql` or the params' values change, and
 * unsubscribes on unmount or when `enabled` turns false. When the client already holds the
 * query, its rows are there on the first render.
 */
export function useLiveQuery<R extends Row = Row>(sql: string, opts: LiveQueryOptions = {}): LiveQuery<R> {
  return useClientState<LiveQuery<R>>(sql, opts, EMPTY, project);
}

const LOADING: LiveState<never> = {
  status: "loading",
  rows: [],
  stats: { id: "", subscribes: 0, snapshots: 0, diffs: 0, inserts: 0, deletes: 0, bytes: 0 },
};

/** `useLiveQuery` with everything the client knows (cache and message stats too), for dev
 * tools. Re-renders on every message. */
export function useLiveQueryDebug<R extends Row = Row>(sql: string, opts: LiveQueryOptions = {}): LiveState<R> {
  return useClientState<LiveState<R>>(sql, opts, LOADING, (_, s) => s);
}
