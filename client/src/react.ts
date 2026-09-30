import { createContext, createElement, useContext, useEffect, useRef, useState, type ReactNode } from "react";

import { defaultUrl, LiveClient, type LiveState } from "./index.ts";
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

function shallowEqual(a: Params, b: Params): boolean {
  const ka = Object.keys(a);
  const kb = Object.keys(b);
  return ka.length === kb.length && ka.every((k) => Object.is(a[k], b[k]));
}

/** Returns a reference that only changes when `params` changes shallowly. */
function useStableParams(params: Params): Params {
  const ref = useRef(params);
  if (!shallowEqual(ref.current, params)) ref.current = params;
  return ref.current;
}

const EMPTY: Params = {};

/**
 * Subscribe to a live query. Resubscribes when `sql` or (shallowly) `params` change and
 * unsubscribes on unmount.
 */
export function useLiveQuery<R extends Row = Row>(
  sql: string,
  params: Params = EMPTY,
  opts: { client?: LiveClient } = {},
): LiveState<R> {
  const client = useClient(opts.client);
  const stable = useStableParams(params);
  const [state, setState] = useState<LiveState<R>>({ status: "loading", rows: [], columns: [], keyColumns: [] });
  useEffect(() => {
    const sub = client.subscribe<R>(sql, stable, setState);
    return () => sub.unsubscribe();
  }, [client, sql, stable]);
  return state;
}
