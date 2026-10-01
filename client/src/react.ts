import { createContext, createElement, useContext, useEffect, useState, type ReactNode } from "react";

import { defaultUrl, LiveClient, type LiveState } from "./index.ts";
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

const LOADING: LiveState<never> = { status: "loading", rows: [], columns: [] };

/** Subscribe to a live query. Resubscribes when `sql` changes and unsubscribes on unmount. */
export function useLiveQuery<R extends Row = Row>(sql: string, opts: { client?: LiveClient } = {}): LiveState<R> {
  const client = useClient(opts.client);
  const [state, setState] = useState<LiveState<R>>(LOADING);
  useEffect(() => {
    const sub = client.subscribe<R>(sql, setState);
    return () => sub.unsubscribe();
  }, [client, sql]);
  return state;
}
