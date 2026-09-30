import { useEffect, useMemo, useState } from "react";
import { useLiveQuery } from "@chlive/client/react";
import type { Params } from "@chlive/client";

import { Card, LineChart, LiveTable, fmt } from "./components.tsx";
import { PATHS, Q, REJECTIONS, chTime } from "./queries.ts";

export function App() {
  return (
    <main>
      <h1>
        chlive <small>live ClickHouse queries: history states + Kafka tail</small>
      </h1>
      <div className="grid">
        <LatestFeed />
        <Counters />
        <TopCountries />
        <PerMinute />
        <Orders />
        <Slowest />
        <DebugPanel />
        <TryQuery />
      </div>
    </main>
  );
}

function LatestFeed() {
  const s = useLiveQuery(Q.latest);
  return (
    <Card n={1} title="Latest events (Rows plan)" sql={Q.latest} state={s} wide>
      <LiveTable state={s} />
    </Card>
  );
}

function Counters() {
  const s = useLiveQuery(Q.counters);
  const r = s.rows[0];
  return (
    <Card n={2} title="Global counters (single row)" sql={Q.counters} state={s}>
      <div className="stats">
        {(["total", "avg_latency", "revenue"] as const).map((k) => (
          <div key={k} className="stat">
            <div className="label">{k}</div>
            <div className="value">{r ? fmt(r[k]) : "…"}</div>
          </div>
        ))}
      </div>
    </Card>
  );
}

function TopCountries() {
  const [path, setPath] = useState("/checkout");
  const params = useMemo(() => ({ path }), [path]);
  const s = useLiveQuery(Q.topCountries, params);
  return (
    <Card
      n={3}
      title="Top countries for a path"
      sql={Q.topCountries}
      state={s}
      extra={
        <label className="control">
          path:{" "}
          <select value={path} onChange={(e) => setPath(e.target.value)}>
            {PATHS.map((p) => (
              <option key={p}>{p}</option>
            ))}
          </select>
        </label>
      }
    >
      <LiveTable state={s} />
    </Card>
  );
}

function PerMinute() {
  // Computed once at mount: a fixed lower bound, not a sliding window (see NOTES.md).
  // Rounded to the minute so tabs opened in the same minute share one subscription.
  const [since] = useState(() => chTime(new Date(Math.floor((Date.now() - 3600_000) / 60_000) * 60_000)));
  const s = useLiveQuery(Q.perMinute, { since });
  const requests = s.rows.map((r) => ({ x: String(r.minute), series: { requests: Number(r.requests) } }));
  const latency = s.rows.map((r) => {
    const [p50, p99] = (r.p50_p99 as number[] | undefined) ?? [0, 0];
    return { x: String(r.minute), series: { p50, p95: Number(r.p95), p99 } };
  });
  return (
    <Card n={4} title="Per-minute requests and latency percentiles" sql={Q.perMinute} state={s} wide
      extra={<p className="muted">since = {since} UTC (fixed at subscribe time)</p>}>
      <div className="charts">
        <LineChart points={requests} colors={{ requests: "#4f9cf9" }} />
        <LineChart points={latency} colors={{ p50: "#3fb950", p95: "#f5a524", p99: "#e5484d" }} />
      </div>
    </Card>
  );
}

function Orders() {
  const s = useLiveQuery(Q.orders);
  return (
    <Card n={5} title="Expressions over aggregates, -If, HAVING" sql={Q.orders} state={s}>
      <LiveTable state={s} />
    </Card>
  );
}

function Slowest() {
  const s = useLiveQuery(Q.slowest);
  return (
    <Card n={6} title="argMax: slowest request per path" sql={Q.slowest} state={s}>
      <LiveTable state={s} />
    </Card>
  );
}

interface DebugSub {
  id: number;
  sql: string;
  params: Params;
  refcount: number;
  rows: number | null;
  stats: { merges: number; last_merge_ms: number; snapshot_ms: number };
}

function DebugPanel() {
  const [subs, setSubs] = useState<DebugSub[]>([]);
  const [err, setErr] = useState<string | null>(null);
  useEffect(() => {
    let alive = true;
    const poll = async () => {
      try {
        const r = await fetch("/debug/subscriptions");
        const j = await r.json();
        if (alive) {
          setSubs(j.subscriptions);
          setErr(null);
        }
      } catch (e) {
        if (alive) setErr(String(e));
      }
    };
    poll();
    const t = setInterval(poll, 1500);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, []);
  return (
    <Card n={7} title="Shared subscriptions (/debug/subscriptions)" sql="-- open this page in a second tab: refcount goes to 2" wide>
      {err && <p className="error">{err}</p>}
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>id</th><th>refcount</th><th>rows</th><th>merges</th><th>merge ms</th><th>snapshot ms</th><th>query</th>
            </tr>
          </thead>
          <tbody>
            {subs.map((s) => (
              <tr key={s.id}>
                <td>{s.id}</td>
                <td className="numcell"><b>{s.refcount}</b></td>
                <td className="numcell">{fmt(s.rows)}</td>
                <td className="numcell">{fmt(s.stats.merges)}</td>
                <td className="numcell">{fmt(s.stats.last_merge_ms)}</td>
                <td className="numcell">{fmt(s.stats.snapshot_ms)}</td>
                <td className="sqlcell" title={s.sql}>
                  {s.sql.replace(/\s+/g, " ").slice(0, 90)}
                  {Object.keys(s.params).length > 0 && <span className="muted"> {JSON.stringify(s.params)}</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function TryQuery() {
  const [draft, setDraft] = useState("SELECT country, count() AS n\nFROM events\nGROUP BY country\nORDER BY n DESC\nLIMIT 5");
  const [paramsText, setParamsText] = useState("{}");
  const [submitted, setSubmitted] = useState<{ sql: string; params: Params } | null>(null);
  const [paramsErr, setParamsErr] = useState<string | null>(null);
  const run = (sql = draft) => {
    try {
      setSubmitted({ sql, params: JSON.parse(paramsText || "{}") });
      setParamsErr(null);
    } catch (e) {
      setParamsErr(`params: ${e}`);
    }
  };
  return (
    <Card n={8} title="Try your own query" sql="-- rejected queries return a clean error" wide>
      <div className="try">
        <textarea value={draft} onChange={(e) => setDraft(e.target.value)} rows={6} spellCheck={false} />
        <input value={paramsText} onChange={(e) => setParamsText(e.target.value)} placeholder='params, e.g. {"path": "/cart"}' />
        <div className="buttons">
          <button className="primary" onClick={() => run()}>Run live</button>
          {REJECTIONS.map((r) => (
            <button key={r.label} onClick={() => { setDraft(r.sql); run(r.sql); }}>
              {r.label}
            </button>
          ))}
        </div>
      </div>
      {paramsErr && <p className="error">{paramsErr}</p>}
      {submitted && <TryResult sql={submitted.sql} params={submitted.params} />}
    </Card>
  );
}

function TryResult({ sql, params }: { sql: string; params: Params }) {
  const s = useLiveQuery(sql, params);
  if (s.status === "error") return <p className="error"><b>{s.errorCode}</b>: {s.error}</p>;
  return <LiveTable state={s} />;
}
