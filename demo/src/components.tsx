import { useEffect, useState, type ReactNode } from "react";
import type { LiveState, Row } from "@chlive/client";

export function Card(props: { n: number; title: string; sql: string; state?: LiveState; children: ReactNode; extra?: ReactNode; wide?: boolean }) {
  return (
    <section className={`card${props.wide ? " wide" : ""}`}>
      <header>
        <h2>
          <span className="num">{props.n}</span> {props.title}
        </h2>
        {props.state && <StatusBadge state={props.state} />}
      </header>
      {props.extra}
      <pre className="sql">{props.sql}</pre>
      {props.state?.status === "error" ? <p className="error">{props.state.error}</p> : props.children}
    </section>
  );
}

export function StatusBadge({ state }: { state: LiveState }) {
  return <span className={`badge ${state.status}`}>{state.status}</span>;
}

export function fmt(v: unknown): string {
  if (typeof v === "number") return Number.isInteger(v) ? v.toLocaleString() : v.toLocaleString(undefined, { maximumFractionDigits: 2 });
  if (Array.isArray(v)) return `[${v.map(fmt).join(", ")}]`;
  if (v === null || v === undefined) return "∅";
  return String(v);
}

/** Highlights a row briefly whenever its contents change. */
function useFlash(row: Row): boolean {
  const sig = JSON.stringify(row);
  const [flash, setFlash] = useState(false);
  const [prev, setPrev] = useState(sig);
  if (sig !== prev) {
    setPrev(sig);
    setFlash(true);
  }
  useEffect(() => {
    if (!flash) return;
    const t = setTimeout(() => setFlash(false), 600);
    return () => clearTimeout(t);
  }, [flash, sig]);
  return flash;
}

function TableRow({ row, columns }: { row: Row; columns: string[] }) {
  const flash = useFlash(row);
  return (
    <tr className={flash ? "flash" : undefined}>
      {columns.map((c) => (
        <td key={c} className={typeof row[c] === "number" ? "numcell" : undefined}>
          {fmt(row[c])}
        </td>
      ))}
    </tr>
  );
}

export function LiveTable({ state }: { state: LiveState }) {
  const columns = state.columns.map((c) => c.name);
  if (state.status === "loading" && state.rows.length === 0) return <p className="muted">loading…</p>;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {state.columns.map((c) => (
              <th key={c.name} title={c.type}>
                {c.name}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {state.rows.map((r) => (
            <TableRow key={r.__key} row={r} columns={columns} />
          ))}
        </tbody>
      </table>
      {state.rows.length === 0 && <p className="muted">no rows</p>}
    </div>
  );
}

/** Tiny dependency-free line chart. */
export function LineChart(props: { points: { x: string; series: Record<string, number> }[]; colors: Record<string, string> }) {
  const W = 640;
  const H = 180;
  const pad = { l: 44, r: 8, t: 8, b: 20 };
  const names = Object.keys(props.colors);
  const max = Math.max(1, ...props.points.flatMap((p) => names.map((n) => p.series[n] ?? 0)));
  const n = props.points.length;
  const x = (i: number) => pad.l + (n <= 1 ? 0 : (i / (n - 1)) * (W - pad.l - pad.r));
  const y = (v: number) => H - pad.b - (v / max) * (H - pad.t - pad.b);
  return (
    <div>
      <svg viewBox={`0 0 ${W} ${H}`} className="chart" role="img">
        {[0, 0.5, 1].map((f) => (
          <g key={f}>
            <line x1={pad.l} x2={W - pad.r} y1={y(max * f)} y2={y(max * f)} className="grid" />
            <text x={pad.l - 6} y={y(max * f) + 4} textAnchor="end" className="axis">
              {fmt(Math.round(max * f))}
            </text>
          </g>
        ))}
        {n > 0 && (
          <>
            <text x={pad.l} y={H - 4} className="axis">{props.points[0].x.slice(11, 16)}</text>
            <text x={W - pad.r} y={H - 4} textAnchor="end" className="axis">{props.points[n - 1].x.slice(11, 16)}</text>
          </>
        )}
        {names.map((name) => (
          <polyline
            key={name}
            fill="none"
            stroke={props.colors[name]}
            strokeWidth={2}
            points={props.points.map((p, i) => `${x(i)},${y(p.series[name] ?? 0)}`).join(" ")}
          />
        ))}
      </svg>
      <div className="legend">
        {names.map((name) => (
          <span key={name}>
            <i style={{ background: props.colors[name] }} /> {name}
          </span>
        ))}
      </div>
    </div>
  );
}
