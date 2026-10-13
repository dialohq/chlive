import type { Column, Row } from "./protocol.ts";

/**
 * A column's ClickHouse type, taken apart: `Nullable(DateTime64(3, 'UTC'))` is
 * `{ type: "Nullable(DateTime64(3, 'UTC'))", base: "DateTime64", args: ["3", "'UTC'"], nullable: true }`.
 * `LowCardinality` is unwrapped too.
 */
export interface ClickHouseType {
  type: string;
  base: string;
  args: string[];
  nullable: boolean;
}

/**
 * Turns one value of a column, as ClickHouse renders it in JSON (never null), into what rows
 * hold. Throwing fails the subscription (`CLIENT_ERROR`).
 */
export type ValueParser = (value: unknown, type: ClickHouseType) => unknown;

/** Parsers by base type name (`DateTime64`, `UUID`, …); `null` turns a default one off. */
export type ValueParsers = Record<string, ValueParser | null>;

const DATE = /^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?)?Z?$/;

/**
 * ClickHouse's `YYYY-MM-DD[ hh:mm:ss[.f…]]` (or ISO, with `T` and `Z`) as a `Date`, read as UTC.
 * Digits past the millisecond are dropped. Throws on anything else. For custom parsers to build
 * on, e.g. `spacetime(parseClickHouseDate(v).getTime())`.
 */
export function parseClickHouseDate(s: string): Date {
  const m = DATE.exec(s);
  if (!m) throw new Error(`not a ClickHouse date or time: ${JSON.stringify(s)}`);
  const [, y, mo, d, h = "0", mi = "0", sec = "0", frac = ""] = m;
  const ms = Number((frac + "00").slice(0, 3));
  const date = new Date(Date.UTC(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi), Number(sec), ms));
  // Date.UTC maps years 0-99 to 1900-1999
  if (Number(y) < 100) date.setUTCFullYear(Number(y));
  return date;
}

const toDate: ValueParser = (v) => parseClickHouseDate(String(v));

/** `Date`, `Date32`, `DateTime` and `DateTime64` as JS `Date`s (UTC; DateTime64 to the millisecond). */
export const defaultParsers: Readonly<ValueParsers> = Object.freeze({
  Date: toDate,
  Date32: toDate,
  DateTime: toDate,
  DateTime64: toDate,
});

/** Takes a ClickHouse type name apart (see `ClickHouseType`). */
export function parseClickHouseType(type: string): ClickHouseType {
  let t = type.trim();
  let nullable = false;
  for (;;) {
    const inner = unwrap(t, "Nullable") ?? unwrap(t, "LowCardinality");
    if (inner === undefined) break;
    if (t.startsWith("Nullable")) nullable = true;
    t = inner;
  }
  const open = t.indexOf("(");
  if (open < 0 || !t.endsWith(")")) return { type, base: t, args: [], nullable };
  return { type, base: t.slice(0, open), args: splitArgs(t.slice(open + 1, -1)), nullable };
}

function unwrap(t: string, name: string): string | undefined {
  return t.startsWith(name + "(") && t.endsWith(")") ? t.slice(name.length + 1, -1).trim() : undefined;
}

/** Top-level, comma-separated arguments (nested types and quoted strings kept whole). */
function splitArgs(s: string): string[] {
  const args: string[] = [];
  let depth = 0;
  let quoted = false;
  let start = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (quoted) {
      if (c === "\\") i++;
      else if (c === "'") quoted = false;
    } else if (c === "'") quoted = true;
    else if (c === "(") depth++;
    else if (c === ")") depth--;
    else if (c === "," && depth === 0) {
      args.push(s.slice(start, i).trim());
      start = i + 1;
    }
  }
  const last = s.slice(start).trim();
  if (last || args.length) args.push(last);
  return args;
}

/** The parsers in effect: the defaults with the option's merged over them, `null`s removed. */
export function resolveParsers(option: ValueParsers | false | undefined): ValueParsers {
  if (option === false) return {};
  const merged: ValueParsers = { ...defaultParsers, ...option };
  for (const [k, v] of Object.entries(merged)) if (!v) delete merged[k];
  return merged;
}

/** One column with a parser. */
export interface ColumnParser {
  name: string;
  parse: ValueParser;
  type: ClickHouseType;
}

/** The columns that have a parser. */
export function columnParsers(columns: Column[], parsers: ValueParsers): ColumnParser[] {
  const out: ColumnParser[] = [];
  for (const c of columns) {
    const type = parseClickHouseType(c.type);
    const parse = Object.hasOwn(parsers, type.base) ? parsers[type.base] : undefined;
    if (parse) out.push({ name: c.name, parse, type });
  }
  return out;
}

/** A copy of the row with its parsed columns replaced; null and missing values stay. */
export function parseRow<R extends Row = Row>(row: Row, cols: ColumnParser[]): R {
  const out: Row = { ...row };
  for (const c of cols) {
    const v = row[c.name];
    if (v !== null && v !== undefined) out[c.name] = c.parse(v, c.type);
  }
  return out as R;
}
