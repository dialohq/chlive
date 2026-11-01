# chlive-core as WebAssembly, against the TS client's ResultStore

`wasm && bun bench/wasm.ts`. Bun 1.4 on the dev VM, 2026-11-01. `chlive-wasm` is built with
`opt-level = "s"`, LTO, `panic = "abort"`, then `wasm-opt -Oz`. Rows look like the dev stack's
events (`ts DateTime64(3), user_id String, latency_ms UInt32`). Each measurement includes what
listeners need: a new rows array after every snapshot or diff.

## Size and load

| | raw | gzip -9 | brotli -q 11 |
|---|---|---|---|
| `opt-level = "s"` (kept) | 531 KB | 205 KB | 162 KB |
| `opt-level = "z"` | 477 KB | 191 KB | 154 KB |

Built-in timezones (chrono-tz) account for only about 5 KB. The rest is code: the plans, the
stream state, the aggregates and serde_json.

Load: compiling takes 3 ms and instantiating 0.4 ms.

## Snapshot (ms)

| rows | TS, dates parsed (the default) | TS, no parsers | WASM `apply(text)` | WASM `apply_value(obj)` | WASM `apply(text)` + every row out |
|---|---|---|---|---|---|
| 1k | 0.50 | 0.14 | 0.67 | 3.2 | 1.9 |
| 57k | 22.0 | 6.4 | 29.5 | 139 | 47.5 |
| 500k | 191 | 61 | 257 | 2548 | 393 |

## Diffs (ms per diff)

| 57k rows: an insert at an index plus a delete | median | p99 |
|---|---|---|
| TS, dates parsed | 0.72 | 0.84 |
| TS, no parsers | 0.51 | 0.71 |
| WASM `apply(text)`, changes mirrored into a new JS array | 0.38 | 0.75 |
| WASM `apply(text)` + every row out | 14.5 | 14.8 |

| 5k groups: an update by id that moves | median | p99 |
|---|---|---|
| TS | 0.34 | 0.42 |
| WASM `apply(text)` (changes out only) | 0.62 | 0.69 |

Both stores end with the same rows.

## Predicting (`order by latency_ms desc`, 57k rows)

| | seed from the rows (ms) | place one row, median (ms) | p99 (ms) |
|---|---|---|---|
| no LIMIT | 105 | 0.007 | 0.04 |
| LIMIT 200 | 0.4 | 0.004 | 0.009 |

## What it means

- **Size passes** the proposed bar: 205 KB gzipped, under 300 KB.
- **Snapshots are where WASM loses.** V8/JSC's `JSON.parse` builds JS objects 4–5× faster than
  serde_json builds Rust maps (6 ms against 30 ms at 57k rows). Either way, the UI needs JS
  objects:
  - Handing every row out of WASM costs another full serialize and parse (47 ms, and 14 ms per
    diff if repeated).
  - Mirroring changes keeps every row twice, once in WASM and once in JS.
  - Passing parsed JS values in through serde-wasm-bindgen is the slowest of all.
- **Diffs are a draw.** Applying them costs well under a millisecond either way; the O(n) copy
  that gives listeners a new array dominates.
- **Predicting is very cheap**: microseconds per pending row. Seeding is the only real cost:
  105 ms for an unlimited 57k-row ordered result (it parses every row), and under a
  millisecond with a LIMIT. Seeding only the sort-key columns, rather than whole rows, would
  cut it.

**Recommendation:** the TS `ResultStore` keeps the rows, and WASM only predicts. An optimistic
subscription loads the WASM module and its plan when first used, and seeds it from the rows
`ResultStore` already holds. It uses the same core as the server, with no second copy of the
rows on the hot path. `chlive-core`'s `ResultRows` remains the single Rust implementation, for
the Rust client and the server's hub.
