/**
 * Optimistic inserts: the client's own rows, shown in its live results before the server has
 * them, exactly where the server's diffs will put them. Each optimistic subscription gets its
 * plan from the server (`plan`); chlive-core, the server's own code as WebAssembly, places the
 * pending rows by it over the rows the client has. Loaded only when a plan arrives.
 */

import type { Change, Row } from "./protocol.ts";

type Wasm = typeof import("../wasm/chlive_wasm.js");
type Predictor = InstanceType<Wasm["Predictor"]>;

let wasm: Wasm | undefined;
let loading: Promise<Wasm> | undefined;

/** chlive-core as WebAssembly, once loaded. */
export function loadedWasm(): Wasm | undefined {
  return wasm;
}

/** Load chlive-core as WebAssembly (once). */
export function loadWasm(): Promise<Wasm> {
  loading ??= import("../wasm/chlive_wasm.js").then(async (m) => {
    await m.default();
    wasm = m;
    return m;
  });
  return loading;
}

/** An optimistic insert's rows, until the results over its table have them. */
export interface PendingRows {
  table: string;
  rows: Row[];
  /** Each row's Kafka `[partition, offset]`, once the server acknowledged them. */
  offsets?: [number, number][];
  /** `Date.now()` then. */
  ackedAt?: number;
}

/** Whether a result complete to cursor `at` has every row of an acknowledged insert. */
export function covered(w: Wasm, at: string | undefined, p: PendingRows): boolean {
  return at !== undefined && p.offsets !== undefined && p.offsets.every(([part, offset]) => w.covers(at, part, offset));
}

/**
 * One result's predictions. The predictor keeps a copy of the result's rows (what placing needs
 * of them): seeded when first asked for a view, then kept in step with the server's diffs, and
 * let go with {@link free}.
 */
export class Prediction {
  private predictor: Predictor | undefined;
  private readonly w: Wasm;
  private readonly plan: string;
  private readonly user: string | undefined;

  /** Throws if chlive-core can't run the plan (e.g. a timezone the WebAssembly build hasn't). */
  constructor(w: Wasm, plan: unknown, user: string | undefined) {
    this.w = w;
    this.plan = JSON.stringify(plan);
    this.user = user;
    // fails now rather than at the first insert
    new w.Predictor(this.plan, user).free();
  }

  /** A server diff for the result, as it came (its text). */
  apply(diff: string): void {
    if (!this.predictor) return;
    try {
      this.predictor.apply(diff);
    } catch {
      // out of step: seeded again when next needed
      this.free();
    }
  }

  /**
   * The changes that place the pending rows (in order) in the result's rows; `rows` gives them
   * (with their ids) when the predictor has to be seeded.
   */
  view(rows: () => { rows: Row[]; ids?: string[] }, pending: PendingRows[], now = Date.now()): Change[] {
    if (!this.predictor) {
      const p = new this.w.Predictor(this.plan, this.user);
      const seed = rows();
      p.seed(JSON.stringify(seed.rows), seed.ids && JSON.stringify(seed.ids));
      this.predictor = p;
    }
    const items = pending.flatMap((p) => p.rows.map((row, i) => ({ row, at: p.offsets?.[i] })));
    return JSON.parse(this.predictor.view(JSON.stringify(items), now)) as Change[];
  }

  /** Whether it holds a copy of the rows. */
  get seeded(): boolean {
    return this.predictor !== undefined;
  }

  free(): void {
    this.predictor?.free();
    this.predictor = undefined;
  }
}
