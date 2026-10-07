import { Context, Effect, Layer } from "effect";
import type { ContextInput } from "../context/model.js";
import type { ContextCapture } from "./contracts.js";

/** Identity selection is cheap; evidence is read only for an uncaptured session. */
export interface CapturePlan {
  readonly sessionId: string;
  readonly records: Effect.Effect<ContextCapture["records"] | undefined>;
}

export interface CapturePolicy {
  readonly matches: (path: string) => boolean;
  readonly capture: (record: ContextInput) => CapturePlan | undefined;
}
export class ContextCaptures extends Context.Service<
  ContextCaptures,
  {
    readonly register: (policies: readonly CapturePolicy[]) => Effect.Effect<void>;
    readonly select: (record: ContextInput) => CapturePlan | undefined;
  }
>()("memory/ContextCaptures") {
  static readonly layer = Layer.sync(ContextCaptures, () => {
    const policies = new Set<CapturePolicy>();
    return {
      register: (values) =>
        Effect.sync(() => {
          for (const policy of values) policies.add(policy);
        }),
      select: (record) =>
        [...policies].find((policy) => policy.matches(record.path))?.capture(record),
    };
  });
}
