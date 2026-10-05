import { Context, Effect, Layer } from "effect";
import type { ContextInput } from "../context/model.js";
import type { ContextCapture } from "./contracts.js";

export interface CapturePolicy {
  readonly matches: (path: string) => boolean;
  readonly capture: (record: ContextInput) => Effect.Effect<ContextCapture | undefined>;
}
export class ContextCaptures extends Context.Service<
  ContextCaptures,
  {
    readonly register: (policies: readonly CapturePolicy[]) => Effect.Effect<void>;
    readonly select: (record: ContextInput) => Effect.Effect<ContextCapture | undefined>;
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
        [...policies].find((policy) => policy.matches(record.path))?.capture(record) ??
        Effect.succeed(undefined),
    };
  });
}
