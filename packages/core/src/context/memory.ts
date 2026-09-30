import { Context, Data, Effect } from "effect";
import type { ContextCapture } from "./model.js";

export class MemoryRecallError extends Data.TaggedError("MemoryRecallError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

/** Backend connections and daemon settings stay in the adapter. */
export class MemoryRecall extends Context.Service<
  MemoryRecall,
  {
    readonly search: (query: string) => Effect.Effect<unknown, MemoryRecallError>;
    readonly expand: (
      ids: readonly { obsId: string; sessionId?: string }[],
    ) => Effect.Effect<unknown, MemoryRecallError>;
  }
>()("memory/Recall") {}

export class ContextCaptureSink extends Context.Service<
  ContextCaptureSink,
  {
    readonly capture: (input: ContextCapture) => Effect.Effect<void>;
    readonly drain: Effect.Effect<void>;
  }
>()("context/CaptureSink") {}
