import { Context, Data, Effect } from "effect";
import type { PublicContext } from "@aster/api-contracts";
export interface ContextCapture {
  readonly sessionId: string;
  readonly records: readonly PublicContext[];
}

export class MemoryRecallError extends Data.TaggedError("MemoryRecallError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export class MemoryCaptureError extends Data.TaggedError("MemoryCaptureError")<{
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

/** Backend capabilities and public metadata, independent of transport and daemon settings. */
export class MemoryBackend extends Context.Service<
  MemoryBackend,
  {
    readonly description: string;
    readonly retrieval: "bm25" | "hybrid";
    readonly llm?: { readonly provider: string; readonly model: string };
    readonly recall: MemoryRecall["Service"];
    readonly capture: (input: ContextCapture) => Effect.Effect<void, MemoryCaptureError>;
    /** Join admitted backend operations after Actor workers stop; interruption is not cancellation. */
    readonly drain: Effect.Effect<void>;
  }
>()("memory/Backend") {}

export class ContextCaptureSink extends Context.Service<
  ContextCaptureSink,
  {
    readonly capture: (input: ContextCapture) => Effect.Effect<void>;
    readonly drain: Effect.Effect<void>;
  }
>()("context/CaptureSink") {}
