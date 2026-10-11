import { Context, Data, Effect, Schema } from "effect";
import type { ContextText } from "../context/contracts.js";

const NonBlank = Schema.String.check(Schema.isPattern(/\S/));
const MemoryType = Schema.Literals([
  "pattern",
  "preference",
  "architecture",
  "bug",
  "workflow",
  "fact",
]);
const StringList = Schema.Array(NonBlank).pipe(Schema.withDecodingDefaultKey(Effect.succeed([])));

export const MemorySaveInput = Schema.Struct({
  content: NonBlank.check(Schema.isMaxLength(12000)),
  type: MemoryType.pipe(Schema.withDecodingDefaultKey(Effect.succeed("fact"))),
  concepts: StringList,
  files: StringList,
});
export type MemorySaveInput = typeof MemorySaveInput.Type;
export const MemorySaveRequest = Schema.Struct({
  requestId: Schema.NonEmptyString,
  source: NonBlank,
  input: MemorySaveInput,
});
export type MemorySaveRequest = typeof MemorySaveRequest.Type;

export const MemoryWriteReceipt = Schema.Struct({ memoryId: Schema.NonEmptyString });
export type MemoryWriteReceipt = typeof MemoryWriteReceipt.Type;

export interface ContextCapture {
  readonly sessionId: string;
  readonly records: readonly ContextText[];
}

export class MemoryRecallError extends Data.TaggedError("MemoryRecallError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export class MemoryCaptureError extends Data.TaggedError("MemoryCaptureError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export class MemoryWriteError extends Data.TaggedError("MemoryWriteError")<{
  readonly message: string;
  readonly outcome: "failed" | "unknown";
  readonly cause?: unknown;
}> {}

export interface MemoryRecallOperations {
  readonly search: (query: string) => Effect.Effect<unknown, MemoryRecallError>;
  readonly expand: (
    ids: readonly { obsId: string; sessionId?: string }[],
  ) => Effect.Effect<unknown, MemoryRecallError>;
}

/** Backend capabilities and public metadata, independent of transport and daemon settings. */
export class MemoryBackend extends Context.Service<
  MemoryBackend,
  {
    readonly description: string;
    readonly retrieval: "bm25" | "hybrid";
    readonly llm?: { readonly provider: string; readonly model: string };
    readonly recall: MemoryRecallOperations;
    readonly capture: (input: ContextCapture) => Effect.Effect<void, MemoryCaptureError>;
    readonly save: (input: MemorySaveInput) => Effect.Effect<MemoryWriteReceipt, MemoryWriteError>;
    /** Join admitted backend operations after Actor workers stop; interruption is not cancellation. */
    readonly drain: Effect.Effect<void>;
  }
>()("memory/Backend") {}
