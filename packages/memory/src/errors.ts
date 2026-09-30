import { Data } from "effect";

export class MemoryStartupError extends Data.TaggedError("MemoryStartupError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export class MemoryCaptureError extends Data.TaggedError("MemoryCaptureError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export class MemoryConnectionError extends Data.TaggedError("MemoryConnectionError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export { MemoryRecallError } from "@aster/core";
