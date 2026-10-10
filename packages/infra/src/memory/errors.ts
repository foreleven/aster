import { Data } from "effect";

export class MemoryStartupError extends Data.TaggedError("MemoryStartupError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export class MemoryConnectionError extends Data.TaggedError("MemoryConnectionError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export class MemoryConfigurationError extends Data.TaggedError("MemoryConfigurationError")<{
  readonly message: string;
}> {}

export class MemoryProtocolError extends Data.TaggedError("MemoryProtocolError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}
