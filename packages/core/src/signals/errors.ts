import { Data } from "effect";

export class SignalDetectionError extends Data.TaggedError("SignalDetectionError")<{
  readonly path: string;
  readonly message: string;
  readonly cause?: unknown;
}> {}
