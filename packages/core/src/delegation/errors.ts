import { Data } from "effect";

export class DelegationError extends Data.TaggedError("DelegationError")<{
  readonly path: string;
  readonly message: string;
  readonly cause: unknown;
}> {}
