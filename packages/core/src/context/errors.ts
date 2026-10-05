import { Data, Schema } from "effect";

export class ContextConflict extends Schema.TaggedError<ContextConflict>()("ContextConflict", {
  path: Schema.String,
  expectedRevision: Schema.Int,
  actualRevision: Schema.Int,
}) {}

export class ContextValidationError extends Data.TaggedError("ContextValidationError")<{
  readonly path: string;
  readonly cause: unknown;
}> {}

/** A failed commit may have left a durable pending file; the owner must recover before retry. */
export class ContextCommitError extends Data.TaggedError("ContextCommitError")<{
  readonly path: string;
  readonly cause: unknown;
}> {}

/** Loading or reconciling storage failed; the uncertain owner remains fenced. */
export class ContextRecoveryError extends Data.TaggedError("ContextRecoveryError")<{
  readonly path: string;
  readonly cause: unknown;
}> {}
