import { Data } from "effect";

export class ContextDescriptionError extends Data.TaggedError("ContextDescriptionError")<{
  readonly path: string;
  readonly message: string;
  readonly cause: unknown;
}> {}

export class GoalScreeningError extends Data.TaggedError("GoalScreeningError")<{
  readonly path: string;
  readonly message: string;
  readonly cause: unknown;
}> {}
