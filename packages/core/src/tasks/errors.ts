import { Data } from "effect";

export class TaskPreparationError extends Data.TaggedError("TaskPreparationError")<{
  readonly operation: "prepare" | "readiness";
  readonly message: string;
  readonly cause: unknown;
}> {}

/** Transport/protocol failures do not imply that an external operation had no side effect. */
export class ExternalAgentError extends Data.TaggedError("ExternalAgentError")<{
  readonly operation: "submit" | "status" | "resume" | "wait" | "respond";
  readonly message: string;
  readonly cause?: unknown;
}> {}
