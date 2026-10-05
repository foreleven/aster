import { Data } from "effect";

/** Transport/protocol failures do not imply that an external operation had no side effect. */
export class ExternalAgentError extends Data.TaggedError("ExternalAgentError")<{
  readonly operation: "submit" | "status" | "resume" | "wait" | "respond" | "lookup";
  readonly message: string;
  readonly cause?: unknown;
}> {}
