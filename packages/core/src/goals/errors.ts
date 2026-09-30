import { Data } from "effect";

export class GoalOperationError extends Data.TaggedError("GoalOperationError")<{
  readonly goal: string;
  readonly operation: "plan" | "reconcile" | "deactivate" | "edit-signal";
  readonly message: string;
  readonly cause?: unknown;
}> {}

/** Expected model, output-validation or compaction failure; defects remain defects. */
export class GoalReasoningError extends Data.TaggedError("GoalReasoningError")<{
  readonly operation: "plan" | "compact";
  readonly message: string;
  readonly cause?: unknown;
}> {}
