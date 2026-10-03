import { Schema } from "effect";
import { CommandIdentifier } from "./command.js";

export const GoalIntent = Schema.Struct({
  intentId: Schema.NonEmptyString,
  goalSlug: Schema.String,
  source: Schema.Struct({
    contextPath: Schema.String,
    actorPath: Schema.String,
    name: Schema.String,
    kind: Schema.Literal("lark-chat"),
  }),
  content: Schema.Struct({
    summary: Schema.String,
    summaryRevision: Schema.String,
    summaryFingerprint: Schema.String,
  }),
  relevance: Schema.Struct({
    score: Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
    rationale: Schema.String,
    screeningRecordId: Schema.String,
    threshold: Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
    policyVersion: Schema.String,
  }),
  createdAt: Schema.String,
});
export type GoalIntent = typeof GoalIntent.Type;

export const GoalInputPayload = Schema.Union([
  Schema.TaggedStruct("GoalIntent", { intent: GoalIntent }),
  Schema.TaggedStruct("UserInput", { text: Schema.NonEmptyString }),
  Schema.TaggedStruct("PersonalMessage", {
    text: Schema.NonEmptyString,
    source: Schema.Literal("/personal"),
    requestId: Schema.String,
  }),
  Schema.TaggedStruct("SignalOccurrence", {
    occurrenceId: Schema.String,
    signalPath: Schema.String,
    evidence: Schema.String,
  }),
  Schema.TaggedStruct("ExecutionFeedback", {
    runPath: Schema.String,
    taskId: Schema.optional(Schema.String),
    evaluationId: Schema.optional(Schema.String),
    status: Schema.String,
    terminal: Schema.Boolean,
    text: Schema.String,
  }),
  Schema.TaggedStruct("Startup", { reason: Schema.String }),
]);
export type GoalInputPayload = typeof GoalInputPayload.Type;
export const GoalInput = Schema.Struct({
  inputId: Schema.NonEmptyString,
  goalSlug: Schema.NonEmptyString,
  ordinal: Schema.Int.check(Schema.isGreaterThan(0)),
  receivedAt: Schema.NonEmptyString,
  payload: GoalInputPayload,
});
export type GoalInput = typeof GoalInput.Type;

export const GoalTaskOutput = Schema.Struct({
  id: Schema.NonEmptyString,
  taskId: Schema.String,
  operation: Schema.String,
  title: Schema.String,
  runPath: Schema.optional(Schema.String),
});
export type GoalTaskOutput = typeof GoalTaskOutput.Type;

export const GoalTimelineOutput = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literals(["task", "signal"]),
  target: Schema.String,
  operation: Schema.String,
  title: Schema.String,
  status: Schema.String,
  runPath: Schema.optional(Schema.String),
  attempts: Schema.optional(Schema.Int),
  error: Schema.optional(Schema.String),
});
export const GoalTimelineGroup = Schema.Struct({
  evaluationId: Schema.NonEmptyString,
  ordinal: Schema.Int,
  status: Schema.Literals([
    "pending",
    "running",
    "failed",
    "reconciliation_required",
    "completed",
    "partially_applied",
  ]),
  retryOf: Schema.optional(Schema.String),
  startedAt: Schema.String,
  finishedAt: Schema.optional(Schema.String),
  inputs: Schema.Array(GoalInput),
  disposition: Schema.optional(Schema.Literals(["advance", "no_change", "ignored"])),
  conclusion: Schema.optional(
    Schema.Struct({
      text: Schema.String,
      evidence: Schema.Array(Schema.String),
      applied: Schema.Boolean,
    }),
  ),
  outputs: Schema.Array(GoalTimelineOutput),
  error: Schema.optional(Schema.String),
  agentRun: Schema.Struct({ sessionId: Schema.String, requestId: Schema.String }),
});
export type GoalTimelineGroup = typeof GoalTimelineGroup.Type;
export const GoalTimelinePage = Schema.Struct({
  groups: Schema.Array(GoalTimelineGroup),
  pendingInputs: Schema.Array(GoalInput),
  total: Schema.Int,
  nextBefore: Schema.NullOr(Schema.Int),
});
export type GoalTimelinePage = typeof GoalTimelinePage.Type;

/** A user-authorized replay of one frozen, idempotent Signal command. */
export const RetryGoalSignalInput = Schema.Struct({
  slug: Schema.NonEmptyString,
  requestId: CommandIdentifier,
  operationId: CommandIdentifier,
  expectedAttempts: Schema.Int.check(Schema.isGreaterThan(0)),
});
export type RetryGoalSignalInput = typeof RetryGoalSignalInput.Type;
