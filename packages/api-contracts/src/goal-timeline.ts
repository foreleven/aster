import { Schema } from "effect";
import { CommandIdentifier } from "./command.js";

export const GoalIntent = Schema.Struct({
  intentId: Schema.NonEmptyString,
  goalSlug: Schema.String,
  source: Schema.Struct({
    contextPath: Schema.String,
    actorPath: Schema.String,
    name: Schema.String,
    kind: Schema.Literal("context"),
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
  Schema.TaggedStruct("TaskMessage", {
    requestId: Schema.String,
    source: Schema.String,
    text: Schema.String,
  }),
  Schema.TaggedStruct("ExecutionFeedback", {
    runPath: Schema.String,
    status: Schema.String,
    terminal: Schema.Boolean,
    text: Schema.String,
  }),
  Schema.TaggedStruct("GoalStarted", { pursuit: Schema.NonEmptyString }),
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

export const RetryGoalTurnInput = Schema.Struct({
  slug: Schema.NonEmptyString,
  requestId: CommandIdentifier,
  turnId: CommandIdentifier,
});
export type RetryGoalTurnInput = typeof RetryGoalTurnInput.Type;

/** Public delivery history; Pi's native tool transcript stays private. */
export const GoalTimelineGroup = Schema.Struct({
  requestId: Schema.NonEmptyString,
  ordinal: Schema.Int,
  status: Schema.Literals(["pending", "running", "completed", "failed", "unknown", "ignored"]),
  retryOf: Schema.optional(Schema.String),
  input: GoalInput,
  response: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
});
export type GoalTimelineGroup = typeof GoalTimelineGroup.Type;
export const GoalTimelinePage = Schema.Struct({
  groups: Schema.Array(GoalTimelineGroup),
  total: Schema.Int,
  nextBefore: Schema.NullOr(Schema.Int),
});
export type GoalTimelinePage = typeof GoalTimelinePage.Type;
