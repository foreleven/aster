import { Schema } from "effect";
export const GoalPath = Schema.String.check(Schema.isPattern(/^\/goals\/[a-z0-9][a-z0-9-]*$/));

export const GoalIntent = Schema.Struct({
  intentId: Schema.NonEmptyString,
  source: Schema.Struct({
    contextPath: Schema.String,
    name: Schema.String,
  }),
  content: Schema.Struct({
    summary: Schema.String,
  }),
  relevance: Schema.Struct({
    score: Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
    rationale: Schema.String,
    threshold: Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
  }),
  createdAt: Schema.String,
});
export type GoalIntent = typeof GoalIntent.Type;

export const GoalExecutionFeedback = Schema.TaggedStruct("ExecutionFeedback", {
  taskPath: Schema.String,
  status: Schema.String,
  text: Schema.String,
});

export const GoalInputPayload = Schema.TaggedUnion({
  GoalIntent: { intent: GoalIntent },
  UserInput: { text: Schema.NonEmptyString },
  TaskMessage: {
    requestId: Schema.String,
    source: Schema.String,
    text: Schema.String,
  },
  ExecutionFeedback: GoalExecutionFeedback.fields,
  GoalStarted: {},
});
export type GoalInputPayload = typeof GoalInputPayload.Type;
