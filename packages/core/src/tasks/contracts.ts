import { Effect, Schema } from "effect";

import { CommandIdentifier, ContextRevision } from "../operations.js";
import { ContextText } from "../context/contracts.js";

import { GoalPath } from "../goals/contracts.js";
export const RemainingAgentTurns = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 4 }));
export type RemainingAgentTurns = typeof RemainingAgentTurns.Type;
export const PublicContextPath = Schema.String.check(
  Schema.isPattern(/^\/(?:[^\s/?#]+(?:\/[^\s/?#]+)*)?$/),
);
export const TaskEvidence = Schema.Struct({
  content: Schema.String,
  sources: Schema.Array(Schema.String).pipe(Schema.withDecodingDefaultKey(Effect.succeed([]))),
});
export const InternalTaskInput = Schema.Struct({
  instructions: Schema.NonEmptyString,
  contextPaths: Schema.Array(PublicContextPath).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed([])),
  ),
  evidence: Schema.Array(TaskEvidence).pipe(Schema.withDecodingDefaultKey(Effect.succeed([]))),
});
export type InternalTaskInput = typeof InternalTaskInput.Type;
export const ExternalExecutorName = Schema.NonEmptyString.check(
  Schema.makeFilter((value) => value !== "internal", {
    expected: "An external executor name; internal work uses start_task",
  }),
);
export const ExternalTaskInput = Schema.Struct({
  executor: ExternalExecutorName,
  instructions: Schema.NonEmptyString,
  evidence: InternalTaskInput.fields.evidence,
});
export type ExternalTaskInput = typeof ExternalTaskInput.Type;
export const PreparedTask = Schema.Struct({
  instructions: Schema.NonEmptyString,
  contextPaths: InternalTaskInput.fields.contextPaths,
  input: Schema.Array(Schema.toType(TaskEvidence)),
});
export type PreparedTask = typeof PreparedTask.Type;
export const prepareInternalTask = (input: InternalTaskInput): PreparedTask => ({
  instructions: input.instructions,
  contextPaths: [...new Set(input.contextPaths)],
  input: input.evidence,
});
export const prepareExternalTask = (input: ExternalTaskInput): PreparedTask => ({
  instructions: input.instructions,
  contextPaths: [],
  input: input.evidence,
});
/** Concrete work executed internally or by an external executor; feedback belongs to a Goal. */
export const Task = Schema.TaggedUnion({
  Agent: { task: PreparedTask, replyTo: GoalPath },
  Delegate: {
    agent: ExternalExecutorName,
    task: PreparedTask,
    replyTo: GoalPath,
  },
});
export type Task = typeof Task.Type;
export const TaskMessage = Schema.Struct({
  requestId: CommandIdentifier,
  source: Schema.String.check(Schema.isPattern(/^\/(?:goals|signals)\/[a-z0-9][a-z0-9-]*$/)),
  task: Task,
  createdAt: Schema.NonEmptyString,
  remainingAgentTurns: RemainingAgentTurns,
  evidence: Schema.optional(ContextText),
});
export type TaskMessage = typeof TaskMessage.Type;
export type TaskOrigin = Pick<
  TaskMessage,
  "source" | "requestId" | "createdAt" | "remainingAgentTurns"
>;
/** Internal admission into the durable external execution owner. */
export const TaskDeliveryInput = Schema.Struct({
  requestId: CommandIdentifier,
  source: TaskMessage.fields.source,
  target: Schema.String.check(Schema.isPattern(/^\/tasks\/[a-f0-9]{64}$/)),
  createdAt: Schema.NonEmptyString,
  agent: Schema.NonEmptyString,
  task: PreparedTask,
  replyTo: GoalPath,
  remainingAgentTurns: RemainingAgentTurns,
  evidence: Schema.optional(ContextText),
});
export type TaskDeliveryInput = typeof TaskDeliveryInput.Type;

export const TaskPath = Schema.String.check(Schema.isPattern(/^\/tasks\/[a-f0-9]{64}$/));
/** Explicit operator authorization, addressed directly to the execution owner. */
export const TaskRecoveryInput = Schema.Struct({
  requestId: CommandIdentifier,
  target: TaskPath,
  expectedRevision: ContextRevision,
});
export type TaskRecoveryInput = typeof TaskRecoveryInput.Type;
export const FollowupTaskInput = Schema.Struct({
  requestId: CommandIdentifier,
  target: TaskPath,
  source: Schema.String.check(Schema.isPattern(/^\/goals\/[a-z0-9][a-z0-9-]*$/)),
  text: Schema.NonEmptyString,
});
export type FollowupTaskInput = typeof FollowupTaskInput.Type;
