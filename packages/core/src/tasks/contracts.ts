import { Schema } from "effect";

import { CommandIdentifier, ContextRevision } from "../operations.js";
import { PublicContext } from "../context/contracts.js";

import { GoalPath } from "../goals/contracts.js";
export const RemainingAgentTurns = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 4 }));
export type RemainingAgentTurns = typeof RemainingAgentTurns.Type;
export const PreparedTask = Schema.Struct({
  instructions: Schema.NonEmptyString,
  input: Schema.Array(
    Schema.Struct({ content: Schema.String, sources: Schema.Array(Schema.String) }),
  ),
});
export type PreparedTask = typeof PreparedTask.Type;
/** A Task is a message with a typed Actor destination. External execution replies to a Goal. */
export const Task = Schema.TaggedUnion({
  Goal: { target: GoalPath, text: Schema.NonEmptyString },
  Agent: { task: PreparedTask, replyTo: GoalPath },
  Delegate: {
    agent: Schema.NonEmptyString,
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
  evidence: Schema.optional(PublicContext),
});
export type TaskMessage = typeof TaskMessage.Type;
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
  evidence: Schema.optional(PublicContext),
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
