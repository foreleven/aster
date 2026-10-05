import { Schema } from "effect";
import { CausalChain } from "./causal.js";
import { CommandIdentifier, CommandReceipt } from "./command.js";
import { TaskAction } from "./writeback.js";
import { PublicContext } from "./models.js";

export const GoalPath = Schema.String.check(Schema.isPattern(/^\/goals\/[a-z0-9][a-z0-9-]*$/));
export const PreparedTask = Schema.Struct({
  instructions: Schema.NonEmptyString,
  input: Schema.Array(
    Schema.Struct({ content: Schema.String, sources: Schema.Array(Schema.String) }),
  ),
});
export type PreparedTask = typeof PreparedTask.Type;
/** A Task is a message with a typed Actor destination. External execution replies to a Goal. */
export const Task = Schema.Union([
  Schema.TaggedStruct("Goal", { target: GoalPath, text: Schema.NonEmptyString }),
  Schema.TaggedStruct("Delegate", {
    agent: Schema.NonEmptyString,
    task: PreparedTask,
    replyTo: GoalPath,
    action: Schema.optional(TaskAction),
  }),
]);
export type Task = typeof Task.Type;
export const TaskMessage = Schema.Struct({
  requestId: CommandIdentifier,
  source: Schema.String.check(Schema.isPattern(/^\/(?:goals|signals)\/[a-z0-9][a-z0-9-]*$/)),
  task: Task,
  createdAt: Schema.NonEmptyString,
  causal: CausalChain,
  evidence: Schema.optional(PublicContext),
});
export type TaskMessage = typeof TaskMessage.Type;
/** Internal admission into the durable external execution owner. */
export const TaskDeliveryInput = Schema.Struct({
  requestId: CommandIdentifier,
  source: TaskMessage.fields.source,
  target: Schema.String.check(Schema.isPattern(/^\/runs\/[a-f0-9]{64}$/)),
  createdAt: Schema.NonEmptyString,
  agent: Schema.NonEmptyString,
  task: PreparedTask,
  replyTo: GoalPath,
  causal: CausalChain,
  action: Schema.optional(TaskAction),
  evidence: Schema.optional(PublicContext),
});
export type TaskDeliveryInput = typeof TaskDeliveryInput.Type;
export const TaskAdmission = Schema.Struct({ input: TaskDeliveryInput, receipt: CommandReceipt });
