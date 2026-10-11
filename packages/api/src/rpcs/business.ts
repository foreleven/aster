import { ApplicationError, SignalTrigger, SignalAction, TaskTitle } from "@aster/core/contracts";
import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/rpc";

export const GoalOverview = Schema.Struct({
  path: Schema.String,
  revision: Schema.Int,
  slug: Schema.String,
  title: Schema.String,
  description: Schema.String,
  status: Schema.Literals(["active", "completed"]),
  summary: Schema.String,
  tasks: Schema.Array(Schema.String),
  retryableInputId: Schema.optional(Schema.String),
  lastError: Schema.optional(Schema.String),
});
export type GoalOverview = typeof GoalOverview.Type;
export const TaskOverview = Schema.Struct({
  path: Schema.String,
  title: TaskTitle,
  revision: Schema.Int,
  description: Schema.String,
  status: Schema.String,
  sourcePath: Schema.String,
  replyTo: Schema.String,
  agent: Schema.String,
  inputs: Schema.Int,
});
export type TaskOverview = typeof TaskOverview.Type;
export const SignalOverview = Schema.Struct({
  path: Schema.String,
  revision: Schema.Int,
  description: Schema.String,
  owner: Schema.optional(Schema.String),
  status: Schema.Literals(["active", "paused", "deleted"]),
  trigger: SignalTrigger,
  action: SignalAction,
  version: Schema.Int,
  nextDue: Schema.optional(Schema.NullOr(Schema.String)),
});
export type SignalOverview = typeof SignalOverview.Type;
export const BusinessRpcs = RpcGroup.make(
  Rpc.make("ListTasks", { success: Schema.Array(TaskOverview), error: ApplicationError }),
  Rpc.make("ListSignals", { success: Schema.Array(SignalOverview), error: ApplicationError }),
);
