import {
  ApplicationError,
  ApprovalResponse,
  PublicContext,
  PublicApprovalEntry,
  TaskRecoveryInput,
  ContextQueryInput,
  ContextQueryResult,
  ContextQueryError,
  RecoveryInput,
  TaskPath,
  CommandReceipt,
} from "@aster/core/contracts";

import { ProcessingOwner, ProcessingSnapshot } from "./recovery.js";
export * from "./recovery.js";
import { GoalTimelinePage, RetryGoalTurnInput } from "./goal-timeline.js";
export * from "./goal-timeline.js";
import { TaskInspection } from "./task-inspection.js";

export * from "./task-inspection.js";

import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/rpc";
import { RuntimeSnapshot } from "./models.js";
export * from "./models.js";

import { QueryInvalidation } from "./changes.js";
export * from "./changes.js";
export const ApplicationRpcs = RpcGroup.make(
  Rpc.make("SubscribeInvalidations", {
    success: QueryInvalidation,
    error: ApplicationError,
    stream: true,
  }),
  Rpc.make("CheckTask", {
    payload: TaskRecoveryInput,
    success: CommandReceipt,
    error: ApplicationError,
  }),
  Rpc.make("RetryTask", {
    payload: TaskRecoveryInput,
    success: CommandReceipt,
    error: ApplicationError,
  }),
  Rpc.make("InspectTask", {
    payload: { path: TaskPath },
    success: TaskInspection,
    error: ApplicationError,
  }),
  Rpc.make("InspectProcessing", {
    payload: { owner: ProcessingOwner },
    success: ProcessingSnapshot,
    error: ApplicationError,
  }),
  Rpc.make("RecoverProcessing", {
    payload: RecoveryInput,
    success: CommandReceipt,
    error: ApplicationError,
  }),
  Rpc.make("ListContexts", { success: Schema.Array(PublicContext), error: ApplicationError }),
  Rpc.make("QueryContext", {
    payload: ContextQueryInput,
    success: ContextQueryResult,
    error: ContextQueryError,
  }),
  Rpc.make("GetContext", {
    payload: { path: Schema.String },
    success: PublicContext,
    error: ApplicationError,
  }),
  Rpc.make("ListGoals", { success: Schema.Array(PublicContext), error: ApplicationError }),
  Rpc.make("RetryGoalTurn", {
    payload: RetryGoalTurnInput,
    success: CommandReceipt,
    error: ApplicationError,
  }),
  Rpc.make("GetGoalTimeline", {
    payload: {
      slug: Schema.String,
      before: Schema.optional(Schema.Int),
      limit: Schema.optional(Schema.Int),
    },
    success: GoalTimelinePage,
    error: ApplicationError,
  }),
  Rpc.make("ListApprovals", {
    success: Schema.Array(PublicApprovalEntry),
    error: ApplicationError,
  }),
  Rpc.make("InspectRuntime", { success: RuntimeSnapshot, error: ApplicationError }),
  Rpc.make("SendGoalMessage", {
    payload: {
      slug: Schema.String,
      text: Schema.String,
      requestId: Schema.optional(Schema.NonEmptyString),
    },
    success: Schema.Void,
    error: ApplicationError,
  }),
  Rpc.make("EndGoal", {
    payload: { slug: Schema.String, requestId: Schema.optional(Schema.NonEmptyString) },
    success: Schema.Void,
    error: ApplicationError,
  }),
  Rpc.make("RespondToApproval", {
    payload: { id: Schema.String, response: ApprovalResponse },
    success: Schema.Void,
    error: ApplicationError,
  }),
);
