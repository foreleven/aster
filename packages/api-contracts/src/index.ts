import { ResumeTaskDeliveryInput } from "./task-control.js";
import { ContextQueryInput, ContextQueryResult, ContextQueryError } from "./context-query.js";
export * from "./context-query.js";
import { RecoveryInput, ProcessingOwner, ProcessingSnapshot } from "./recovery.js";
export * from "./recovery.js";
import { GoalTimelinePage, RetryGoalTurnInput } from "./goal-timeline.js";
export * from "./goal-timeline.js";
export * from "./causal.js";
export * from "./writeback.js";
export * from "./task-control.js";
export * from "./task-command.js";
import { TaskInspection } from "./task-inspection.js";
import { TaskPath } from "./task-control.js";
export * from "./task-inspection.js";
import { CommandReceipt } from "./command.js";
export * from "./command.js";
export * from "./signal-command.js";
import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/rpc";
import {
  ApplicationError,
  ApprovalEntry,
  ApprovalResponse,
  HistoryPage,
  PublicContext,
  RuntimeSnapshot,
} from "./models.js";
export * from "./models.js";

export const QueryKeys = {
  all: "all-queries",
  contexts: "contexts",
  goals: "goals",
  approvals: "approvals",
  runtime: "runtime",
  context: (path: string) => `context:${path}`,
  history: (slug: string) => `goal-history:${slug}`,
} as const;

/** Stable wire keys shared by server commit notifications and client mutation invalidation. */
export const contextQueryKeys = (path: string): readonly string[] => {
  const keys = [QueryKeys.contexts, QueryKeys.context(path)];
  const goal = /^\/goals\/([^/]+)$/.exec(path);
  if (goal) keys.push(QueryKeys.goals, QueryKeys.history(goal[1]!));
  if (path === "/approvals") keys.push(QueryKeys.approvals);
  return keys;
};
export const QueryInvalidation = Schema.TaggedStruct("Invalidate", {
  keys: Schema.Array(Schema.String),
});
export type QueryInvalidation = typeof QueryInvalidation.Type;
export const ApplicationRpcs = RpcGroup.make(
  Rpc.make("ResumeTask", {
    payload: ResumeTaskDeliveryInput,
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
  Rpc.make("GetGoalHistory", {
    payload: {
      slug: Schema.String,
      before: Schema.optional(Schema.Int),
      limit: Schema.optional(Schema.Int),
    },
    success: HistoryPage,
    error: ApplicationError,
  }),
  Rpc.make("ListApprovals", { success: Schema.Array(ApprovalEntry), error: ApplicationError }),
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
