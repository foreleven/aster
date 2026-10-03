import { RecoveryInput, ProcessingOwner, ProcessingSnapshot } from "./recovery.js";
export * from "./recovery.js";
import { GoalTimelinePage, RetryGoalSignalInput } from "./goal-timeline.js";
export * from "./goal-timeline.js";
export * from "./notification.js";
export * from "./writeback.js";
import { PersonalResumeRunInput } from "./run-command.js";
export * from "./run-command.js";
import { PersonalStartTaskInput } from "./task-command.js";
export * from "./task-command.js";
import { DelegationInspection, DelegationPath } from "./delegation.js";
export * from "./delegation.js";
import { PersonalApprovalResponseInput, PersonalApprovalRequestInput } from "./approval-command.js";
import { CommandReceipt } from "./command.js";
export * from "./command.js";
export * from "./approval-command.js";
import { PersonalSignalCommandInput, SignalDeliveryReceipt } from "./signal-command.js";
export * from "./signal-command.js";
import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/unstable/rpc";
import {
  PersonalInput,
  PersonalReceipt,
  PersonalRetryInput,
  PersonalGoalMessageInput,
} from "./personal.js";
import { GoalDeliveryReceipt } from "./delivery.js";
export * from "./personal.js";
export * from "./delivery.js";
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
  Rpc.make("RequestPersonalApproval", {
    payload: PersonalApprovalRequestInput,
    success: CommandReceipt,
    error: ApplicationError,
  }),
  Rpc.make("ResumePersonalRun", {
    payload: PersonalResumeRunInput,
    success: CommandReceipt,
    error: ApplicationError,
  }),
  Rpc.make("StartPersonalTask", {
    payload: PersonalStartTaskInput,
    success: CommandReceipt,
    error: ApplicationError,
  }),
  Rpc.make("InspectPersonalDelegation", {
    payload: { path: DelegationPath },
    success: DelegationInspection,
    error: ApplicationError,
  }),
  Rpc.make("RespondPersonalApproval", {
    payload: PersonalApprovalResponseInput,
    success: CommandReceipt,
    error: ApplicationError,
  }),
  Rpc.make("ApplyPersonalSignal", {
    payload: PersonalSignalCommandInput,
    success: SignalDeliveryReceipt,
    error: ApplicationError,
  }),
  Rpc.make("SendPersonalGoalMessage", {
    payload: PersonalGoalMessageInput,
    success: GoalDeliveryReceipt,
    error: ApplicationError,
  }),
  Rpc.make("GetPersonal", { success: PublicContext, error: ApplicationError }),
  Rpc.make("RetryPersonalInput", {
    payload: PersonalRetryInput,
    success: PersonalReceipt,
    error: ApplicationError,
  }),
  Rpc.make("SendPersonalMessage", {
    payload: PersonalInput,
    success: PersonalReceipt,
    error: ApplicationError,
  }),
  Rpc.make("ListContexts", { success: Schema.Array(PublicContext), error: ApplicationError }),
  Rpc.make("GetContext", {
    payload: { path: Schema.String },
    success: PublicContext,
    error: ApplicationError,
  }),
  Rpc.make("ListGoals", { success: Schema.Array(PublicContext), error: ApplicationError }),
  Rpc.make("RetryGoalSignal", {
    payload: RetryGoalSignalInput,
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
    payload: { slug: Schema.String },
    success: Schema.Void,
    error: ApplicationError,
  }),
  Rpc.make("RespondToApproval", {
    payload: { id: Schema.String, response: ApprovalResponse },
    success: Schema.Void,
    error: ApplicationError,
  }),
);
