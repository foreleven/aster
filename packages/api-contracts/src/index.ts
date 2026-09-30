import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/unstable/rpc";
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
  Rpc.make("ListContexts", { success: Schema.Array(PublicContext), error: ApplicationError }),
  Rpc.make("GetContext", {
    payload: { path: Schema.String },
    success: PublicContext,
    error: ApplicationError,
  }),
  Rpc.make("ListGoals", { success: Schema.Array(PublicContext), error: ApplicationError }),
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
    payload: { slug: Schema.String, text: Schema.String },
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
