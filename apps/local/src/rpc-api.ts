import { Cause, Effect } from "effect";
import { ApplicationRpcs } from "@aster/api-contracts";
import type { ApplicationApi } from "@aster/core";

const traced = <A, E>(tag: string, requestId: unknown, run: Effect.Effect<A, E>) =>
  Effect.logInfo(JSON.stringify({ event: "rpc.request", tag, requestId: String(requestId) })).pipe(
    Effect.andThen(run),
    Effect.tapCause((cause) =>
      Effect.logError(
        JSON.stringify({
          event: "rpc.failed",
          tag,
          requestId: String(requestId),
          cause: Cause.pretty(cause),
        }),
      ),
    ),
  );

/** Transport handlers preserve application errors and durable acknowledgement semantics. */
export const applicationRpcHandlers = (api: ApplicationApi) =>
  ApplicationRpcs.toLayer({
    InspectProcessing: ({ owner }, options) =>
      traced("InspectProcessing", options.requestId, api.inspectProcessing(owner)),
    RecoverProcessing: (input, options) =>
      traced("RecoverProcessing", options.requestId, api.recoverProcessing(input)),
    ResumeTask: (input, options) => traced("ResumeTask", options.requestId, api.resumeTask(input)),
    InspectTask: ({ path }, options) =>
      traced("InspectTask", options.requestId, api.inspectTask(path)),
    ListContexts: (_, options) => traced("ListContexts", options.requestId, api.contexts),
    QueryContext: (input, options) =>
      traced("QueryContext", options.requestId, api.queryContext(input)),
    GetContext: ({ path }, options) => traced("GetContext", options.requestId, api.context(path)),
    ListGoals: (_, options) => traced("ListGoals", options.requestId, api.goals.list),
    RetryGoalTurn: (input, options) =>
      traced("RetryGoalTurn", options.requestId, api.goals.retryTurn(input)),
    GetGoalTimeline: ({ slug, before, limit }, options) =>
      traced("GetGoalTimeline", options.requestId, api.goals.timeline(slug, { before, limit })),
    GetGoalHistory: ({ slug, before, limit }, options) =>
      traced("GetGoalHistory", options.requestId, api.goals.history(slug, { before, limit })),
    ListApprovals: (_, options) => traced("ListApprovals", options.requestId, api.approvals.list),
    InspectRuntime: (_, options) => traced("InspectRuntime", options.requestId, api.inspect),
    SendGoalMessage: ({ slug, text, requestId }, options) =>
      traced("SendGoalMessage", options.requestId, api.goals.sendMessage(slug, text, requestId)),
    EndGoal: ({ slug, requestId }, options) =>
      traced("EndGoal", options.requestId, api.goals.end(slug, requestId)),
    RespondToApproval: ({ id, response }, options) =>
      traced("RespondToApproval", options.requestId, api.approvals.respond(id, response)),
  });
