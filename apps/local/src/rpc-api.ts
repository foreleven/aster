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
    ResumePersonalRun: (input, options) =>
      traced("ResumePersonalRun", options.requestId, api.personal.resumeRun(input)),
    StartPersonalTask: (input, options) =>
      traced("StartPersonalTask", options.requestId, api.personal.startTask(input)),
    InspectPersonalDelegation: ({ path }, options) =>
      traced("InspectPersonalDelegation", options.requestId, api.personal.inspectDelegation(path)),
    RequestPersonalApproval: (input, options) =>
      traced("RequestPersonalApproval", options.requestId, api.personal.requestApproval(input)),
    RespondPersonalApproval: (input, options) =>
      traced("RespondPersonalApproval", options.requestId, api.personal.respondApproval(input)),
    ApplyPersonalSignal: (input, options) =>
      traced("ApplyPersonalSignal", options.requestId, api.personal.applySignal(input)),
    GetPersonal: (_, options) => traced("GetPersonal", options.requestId, api.personal.get),
    SendPersonalGoalMessage: (input, options) =>
      traced("SendPersonalGoalMessage", options.requestId, api.personal.sendGoalMessage(input)),
    RetryPersonalInput: (input, options) =>
      traced("RetryPersonalInput", options.requestId, api.personal.retry(input)),
    SendPersonalMessage: (input, options) =>
      traced("SendPersonalMessage", options.requestId, api.personal.sendMessage(input)),
    ListContexts: (_, options) => traced("ListContexts", options.requestId, api.contexts),
    GetContext: ({ path }, options) => traced("GetContext", options.requestId, api.context(path)),
    ListGoals: (_, options) => traced("ListGoals", options.requestId, api.goals.list),
    RetryGoalSignal: (input, options) =>
      traced("RetryGoalSignal", options.requestId, api.goals.retrySignal(input)),
    GetGoalTimeline: ({ slug, before, limit }, options) =>
      traced("GetGoalTimeline", options.requestId, api.goals.timeline(slug, { before, limit })),
    GetGoalHistory: ({ slug, before, limit }, options) =>
      traced("GetGoalHistory", options.requestId, api.goals.history(slug, { before, limit })),
    ListApprovals: (_, options) => traced("ListApprovals", options.requestId, api.approvals.list),
    InspectRuntime: (_, options) => traced("InspectRuntime", options.requestId, api.inspect),
    SendGoalMessage: ({ slug, text, requestId }, options) =>
      traced("SendGoalMessage", options.requestId, api.goals.sendMessage(slug, text, requestId)),
    EndGoal: ({ slug }, options) => traced("EndGoal", options.requestId, api.goals.end(slug)),
    RespondToApproval: ({ id, response }, options) =>
      traced("RespondToApproval", options.requestId, api.approvals.respond(id, response)),
  });
