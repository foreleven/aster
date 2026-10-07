import { ApplicationError } from "@aster/core/contracts";
import { Cause, Effect, Schema } from "effect";
import { ApplicationRpcs, RuntimeSnapshot } from "@aster/api-contracts";
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
    InspectProcessing: (_, options) =>
      traced("InspectProcessing", options.requestId, api.inspectProcessing()),
    RecoverProcessing: (input, options) =>
      traced("RecoverProcessing", options.requestId, api.recoverProcessing(input)),
    CheckTask: (input, options) => traced("CheckTask", options.requestId, api.checkTask(input)),
    RetryTask: (input, options) => traced("RetryTask", options.requestId, api.retryTask(input)),
    InspectTask: ({ path }, options) =>
      traced("InspectTask", options.requestId, api.inspectTask(path)),
    ListContexts: (_, options) => traced("ListContexts", options.requestId, api.contexts),
    QueryContext: (input, options) =>
      traced("QueryContext", options.requestId, api.queryContext(input)),
    GetContext: ({ path }, options) => traced("GetContext", options.requestId, api.context(path)),
    ListGoals: (_, options) => traced("ListGoals", options.requestId, api.goals.list),
    RetryGoalTurn: ({ slug, ...input }, options) =>
      traced("RetryGoalTurn", options.requestId, api.goals.retryTurn(slug, input)),
    GetGoalTimeline: ({ slug, before, limit }, options) =>
      traced("GetGoalTimeline", options.requestId, api.goals.timeline(slug, { before, limit })),
    ListApprovals: (_, options) => traced("ListApprovals", options.requestId, api.approvals.list),
    InspectRuntime: (_, options) =>
      traced(
        "InspectRuntime",
        options.requestId,
        api.inspect.pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(RuntimeSnapshot)),
          Effect.mapError(
            () =>
              new ApplicationError({
                kind: "unavailable",
                message: "Runtime inspection unavailable",
              }),
          ),
        ),
      ),
    SendGoalMessage: ({ slug, text, requestId }, options) =>
      traced("SendGoalMessage", options.requestId, api.goals.sendMessage(slug, text, requestId)),
    EndGoal: ({ slug, requestId }, options) =>
      traced("EndGoal", options.requestId, api.goals.end(slug, requestId)),
    RespondToApproval: ({ id, response }, options) =>
      traced("RespondToApproval", options.requestId, api.approvals.respond(id, response)),
  });
