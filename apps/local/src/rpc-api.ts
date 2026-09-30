import { ApplicationRpcs } from "@aster/api-contracts";
import type { ApplicationApi } from "@aster/core";

/** Transport handlers preserve application errors and durable acknowledgement semantics. */
export const applicationRpcHandlers = (api: ApplicationApi) =>
  ApplicationRpcs.toLayer({
    ListContexts: () => api.contexts,
    GetContext: ({ path }) => api.context(path),
    ListGoals: () => api.goals.list,
    GetGoalHistory: ({ slug, before, limit }) => api.goals.history(slug, { before, limit }),
    ListApprovals: () => api.approvals.list,
    InspectRuntime: () => api.inspect,
    SendGoalMessage: ({ slug, text }) => api.goals.sendMessage(slug, text),
    EndGoal: ({ slug }) => api.goals.end(slug),
    RespondToApproval: ({ id, response }) => api.approvals.respond(id, response),
  });
