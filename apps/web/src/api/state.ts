import type { ApprovalResponse } from "@aster/core/contracts";
import { Atom, AsyncResult } from "effect/reactivity";
import { contextsQuery, approvalsQuery, resultValue, resultError } from "./client";
import { connection } from "./events";
import { projectContext } from "../contexts/model";

export const contextViews = Atom.make((get) =>
  (resultValue(get(contextsQuery)) ?? [])
    .map(projectContext)
    .filter((context) => context.state.status !== "deleted"),
);
export const approvalEntries = Atom.make((get) => resultValue(get(approvalsQuery)) ?? []);
export const connectionStatus = Atom.make((get) => {
  const contexts = get(contextsQuery);
  const live = get(connection);
  return {
    loaded: resultValue(contexts) !== undefined,
    connected: AsyncResult.isSuccess(live) && live.value,
    loading: contexts.waiting,
    error: resultError(contexts) || resultError(live),
  };
});
export const pendingApprovalResponses = Atom.make<Record<string, ApprovalResponse>>({}).pipe(
  Atom.keepAlive,
);
