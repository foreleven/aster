import type { ApprovalResponse } from "@aster/core/contracts";
import { Atom, AsyncResult } from "effect/reactivity";
import {
  contextsQuery,
  goalsQuery,
  tasksQuery,
  signalsQuery,
  approvalsQuery,
  resultValue,
  resultError,
} from "./client";
import { connection } from "./events";
import { displayContext, displayBusiness } from "../contexts/model";

export const contextViews = Atom.make((get) => {
  const business = [
    ...(resultValue(get(goalsQuery)) ?? []),
    ...(resultValue(get(tasksQuery)) ?? []),
    ...(resultValue(get(signalsQuery)) ?? []),
  ].map(displayBusiness);
  const businessPaths = new Set(business.map((record) => record.path));
  return [
    ...business,
    ...(resultValue(get(contextsQuery)) ?? [])
      .filter(
        (record) =>
          !businessPaths.has(record.path) && !/^\/(goals|tasks|signals)\/[^/]+$/.test(record.path),
      )
      .map(displayContext),
  ].filter((record) => record.state.status !== "deleted");
});
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
