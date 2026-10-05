import type { ApprovalResponse } from "@aster/api-contracts";
import { Atom, AsyncResult } from "effect/reactivity";
import {
  contextsQuery,
  runtimeQuery,
  approvalsQuery,
  resultValue,
  resultError,
} from "../api/client";
import { connection } from "../api/events";
import { projectContext, eventPath, type DashboardRow } from "./model";
import { rowsFor, references, isTaskPath } from "../lib/dashboard";

// Decode only when Context data changes, independently of the telemetry refresh cadence.
export const contextViews = Atom.make((get) =>
  (resultValue(get(contextsQuery)) ?? []).map(projectContext),
);
const contextIndex = Atom.make(
  (get) =>
    new Map(
      get(contextViews)
        .filter((c) => !c.state.deleted)
        .map((c) => [c.path, c]),
    ),
);
export const runtimeView = Atom.make((get) => resultValue(get(runtimeQuery)));
export const dashboardRows = Atom.make((get) =>
  rowsFor(get(contextIndex), get(runtimeView)?.actors ?? []),
);
export const approvalEntries = Atom.make((get) => resultValue(get(approvalsQuery)) ?? []);
export const pendingApprovals = Atom.make(
  (get) => get(approvalEntries).filter((e) => e.status === "pending").length,
);
export const runtimeEvents = Atom.make((get) => [...(get(runtimeView)?.events ?? [])].reverse());
export const failureCount = Atom.make(
  (get) =>
    get(dashboardRows).filter(
      (row) =>
        ["failed", "uncertain"].includes(row.context?.state.status ?? "") || row.actor?.lastError,
    ).length,
);
export const dashboardStatus = Atom.make((get) => {
  const contexts = get(contextsQuery);
  const runtime = get(runtimeQuery);
  const live = get(connection);
  return {
    loaded: resultValue(contexts) !== undefined,
    connected: resultValue(live) === true,
    loading: AsyncResult.isWaiting(contexts) || AsyncResult.isWaiting(runtime),
    at: AsyncResult.isSuccess(contexts) ? contexts.timestamp : undefined,
    error:
      resultError(contexts) ||
      resultError(runtime) ||
      resultError(get(approvalsQuery)) ||
      resultError(live) ||
      get(contextViews).find((c) => c.projectionError)?.projectionError ||
      "",
  };
});
const rowIndex = Atom.make((get) => {
  const index = new Map<string, DashboardRow>();
  for (const row of get(dashboardRows)) {
    index.set(row.path, row);
    if (row.context) index.set(row.context.path, row);
  }
  for (const event of get(runtimeEvents)) {
    const path = eventPath(event);
    if (!index.has(path)) index.set(path, { path, status: "unavailable" });
  }
  return index;
});
export const selectedRow = Atom.family((path: string) =>
  Atom.make((get) => get(rowIndex).get(path)),
);
export const inspectorView = Atom.family((path: string) =>
  Atom.make((get) => {
    const row = get(selectedRow(path));
    const context = row?.context;
    const related = context
      ? get(contextViews)
          .filter(
            (c) =>
              c.state.request?.taskPath === context?.path ||
              c.state.sourcePath === context?.path ||
              `/goals/${c.state.goal || c.state.definition?.goal}` === context?.path,
          )
          .map((c) => c.path)
      : [];
    return {
      row,
      related: [...new Set([...references(context), ...related])],
      events: get(runtimeEvents).filter((e) => eventPath(e) === row?.path),
    };
  }),
);
export const approvalDiagnostics = Atom.make((get) => {
  const contexts = get(contextViews);
  return {
    tasks: contexts.filter((c) => isTaskPath(c.path)).length,
    failures: contexts
      .filter((c) => /^\/goals\/[^/]+$/.test(c.path))
      .flatMap((c) => {
        if (c.state.lastError) return [{ path: c.path, text: c.state.lastError }];
        const last = c.messages.findLast((m) => m.type === "error" || m.type === "assistant");
        return last?.type === "error" ? [{ path: c.path, text: last.text }] : [];
      }),
  };
});

export const pendingApprovalResponses = Atom.make<Record<string, ApprovalResponse>>({}).pipe(
  Atom.keepAlive,
);
