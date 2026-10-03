import { Match } from "effect";
import type { ContextView, RuntimeActor, DashboardRow } from "../dashboard/model";
export const statusLabels: Readonly<Record<string, string>> = {
  open: "Open",
  deleted: "Deleted",
  revoked: "Revoked",
  unavailable: "Not registered",
  running: "Running",
  starting: "Starting",
  restarting: "Restarting",
  checking: "Readiness check",
  "awaiting-confirmation": "Awaiting confirmation",
  active: "Monitoring",
  completed: "Completed",
  failed: "Failed",
  cancelled: "Cancelled",
  uncertain: "Uncertain",
  unknown: "Unknown",
  pending: "Pending approval",
  resolved: "Awaiting delivery",
  acknowledged: "Acknowledged",
  waiting_input: "Awaiting input",
  "waiting-confirmation": "Awaiting confirmation",
  preparing: "Preparing task",
  ready: "Ready",
  stopping: "Stopping",
  stopped: "Stopped",
  "preparation-failed": "Preparation failed",
  blocked: "Blocked",
  rejected: "Rejected",
  submitting: "Submitting",
  idle: "Idle",
  archived: "Not running",
};
export const label = (value?: string) => (value ? statusLabels[value] || value : "—");
export const time = (value?: string | number) => {
  if (value === undefined) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleTimeString("en-US", { hour12: false });
};
export function rowsFor(
  contexts: ReadonlyMap<string, ContextView>,
  actors: readonly RuntimeActor[],
): readonly DashboardRow[] {
  const used = new Set<string>();
  const rows = actors.map((actor) => {
    const path =
      actor.contextPath ||
      actor.path
        .replace(/^\/user/, "")
        .split("/")
        .map((part) => {
          try {
            return part.startsWith("~")
              ? decodeURIComponent(
                  escape(atob(part.slice(1).replace(/-/g, "+").replace(/_/g, "/"))),
                )
              : part;
          } catch {
            return part;
          }
        })
        .join("/");
    const context = contexts.get(path);
    if (context) used.add(path);
    return {
      path: actor.path,
      context,
      actor,
      status: Match.value(actor).pipe(
        Match.when({ processing: true }, () => "processing"),
        Match.when({ phase: "running" }, () => "idle"),
        Match.orElse((actor) => actor.phase),
      ),
    };
  });
  return [
    ...rows,
    ...[...contexts.values()]
      .filter((c) => !used.has(c.path))
      .map((context) => ({ path: context.path, context, status: "archived" })),
  ];
}
export function references(record?: ContextView) {
  const state = record?.state || {};
  return [
    ...new Set(
      [
        state.sourceContext,
        state.sourcePath,
        state.runPath,
        state.definition?.goal && `/goals/${state.definition.goal}`,
        state.request?.runPath,
        state.goal && `/goals/${state.goal}`,
        ...(typeof state.task === "object" ? state.task.input.flatMap((item) => item.sources) : []),
        ...(record?.messages || []).flatMap((m) => m.references),
        ...(record?.personalState?.outbox ?? []).map((item) => item.input.target),
        ...(state.deliveries ?? []).map((item) => item.input.source),
      ].filter((v): v is string => typeof v === "string" && v.startsWith("/")),
    ),
  ];
}
export const isRunPath = (path: string) =>
  /^\/(?:runs\/[^/]+|(?:signals|goals)\/[^/]+\/runs\/[^/]+)$/.test(path);
export function runStages(record: ContextView) {
  const types = new Set((record?.messages || []).map((m) => m.type));
  const status = record?.state?.status;
  return [
    {
      title: "Task preparation",
      done: typeof record.state.task === "object" || types.has("TaskPrepared"),
      active: status === "preparing",
    },
    {
      title: "Readiness check",
      done: types.has("Ready") || types.has("ConfirmationRequested") || types.has("Delegating"),
      active: status === "checking",
    },
    {
      title: "Confirm / Auto",
      done: types.has("Delegating"),
      active: status === "awaiting-confirmation",
    },
    {
      title: "Agent delegation",
      done: types.has("Completed"),
      active: ["submitting", "running", "waiting_input"].includes(status ?? ""),
    },
    { title: "Result delivery", done: types.has("Completed"), active: false },
  ];
}
