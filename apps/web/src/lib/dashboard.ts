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
  ready: "Ready",
  stopping: "Stopping",
  stopped: "Stopped",
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
        state.replyTo,
        state.taskPath,
        state.definition?.goal && `/goals/${state.definition.goal}`,
        state.request?.taskPath,
        state.goal && `/goals/${state.goal}`,
        ...(state.task && "input" in state.task
          ? state.task.input.flatMap((item) => item.sources)
          : []),
        ...(record?.messages || []).flatMap((m) => m.references),
        ...(state.deliveries ?? []).map((item) => item.input.source),
      ].filter((v): v is string => typeof v === "string" && v.startsWith("/")),
    ),
  ];
}
export const isTaskPath = (path: string) => /^\/tasks\/[a-f0-9]{64}$/.test(path);
export function taskStages(record: ContextView) {
  const status = record?.state?.status;
  return [
    {
      title: "Task confirmation",
      done: !["awaiting-confirmation", "rejected"].includes(status ?? ""),
      active: status === "awaiting-confirmation",
    },
    {
      title: "Execution",
      done: status === "completed",
      active: ["submitting", "running", "waiting_input"].includes(status ?? ""),
    },
    { title: "Result delivery", done: status === "completed", active: false },
  ];
}

export function taskText(task: ContextView["state"]["task"]): string | undefined {
  if (!task) return undefined;
  if ("instructions" in task) return task.instructions;
  return task._tag === "Goal" ? task.text : task.task.instructions;
}
