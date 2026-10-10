import { Schema } from "effect";
import { SignalTrigger, Task, type ContextText } from "@aster/core/contracts";

// Structured display fields come from explicit business API responses.
const DisplayState = Schema.Struct({
  title: Schema.optional(Schema.String),
  status: Schema.optional(Schema.String),
  summary: Schema.optional(Schema.Union([Schema.String, Schema.Struct({ text: Schema.String })])),
  tasks: Schema.optional(Schema.Array(Schema.String)),
  retryableInputId: Schema.optional(Schema.String),
  lastError: Schema.optional(Schema.String),
  sourcePath: Schema.optional(Schema.String),
  replyTo: Schema.optional(Schema.String),
  owner: Schema.optional(Schema.String),
  agent: Schema.optional(Schema.String),
  trigger: Schema.optional(SignalTrigger),
  task: Schema.optional(Task),
  nextDue: Schema.optional(Schema.NullOr(Schema.String)),
});
export interface ContextView extends ContextText {
  readonly state: typeof DisplayState.Type;
  readonly rawState: unknown;
}
/** Generic Context data is text. Structured screens consume domain API responses. */
export const displayContext = (record: ContextText): ContextView => ({
  ...record,
  state: {},
  rawState: record.text,
});
export const displayBusiness = (
  record: { path: string; revision: number; description: string } & object,
): ContextView => ({
  path: record.path,
  revision: record.revision,
  description: record.description,
  text: "",
  state: Schema.decodeUnknownSync(DisplayState)(record),
  rawState: record,
});
export const kindOf = (path: string) => {
  if (/^\/goals\/[^/]+$/.test(path)) return "goal";
  if (/^\/tasks\/[^/]+$/.test(path)) return "task";
  if (/^\/signals\/[^/]+$/.test(path)) return "signal";
  if (["/goals", "/tasks", "/signals", "/approvals", "/system-one"].includes(path)) return "system";
  return "source";
};
export const contextTitle = (context: ContextView) =>
  context.state.title || context.description || context.path;
export const summaryText = (summary: ContextView["state"]["summary"]) =>
  typeof summary === "string" ? summary : summary?.text;
export const references = (context: ContextView): readonly string[] => {
  const state = context.state;
  const taskSources =
    state.task && state.task._tag !== "Goal"
      ? state.task.task.input.flatMap((input) => input.sources)
      : [];
  return [
    ...new Set(
      [state.owner, state.sourcePath, state.replyTo, ...(state.tasks ?? []), ...taskSources].filter(
        (value): value is string => !!value && value.startsWith("/"),
      ),
    ),
  ];
};
export const isTaskPath = (path: string) => /^\/tasks\/[a-f0-9]{64}$/.test(path);
export const dateTime = (value: string | number) =>
  new Date(value).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
export const taskText = (task: ContextView["state"]["task"]) =>
  task?._tag === "Goal" ? task.text : task?.task.instructions;
