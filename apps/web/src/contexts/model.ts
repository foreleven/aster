import { Schema, Match } from "effect";
import { SignalTrigger, Task, type PublicContext } from "@aster/core/contracts";

// Display only fields supplied by current public Context projections.
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
  chat: Schema.optional(Schema.Struct({ name: Schema.String })),
  subject: Schema.optional(Schema.String),
  from: Schema.optional(Schema.String),
  bodyPlainText: Schema.optional(Schema.String),
});
export interface ContextView extends Omit<PublicContext, "state"> {
  readonly state: typeof DisplayState.Type;
  readonly projectionError?: string;
  readonly rawState: PublicContext["state"];
}
export const projectContext = (record: PublicContext): ContextView => {
  const decoded = Schema.decodeUnknownResult(DisplayState)(record.state);
  return Match.value(decoded).pipe(
    Match.tag("Success", ({ success }) => ({ ...record, state: success, rawState: record.state })),
    Match.tag("Failure", () => ({
      ...record,
      state: {},
      rawState: record.state,
      projectionError: `Cannot display the public fields of ${record.path}.`,
    })),
    Match.exhaustive,
  );
};
export const kindOf = (path: string) => {
  if (/^\/goals\/[^/]+$/.test(path)) return "goal";
  if (/^\/tasks\/[^/]+$/.test(path)) return "task";
  if (/^\/signals\/[^/]+$/.test(path)) return "signal";
  if (["/goals", "/tasks", "/signals", "/approvals", "/system-one"].includes(path)) return "system";
  return "source";
};
export const contextTitle = (context: ContextView) =>
  context.state.title ||
  context.state.subject ||
  context.state.chat?.name ||
  context.description ||
  context.path;
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
