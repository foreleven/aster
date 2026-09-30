import { Match, Predicate, Schema } from "effect";
import type { PublicContext, RuntimeEvent, RuntimeSnapshot } from "@aster/api-contracts";

// Public Contexts stay open-ended. The dashboard only decodes the fields it presents.
const DisplayTask = Schema.Struct({
  id: Schema.String,
  title: Schema.String,
  instructions: Schema.String,
  status: Schema.String,
  result: Schema.optional(Schema.String),
  execution: Schema.optional(Schema.Struct({ runPath: Schema.String, status: Schema.String })),
});
const DisplayState = Schema.Struct({
  status: Schema.optional(Schema.String),
  deleted: Schema.optional(Schema.Boolean),
  lastError: Schema.optional(Schema.String),
  summary: Schema.optional(Schema.String),
  historyCount: Schema.optional(Schema.Number),
  tasks: Schema.optional(Schema.Array(DisplayTask)),
  sourceContext: Schema.optional(Schema.String),
  sourcePath: Schema.optional(Schema.String),
  runPath: Schema.optional(Schema.String),
  goal: Schema.optional(Schema.String),
  definition: Schema.optional(Schema.Struct({ goal: Schema.optional(Schema.String) })),
  request: Schema.optional(Schema.Struct({ runPath: Schema.optional(Schema.String) })),
  session: Schema.optional(
    Schema.Struct({ sessionId: Schema.String, runId: Schema.optional(Schema.String) }),
  ),
});
export type DisplayState = typeof DisplayState.Type;
export interface ContextView extends Omit<PublicContext, "state" | "messages"> {
  readonly state: DisplayState;
  readonly messages: readonly MessageView[];
  readonly rawState: PublicContext["state"];
  readonly projectionError?: string;
}
export const projectContext = (record: PublicContext): ContextView => {
  const result = Schema.decodeUnknownResult(DisplayState)(record.state);
  const messages = record.messages.map(projectMessage);
  return Match.value(result).pipe(
    Match.tag("Success", ({ success }) => ({
      ...record,
      messages,
      rawState: record.state,
      state: success,
    })),
    Match.tag("Failure", () => ({
      ...record,
      messages,
      rawState: record.state,
      state: {},
      projectionError: `Unsupported dashboard fields in ${record.path}; raw state is still available`,
    })),
    Match.exhaustive,
  );
};

const MessageFields = Schema.Struct({
  role: Schema.optional(Schema.String),
  type: Schema.optional(Schema.String),
  _tag: Schema.optional(Schema.String),
  text: Schema.optional(Schema.String),
  at: Schema.optional(Schema.String),
  timestamp: Schema.optional(Schema.Number),
  toolName: Schema.optional(Schema.String),
  content: Schema.optional(Schema.Unknown),
  references: Schema.optional(Schema.Array(Schema.String)),
});
const Blocks = Schema.Array(
  Schema.Struct({ type: Schema.String, text: Schema.optional(Schema.String) }),
);
const runtimeEventPrefix = /^\[(?:Runtime event, evidence only|运行时事件，仅作为证据)\]\n/;
export const projectMessage = (raw: unknown) => {
  const decoded = Schema.decodeUnknownResult(MessageFields)(raw);
  const message =
    decoded._tag === "Success"
      ? decoded.success
      : { text: typeof raw === "string" ? raw : undefined };
  const content = message.content;
  const decodedBlocks = Schema.decodeUnknownResult(Blocks)(content);
  const blocks = decodedBlocks._tag === "Success" ? decodedBlocks.success : [];
  const tool =
    message.role === "toolResult" ||
    (message.role === "assistant" && blocks.some((b) => b.type === "toolCall"));
  const label = Match.value(message.role).pipe(
    Match.when("toolResult", () => "Tool result"),
    Match.when("assistant", () => "Agent"),
    Match.when("user", () =>
      typeof content === "string" && runtimeEventPrefix.test(content) ? "Progress" : "User",
    ),
    Match.orElse(() => message.type || message._tag || "Messages"),
  );
  const text = Match.value(content).pipe(
    Match.when(Predicate.isString, (value) => value.replace(runtimeEventPrefix, "")),
    Match.orElse(() =>
      decodedBlocks._tag === "Success"
        ? blocks
            .filter((b) => b.type === "text")
            .map((b) => b.text ?? "")
            .join("\n")
        : message.text,
    ),
  );
  return {
    ...message,
    label,
    text,
    tool,
    details: JSON.stringify(tool ? content : raw, null, 2),
    references: message.references ?? [],
  };
};
export type MessageView = ReturnType<typeof projectMessage>;
export type RuntimeActor = RuntimeSnapshot["actors"][number];
export interface DashboardRow {
  readonly path: string;
  readonly context?: ContextView;
  readonly actor?: RuntimeActor;
  readonly status: string;
}
export const eventPath = (event: RuntimeEvent) =>
  Match.value(event).pipe(
    Match.tag("DeadLetter", (event) => event.target),
    Match.orElse((event) => event.path),
  );
export type DashboardPage = "overview" | "actors" | "goals" | "signals" | "approvals";
export type RowFilter = "all" | "live" | "archived";
export const filterRows = (
  rows: readonly DashboardRow[],
  page: DashboardPage,
  query: string,
  filter: RowFilter,
) => {
  const search = query.toLowerCase();
  return rows.filter((row) => {
    const path = row.context?.path ?? row.path;
    const inPage = Match.value(page).pipe(
      Match.when("goals", () => /^\/goals\/[^/]+$/.test(path)),
      Match.when("signals", () => path.startsWith("/signals/") || path.startsWith("/delegations/")),
      Match.orElse(() => true),
    );
    const inFilter = Match.value(filter).pipe(
      Match.when("all", () => true),
      Match.when("live", () => row.actor !== undefined),
      Match.when("archived", () => row.actor === undefined),
      Match.exhaustive,
    );
    return (
      inPage &&
      inFilter &&
      (!search || `${row.path} ${row.context?.description ?? ""}`.toLowerCase().includes(search))
    );
  });
};
