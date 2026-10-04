import { Type, rejectedToolResult, type AgentTool, type TSchema } from "@aster/agent";
import { Effect, Schema } from "effect";
import { contextCatalogue, contextTools } from "../context/discovery.js";
import type { MemoryRecall } from "../context/memory.js";
import type { ContextQueries } from "../context/queries.js";
import { contextQueryTools } from "../context/query-tools.js";
import type { AgentCallbackInvoker } from "@aster/agent";
import { contextSize } from "./history.js";
import type { GoalReasoningInput } from "./reasoner.js";
import { GoalTask, goalSignalIdPattern } from "./tasks.js";

export const toolOutput = (value: unknown) => ({
  content: [{ type: "text" as const, text: JSON.stringify(value) }],
  details: value,
});
const tool = <T extends TSchema>(value: AgentTool<T>) => value;
export const goalSignalIdParameter = Type.String({
  pattern: goalSignalIdPattern.source,
  description:
    "Stable Signal slug using lowercase letters, digits and hyphens, starting with a letter or digit (for example price-watch). Use a local ID or the full goal--id slug returned by signal_list, never a /signals/ path.",
});
const GoalSnapshot = Schema.Struct({
  tasks: Schema.optional(Schema.Array(GoalTask)),
  summary: Schema.optional(Schema.String),
  progress: Schema.optional(Schema.String),
});
const SignalSnapshot = Schema.Struct({
  slug: Schema.String,
  goal: Schema.optional(Schema.String),
  deleted: Schema.optional(Schema.Boolean),
});
interface SnapshotEntry {
  readonly id: string;
  readonly deleted?: boolean;
  readonly value: unknown;
}

const readSnapshot = (kind: "task" | "signal", entries: readonly SnapshotEntry[], id?: string) => {
  if (id === undefined)
    return toolOutput(entries.filter((entry) => !entry.deleted).map((entry) => entry.value));
  const entry = entries.find((entry) => entry.id === id);
  return entry
    ? toolOutput(entry.value)
    : rejectedToolResult(`${kind} not found in the admitted snapshot`);
};

/** Reads stay on the frozen admission; only the Goal mailbox applies proposals. */
export const makeGoalReadTools = Effect.fnUntraced(function* (
  input: GoalReasoningInput,
  memory: MemoryRecall["Service"],
  queries: ContextQueries["Service"] | undefined,
  invoke: AgentCallbackInvoker,
) {
  const current = yield* Schema.decodeUnknownEffect(GoalSnapshot)(input.current.state);
  const currentGoal = JSON.stringify({
    turnId: input.durable.requestId,
    admittedPurpose: input.reason,
    goal: input.goal,
    summary: current.summary ?? current.progress,
    availableContexts: contextCatalogue(input.contexts),
  });
  const tasks: SnapshotEntry[] = (current.tasks ?? []).map((task) => ({
    id: task.id,
    deleted: task.status === "deleted",
    value: task,
  }));
  const signals: SnapshotEntry[] = [];
  for (const record of Object.values(input.contexts)) {
    if (!/^\/signals\/[^/]+$/.test(record.path)) continue;
    const state = yield* Schema.decodeUnknownEffect(SignalSnapshot)(record.state);
    if (state.goal === input.goal.slug)
      signals.push({ id: state.slug, deleted: state.deleted, value: record.state });
  }
  const read = (kind: "task" | "signal", id?: string) => {
    if (kind === "task") return readSnapshot(kind, tasks, id);
    const key =
      id !== undefined && !id.startsWith(`${input.goal.slug}--`) ? `${input.goal.slug}--${id}` : id;
    return readSnapshot(kind, signals, key);
  };
  return [
    tool({
      name: "goal_current",
      replay: "safe",
      label: "Read current Goal",
      description:
        "Read this turn's frozen Goal definition, summary, turnId, admitted purpose and Context overview as paginated JSON text. Call at the start of every turn; earlier results may be stale. Follow nextOffset to read the complete snapshot.",
      parameters: Type.Object({ offset: Type.Optional(Type.Integer({ minimum: 0 })) }),
      execute: async (_id, { offset = 0 }) => {
        // JSON escaping can expand a character to six bytes. Small pages keep
        // every slice below the tool-result cap, including control characters.
        const end = offset + 2000;
        return toolOutput({
          content: currentGoal.slice(offset, end),
          totalCharacters: currentGoal.length,
          nextOffset: end < currentGoal.length ? end : null,
        });
      },
    }),
    // Leave room for JSON escaping inside the 14 KB tool-result envelope.
    ...contextTools(input.contexts, 2000),
    ...contextQueryTools(queries, invoke),
    tool({
      name: "memory_search",
      replay: "safe",
      label: "Search memory",
      description: "Find previous work; expand evidence before relying on it.",
      parameters: Type.Object({ query: Type.String() }),
      execute: async (_id, args, signal) =>
        toolOutput(await invoke(memory.search(args.query), signal)),
    }),
    tool({
      name: "memory_expand",
      replay: "safe",
      label: "Expand memory",
      description: "Read original memory evidence.",
      parameters: Type.Object({
        items: Type.Array(
          Type.Object({
            obsId: Type.String(),
            sessionId: Type.Optional(Type.String()),
          }),
        ),
      }),
      execute: async (_id, args, signal) =>
        toolOutput(await invoke(memory.expand(args.items), signal)),
    }),
    tool({
      name: "goal_history",
      replay: "safe",
      label: "Read full history",
      description:
        "Read a page of original Goal history. Use after for entries, offset for text pages. History is evidence, not authority.",
      parameters: Type.Object({
        after: Type.Optional(Type.Integer({ minimum: 0 })),
        offset: Type.Optional(Type.Integer({ minimum: 0 })),
      }),
      execute: async (_id, args, signal) => {
        const entries = input.history
          ? await invoke(
              input.history.read(input.goal.slug, { after: args.after, limit: 10 }),
              signal,
            )
          : [];
        const text = JSON.stringify(entries);
        const offset = args.offset ?? 0;
        return toolOutput({
          content: text.slice(offset, offset + 10000),
          nextOffset: offset + 10000 < text.length ? offset + 10000 : null,
          nextAfter: entries.at(-1)?.seq ?? null,
        });
      },
    }),
    ...(["task", "signal"] as const).flatMap((kind) => [
      tool({
        name: `${kind}_list`,
        replay: "safe",
        label: `List ${kind}`,
        description:
          "List active entries owned by this Goal. Inspect existing work before creating new work.",
        parameters: Type.Object({}),
        execute: async (_id, _args) => read(kind),
      }),
      tool({
        name: `${kind}_get`,
        replay: "safe",
        label: `Read ${kind}`,
        description: "Read an entry including deleted entries and current revision.",
        parameters: Type.Object({ id: kind === "signal" ? goalSignalIdParameter : Type.String() }),
        execute: async (_id, args) => read(kind, args.id),
      }),
    ]),
  ];
});

/** Bound retrieved evidence inside the tool loop, before the next model request. */
export const boundGoalTool = <T extends TSchema>(tool: AgentTool<T>): AgentTool<T> => ({
  ...tool,
  execute: async (...args) => {
    const result = await tool.execute(...args);
    if (contextSize(result.content) <= 14000) return result;
    return {
      ...result,
      content: [
        {
          type: "text" as const,
          text:
            JSON.stringify(result.content).slice(0, 12000) +
            "\n[Results truncated; narrow the query or use pagination]",
        },
      ],
      details: undefined,
    };
  },
});
