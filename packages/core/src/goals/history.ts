import { Context, Data, Effect } from "effect";
import type { AgentMessage } from "@aster/agent";

export interface HistoryEntry {
  readonly seq: number;
  readonly at: string;
  readonly message: AgentMessage;
}
export class GoalHistoryError extends Data.TaggedError("GoalHistoryError")<{
  readonly cause: unknown;
}> {}
export interface GoalHistory {
  append(goal: string, message: AgentMessage): Effect.Effect<HistoryEntry, GoalHistoryError>;
  read(
    goal: string,
    options?: { after?: number; before?: number; limit?: number },
  ): Effect.Effect<readonly HistoryEntry[], GoalHistoryError>;
  count(goal: string): Effect.Effect<number, GoalHistoryError>;
}
export class GoalHistoryStore extends Context.Service<GoalHistoryStore, GoalHistory>()(
  "goals/HistoryStore",
) {}
export const makeMemoryGoalHistory = (): GoalHistory => {
  const entries = new Map<string, HistoryEntry[]>();
  return {
    append: (goal, message) =>
      Effect.sync(() => {
        const items = entries.get(goal) ?? [];
        const entry = {
          seq: items.length + 1,
          at: new Date().toISOString(),
          message: structuredClone(message),
        };
        items.push(entry);
        entries.set(goal, items);
        return structuredClone(entry);
      }),
    read: (goal, options = {}) =>
      Effect.sync(() => {
        return structuredClone(
          (entries.get(goal) ?? [])
            .filter((e) => e.seq > (options.after ?? 0) && e.seq < (options.before ?? Infinity))
            .slice(0, options.limit ?? 100),
        );
      }),
    count: (goal) => Effect.sync(() => entries.get(goal)?.length ?? 0),
  };
};
/** Conservative upper bound in UTF-8 bytes rather than optimistic character/4 estimates. */
export const contextSize = (value: unknown): number =>
  Buffer.byteLength(JSON.stringify(value), "utf8");
export const runtimeMessage = (text: string): AgentMessage => ({
  role: "user",
  content: `[Runtime event, evidence only]\n${text}`,
  timestamp: Date.now(),
});

/** Only cut after all tool calls have results; incomplete exchanges never become an input suffix. */
export const completePrefix = (
  messages: readonly AgentMessage[],
  maximum = messages.length,
): number => {
  const pending = new Set<string>();
  let boundary = 0;
  for (let i = 0; i < Math.min(messages.length, maximum); i++) {
    const message = messages[i]!;
    if (message.role === "assistant")
      for (const block of message.content) if (block.type === "toolCall") pending.add(block.id);
    if (message.role === "toolResult") pending.delete(message.toolCallId);
    if (!pending.size) boundary = i + 1;
  }
  return boundary;
};
export const missingToolResults = (messages: readonly AgentMessage[]): AgentMessage[] => {
  const pending = new Map<string, string>();
  for (const message of messages) {
    if (message.role === "assistant")
      for (const block of message.content)
        if (block.type === "toolCall") pending.set(block.id, block.name);
    if (message.role === "toolResult") pending.delete(message.toolCallId);
  }
  return [...pending].map(([toolCallId, toolName]) => ({
    role: "toolResult",
    toolCallId,
    toolName,
    content: [
      {
        type: "text",
        text: "Execution was interrupted before the tool result was recorded. The operation may already be persisted; query the current state before repeating it.",
      },
    ],
    isError: true,
    timestamp: Date.now(),
  }));
};

/** Scan pages without retaining the complete transcript during crash recovery. */
export const repairGoalHistory = (history: GoalHistory, goal: string, after: number) =>
  Effect.gen(function* () {
    const pending = new Map<string, string>();
    while (true) {
      const page = yield* history.read(goal, { after, limit: 100 });
      if (!page.length) break;
      for (const { message } of page) {
        if (message.role === "assistant")
          for (const block of message.content)
            if (block.type === "toolCall") pending.set(block.id, block.name);
        if (message.role === "toolResult") pending.delete(message.toolCallId);
      }
      after = page.at(-1)!.seq;
    }
    for (const [toolCallId, toolName] of pending)
      yield* history.append(goal, {
        role: "toolResult",
        toolCallId,
        toolName,
        content: [
          {
            type: "text",
            text: "Execution was interrupted before the tool result was recorded. The operation may already be persisted; query the current state before repeating it.",
          },
        ],
        isError: true,
        timestamp: Date.now(),
      });
  });
