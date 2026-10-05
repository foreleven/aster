import { Context, Data, Effect } from "effect";
import type { AgentMessage } from "@aster/agent";
import { isDeepStrictEqual } from "node:util";

export interface HistoryEntry {
  readonly requestId?: string;
  readonly seq: number;
  readonly at: string;
  readonly message: AgentMessage;
}
export class GoalHistoryError extends Data.TaggedError("GoalHistoryError")<{
  readonly cause: unknown;
}> {}
export interface GoalHistory {
  append(
    goal: string,
    message: AgentMessage,
    requestId?: string,
  ): Effect.Effect<HistoryEntry, GoalHistoryError>;
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
    append: (goal, message, requestId) =>
      Effect.suspend(() => {
        const items = entries.get(goal) ?? [];
        const previous =
          requestId === undefined
            ? undefined
            : items.find((entry) => entry.requestId === requestId);
        if (previous)
          return isDeepStrictEqual(previous.message, message)
            ? Effect.succeed(structuredClone(previous))
            : Effect.fail(
                new GoalHistoryError({
                  cause: new Error("History request ID belongs to another message"),
                }),
              );
        const entry = {
          ...(requestId === undefined ? {} : { requestId }),
          seq: items.length + 1,
          at: new Date().toISOString(),
          message: structuredClone(message),
        };
        items.push(entry);
        entries.set(goal, items);
        return Effect.succeed(structuredClone(entry));
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
