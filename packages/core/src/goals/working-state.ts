import { Effect, Schema } from "effect";
import type { ContextRegistry } from "../context/registry.js";
import { GoalState } from "./state.js";
import type { GoalHistory } from "./history.js";

/** Mailbox-only writes; full history and the bounded public working window share one commit path. */
export const goalWorkingState = (
  registry: ContextRegistry["Service"],
  history: GoalHistory,
  path: () => string,
) => {
  const current = () => registry.get(path())!;
  const state = () => Schema.decodeUnknownSync(GoalState)(current().state);
  const save = Effect.fn("Goal.save")(function* (
    patch: Partial<GoalState> = {},
    expectedRevision?: number,
  ) {
    const snapshot = current();
    const previous = Schema.decodeUnknownSync(GoalState)(snapshot.state);
    const s = { ...previous, ...patch };
    const historyCount = yield* history.count(s.definition.slug).pipe(Effect.orDie);
    // This is a recent business-input view. Pi independently owns its model context budget.
    const entries = yield* history
      .read(s.definition.slug, { after: Math.max(0, historyCount - 100), limit: 100 })
      .pipe(Effect.orDie);
    const messages = entries.map((entry) => entry.message);
    return yield* registry
      .commit(
        {
          ...snapshot,
          state: s,
          messages,
        },
        { expectedRevision: expectedRevision ?? snapshot.revision ?? 0 },
      )
      .pipe(Effect.asVoid);
  });
  return { current, state, save };
};
