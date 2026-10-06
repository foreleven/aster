import { Effect, Schema } from "effect";
import type { ContextRegistry } from "../context/registry.js";
import { GoalState } from "./state.js";

/** Mailbox-only business-state writes. Pi owns all conversation messages. */
export const goalWorkingState = (registry: ContextRegistry["Service"], path: () => string) => {
  const current = () => registry.get(path())!;
  const state = () => Schema.decodeUnknownSync(GoalState)(current().state);
  const save = Effect.fn("Goal.save")(function* (
    patch: Partial<GoalState> = {},
    expectedRevision?: number,
  ) {
    const snapshot = current();
    const previous = Schema.decodeUnknownSync(GoalState)(snapshot.state);
    const s = { ...previous, ...patch };
    return yield* registry
      .commit(
        {
          ...snapshot,
          state: s,
          messages: [],
        },
        { expectedRevision: expectedRevision ?? snapshot.revision ?? 0 },
      )
      .pipe(Effect.asVoid);
  });
  return { current, state, save };
};
