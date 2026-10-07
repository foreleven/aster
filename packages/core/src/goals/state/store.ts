import { GoalSnapshot } from "./snapshot.js";
import { ContextRegistry } from "../../context/registry.js";
import { Effect, Ref, Schema } from "effect";

/** Private persistence adapter. GoalState serializes all business transitions. */
export const makeGoalStore = Effect.fn("GoalStore.make")(function* (
  path: string,
  initial: GoalSnapshot,
) {
  const registry = yield* ContextRegistry;
  const restored =
    registry.get(path) ??
    (yield* registry
      .commit(
        { path, description: initial.definition.description, messages: [], state: initial },
        { expectedRevision: 0 },
      )
      .pipe(Effect.orDie));
  const snapshot = yield* Ref.make(
    yield* Schema.decodeUnknownEffect(GoalSnapshot)(restored.state).pipe(Effect.orDie),
  );
  const current = Effect.sync(() => registry.get(path)!);
  const save = Effect.fn("GoalStore.save")(function* (patch: Partial<GoalSnapshot>) {
    // Description initialization can advance the Context revision independently.
    const record = yield* current;
    const state = { ...(yield* Ref.get(snapshot)), ...patch };
    yield* registry.commit(
      { ...record, state, messages: [] },
      { expectedRevision: record.revision },
    );
    yield* Ref.set(snapshot, state);
    // Once storage accepts a write, its in-memory mirror must drain with it.
  }, Effect.uninterruptible);
  return { current, read: Ref.get(snapshot), save };
});
export type GoalStore = Effect.Success<ReturnType<typeof makeGoalStore>>;
