import { GoalSnapshot } from "./snapshot.js";
import { goalView } from "../view.js";
import { ContextSession } from "../../context/session.js";
import { Effect, Schema, Ref } from "effect";

/** GoalState's writer serializes mailbox and local tool transitions. */
export const makeGoalStore = Effect.fn("GoalStore.make")(function* (
  path: string,
  initial: GoalSnapshot,
) {
  const session = yield* ContextSession.make({
    path,
    state: GoalSnapshot,
    message: Schema.Never,
    view: goalView,
    initial: { state: initial, description: initial.definition.description },
  }).pipe(Effect.orDie);
  const committed = yield* Ref.make(yield* session.state.get.pipe(Effect.orDie));
  const save = Effect.fn("GoalStore.save")(function* (patch: Partial<GoalSnapshot>) {
    const state = { ...(yield* Ref.get(committed)), ...patch };
    yield* session.set({ state });
    yield* Ref.set(committed, state);
  }, Effect.uninterruptible);
  return {
    current: session.current.pipe(
      Effect.map((record) => record!),
      Effect.orDie,
    ),
    read: Ref.get(committed),
    save,
  };
});
export type GoalStore = Effect.Success<ReturnType<typeof makeGoalStore>>;
