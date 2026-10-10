import { GoalSnapshot } from "./snapshot.js";
import { DurableContext } from "../../context/store.js";
import { Effect, Schema, Ref } from "effect";

/** GoalState's writer serializes mailbox and local tool transitions. */
export const makeGoalStore = Effect.fn("GoalStore.make")(function* (
  path: string,
  initial: GoalSnapshot,
) {
  const session = yield* (yield* DurableContext)
    .open({
      path,
      state: GoalSnapshot,
      message: Schema.Never,
      serialize: serializeGoalState,
      initial: { state: initial, description: initial.definition.description },
    })
    .pipe(Effect.orDie);
  const committed = yield* Ref.make(yield* session.state.get.pipe(Effect.orDie));
  const save = Effect.fn("GoalStore.save")(function* (patch: Partial<GoalSnapshot>) {
    const state = { ...(yield* Ref.get(committed)), ...patch };
    const saved = yield* session.state.set(state, { description: state.definition.description });
    yield* Ref.set(committed, saved.state);
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

export const serializeGoalState = ({ definition, status, summary }: GoalSnapshot) =>
  JSON.stringify({ ...definition, status, summary });
