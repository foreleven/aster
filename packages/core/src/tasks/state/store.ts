import { Effect, Schema, Ref } from "effect";
import { ContextSession } from "../../context/session.js";
import { taskView } from "../view.js";
import { TaskSnapshot } from "./snapshot.js";

/** An unadmitted Task has no durable state. Its first accepted command creates it. */
export const makeTaskStore = Effect.fn("TaskStore.make")(function* (path: string) {
  const session = yield* ContextSession.make({
    path,
    state: TaskSnapshot,
    message: Schema.Never,
    view: taskView,
  }).pipe(Effect.orDie);
  const current = session.current.pipe(Effect.orDie);
  const restored = yield* current;
  const committed = yield* Ref.make(
    restored ? Schema.decodeUnknownSync(TaskSnapshot)(restored.state) : undefined,
  );
  const read = Ref.get(committed).pipe(
    Effect.flatMap((state) =>
      state ? Effect.succeed(state) : Effect.die(new Error(`Task has not been admitted: ${path}`)),
    ),
  );
  const commit = Effect.fn("TaskStore.commit")(function* (
    state: TaskSnapshot,
    expectedRevision?: number,
  ) {
    yield* session.set({ state, description: "Task" }, { expectedRevision }).pipe(Effect.orDie);
    yield* Ref.set(committed, state);
  }, Effect.uninterruptible);
  const save = Effect.fnUntraced(function* (patch: Partial<TaskSnapshot>) {
    yield* commit({ ...(yield* read), ...patch });
  });
  return {
    path,
    read,
    current,
    exists: Ref.get(committed).pipe(Effect.map((value) => value !== undefined)),
    commit,
    save,
  };
});
export type TaskStore = Effect.Success<ReturnType<typeof makeTaskStore>>;
