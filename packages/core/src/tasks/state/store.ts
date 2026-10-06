import { Effect, Ref, Schema } from "effect";
import { ContextRegistry } from "../../context/registry.js";
import { TaskSnapshot } from "./snapshot.js";

/** Private to TaskState. Only the owning Actor mailbox calls its business operations. */
export const makeTaskStore = Effect.fn("TaskStore.make")(function* (path: string) {
  const registry = yield* ContextRegistry;
  const restored = registry.get(path);
  const snapshot = yield* Ref.make(
    restored
      ? yield* Schema.decodeUnknownEffect(TaskSnapshot)(restored.state).pipe(Effect.orDie)
      : undefined,
  );
  const read = Ref.get(snapshot).pipe(
    Effect.flatMap((state) =>
      state ? Effect.succeed(state) : Effect.die(new Error(`Task has not been admitted: ${path}`)),
    ),
  );
  const current = Effect.sync(() => registry.get(path));
  const commit = Effect.fn("TaskStore.commit")(function* (
    state: TaskSnapshot,
    expectedRevision?: number,
  ) {
    const record = yield* current;
    yield* registry
      .commit(
        {
          path,
          description: record?.description ?? "Task",
          state,
          messages: [],
        },
        { expectedRevision: expectedRevision ?? record?.revision ?? 0 },
      )
      .pipe(Effect.orDie);
    yield* Ref.set(snapshot, state);
  }, Effect.uninterruptible);
  const save = Effect.fn("TaskStore.save")(function* (patch: Partial<TaskSnapshot>) {
    yield* commit({ ...(yield* read), ...patch });
  });
  return {
    path,
    read,
    current,
    exists: Ref.get(snapshot).pipe(Effect.map((value) => value !== undefined)),
    commit,
    save,
  };
});
export type TaskStore = Effect.Success<ReturnType<typeof makeTaskStore>>;
