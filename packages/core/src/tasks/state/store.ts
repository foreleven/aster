import { Effect, Schema, Ref } from "effect";
import { DurableContext } from "../../context/store.js";
import { TaskSnapshot } from "./snapshot.js";

/** An unadmitted Task has no durable state. Its first accepted command creates it. */
export const makeTaskStore = Effect.fn("TaskStore.make")(function* (path: string) {
  const session = yield* (yield* DurableContext)
    .open({
      path,
      state: TaskSnapshot,
      message: Schema.Never,
      serialize: serializeTaskState,
    })
    .pipe(Effect.orDie);
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
    const saved = yield* session.state
      .set(state, { expectedRevision, description: state.admission.title })
      .pipe(Effect.orDie);
    yield* Ref.set(committed, saved.state);
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

export const serializeTaskState = ({ status, admission, outcomeEntryId }: TaskSnapshot) =>
  JSON.stringify({
    title: admission.title,
    status,
    source: admission.source,
    goal: admission.replyTo,
    agent: admission.agent,
    outcomeEntryId,
  });
