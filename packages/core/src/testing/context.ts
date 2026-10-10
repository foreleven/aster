import { publicJson } from "../json.js";
import { TaskSnapshot } from "../tasks/state/snapshot.js";
import { serializeGoalState } from "../goals/state/store.js";
import { serializeTaskState } from "../tasks/state/store.js";
import { serializeApprovalState } from "../approvals/state.js";
import { ApprovalSnapshot } from "../approvals/snapshot.js";
import { GoalSnapshot } from "../goals/state/snapshot.js";
import { ActorNotFound, ReplyTo, type ActorRef } from "@aster/actor";
import type { CurrentActors } from "../services/actors.js";
import { Deferred, Effect, Schema } from "effect";
import {
  makeDurableContext,
  type DurableContext,
  type ContextHandle,
  type ContextCommitOptions,
} from "../context/store.js";
import {
  ContextCommitError,
  ContextRecoveryError,
  ContextValidationError,
} from "../context/errors.js";
import {
  StoredContext as StoredContextSchema,
  type StoredContext,
  type ContextInput,
} from "../context/model.js";
export { taskCapture } from "../tasks/view.js";

export interface ContextStore {
  readonly loadAll: () => readonly StoredContext[];
  readonly save: (record: StoredContext) => void;
}
export interface TestDefinition {
  readonly state: Schema.ConstraintDecoder<object>;
  readonly message: Schema.ConstraintDecoder<unknown>;
  readonly serialize?: (state: object) => string;
  readonly events?: "none" | "state";
}
const defaultSerialize = (state: object): string => serializeFixtureState(state);
/** Test-only seeding and inspection over an injected store; production has one owner API. */
export const makeTestContextWithBackend = (
  backend: DurableContext["Service"],
  records = new Map<string, StoredContext>(),
) => {
  const definitions = new Map<string, TestDefinition>();
  const handles = new Map<string, ContextHandle<object, unknown>>();
  const ownerFor = Effect.fnUntraced(function* (path: string) {
    const existing = handles.get(path);
    if (existing) return existing;
    const definition = definitions.get(path);
    if (!definition)
      return yield* new ContextValidationError({
        path,
        cause: new Error("Unregistered test Context"),
      });
    const owner = yield* backend.open({
      path,
      ...definition,
      serialize: definition.serialize ?? defaultSerialize,
    });
    handles.set(path, owner);
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        handles.delete(path);
      }),
    );
    return owner;
  });
  return {
    ...backend,
    backend,
    records,
    get: (path: string) => structuredClone(records.get(path)?.snapshot),
    snapshot: () =>
      Object.fromEntries([...records].map(([path, record]) => [path, record.snapshot])),
    subscribe: backend.subscribe,
    changes: backend.changes,
    register: (path: string, definition: TestDefinition) =>
      Effect.sync(() => {
        definitions.set(path, definition);
      }),
    commit: Effect.fnUntraced(function* (
      input: ContextInput & { readonly text?: string },
      options: ContextCommitOptions,
    ) {
      const owner = yield* ownerFor(input.path);
      const current = yield* owner.current;
      if (!current) {
        yield* owner.state.set(input.state, { ...options, description: input.description });
        if (input.messages.length) yield* owner.messages.append(input.messages);
      } else {
        const keyOf = (value: unknown) => JSON.stringify(value);
        const keep = new Set(input.messages.map(keyOf));
        yield* owner.commit(
          () => ({
            state: input.state,
            messages: {
              append: input.messages.filter(
                (message) => !current.messages.some((value) => keyOf(value) === keyOf(message)),
              ),
              remove: current.messages.filter((message) => !keep.has(keyOf(message))).map(keyOf),
            },
          }),
          { ...options, description: input.description },
        );
      }
      const saved = yield* owner.snapshot;
      const snapshot = { ...saved, text: yield* owner.serializeState };
      records.set(input.path, {
        snapshot,
        events: backend.journal().filter((event) => event.record.path === input.path),
      });
      return snapshot;
    }, Effect.scoped),
  };
};
export const makeTestContext = (store?: ContextStore) =>
  Effect.gen(function* () {
    const records = new Map(
      (store?.loadAll() ?? []).map((record) => [record.snapshot.path, record]),
    );
    const backend = yield* makeDurableContext({
      load: Effect.try({
        try: () => {
          const loaded = (store?.loadAll() ?? [...records.values()]).map((record) =>
            Schema.decodeUnknownSync(StoredContextSchema)(publicJson(record)),
          );
          records.clear();
          for (const record of loaded) records.set(record.snapshot.path, structuredClone(record));
          return loaded;
        },
        catch: (cause) => new ContextRecoveryError({ path: "/", cause }),
      }),
      save: (record) =>
        Effect.try({
          try: () => {
            const accepted = structuredClone(record);
            store?.save(record);
            records.set(record.snapshot.path, accepted);
          },
          catch: (cause) => new ContextCommitError({ path: record.snapshot.path, cause }),
        }),
    });
    return makeTestContextWithBackend(backend, records);
  });
export type TestContext = Effect.Success<ReturnType<typeof makeTestContext>>;

/** Synchronous domain query transport for isolated model tests; Actor tests use real owners. */
export const testActors = (
  store: Pick<ReturnType<typeof makeTestContextWithBackend>, "get">,
): CurrentActors["Service"] => ({
  select: (path) => ({
    path,
    resolve: () =>
      Effect.suspend(() => {
        const record = store.get(path.slice("/user".length));
        if (!record) return Effect.fail(new ActorNotFound(path));
        const ref: ActorRef<unknown> = {
          path,
          incarnation: "test-domain-reader",
          awaitStarted: Effect.void,
          tell: () => Effect.void,
          ask: <Response>(make: (replyTo: ActorRef<Response>) => unknown) =>
            Effect.gen(function* () {
              const response = yield* Deferred.make<Response>();
              const replyTo: ActorRef<Response> = {
                path: "/test-reply",
                incarnation: "test-reply",
                awaitStarted: Effect.void,
                tell: (value) => Deferred.succeed(response, value).pipe(Effect.asVoid),
                ask: () => Effect.never,
              };
              const query = Schema.decodeUnknownSync(
                Schema.Struct({ _tag: Schema.String, replyTo: ReplyTo<unknown>() }),
              )(make(replyTo));
              const value = path.startsWith("/user/goals/")
                ? {
                    definition: { slug: path.split("/").at(-1)!, description: record.description },
                    summary: "",
                    tasks: [],
                    inputs: [],
                    receipts: [],
                    ...record.state,
                  }
                : record.state;
              yield* query.replyTo.tell(
                query._tag === "GetApprovals"
                  ? Schema.decodeUnknownSync(
                      Schema.Struct({ entries: Schema.Array(Schema.Unknown) }),
                    )(value).entries
                  : value,
              );
              return yield* Deferred.await(response);
            }),
        };
        return Effect.succeed(ref);
      }),
  }),
});

export const testApprovalEntries = (
  store: Pick<ReturnType<typeof makeTestContextWithBackend>, "get">,
) => {
  const record = store.get("/approvals");
  return record ? Schema.decodeUnknownSync(ApprovalSnapshot)(record.state).entries : [];
};
/** Observe admitted handle writes in cancellation tests, without adding a production raw writer. */
export const observeContextWrites = (
  backend: DurableContext["Service"],
  observe: (
    record: import("../context/model.js").ContextInput & { revision: number },
  ) => Effect.Effect<void>,
): DurableContext["Service"] => ({
  ...backend,
  open: (options) =>
    backend.open(options).pipe(
      Effect.map((owner) => ({
        ...owner,
        state: {
          ...owner.state,
          set: (state, commitOptions) =>
            owner.state.set(state, commitOptions).pipe(Effect.tap(observe)),
          update: (change, commitOptions) =>
            owner.state.update(change, commitOptions).pipe(Effect.tap(observe)),
        },
        commit: (change, commitOptions) =>
          owner.commit(change, commitOptions).pipe(Effect.tap(observe)),
      })),
    ),
});

/** New-format fixture construction; this is not a production migration decoder. */
export const serializeFixtureState = (state: unknown): string => {
  const goal = Schema.decodeUnknownOption(GoalSnapshot)(state);
  if (goal._tag === "Some") return serializeGoalState(goal.value);
  const task = Schema.decodeUnknownOption(TaskSnapshot)(state);
  if (task._tag === "Some") return serializeTaskState(task.value);
  const approval = Schema.decodeUnknownOption(ApprovalSnapshot)(state);
  if (approval._tag === "Some") return serializeApprovalState(approval.value);
  return JSON.stringify(state);
};
