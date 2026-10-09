import { isDeepStrictEqual } from "node:util";
import { Data, Effect, Option, Ref, Schema, Semaphore } from "effect";
import type { ContextViewPolicy } from "./view.js";
import { DurableContext } from "./store.js";
import { ContextPath, type ContextInput } from "./model.js";
import { ContextRegistry } from "./registry.js";
import { ContextConflict, ContextValidationError } from "./errors.js";
import { ContextSessionLayout } from "./session-storage.js";
import type { ContextCommitOptions } from "./store.js";

export class ContextSessionClosed extends Data.TaggedError("ContextSessionClosed")<{
  readonly path: string;
}> {}

export interface ContextSessionOptions<S extends object, M> {
  readonly path: string;
  readonly state: Schema.ConstraintDecoder<S>;
  readonly message: Schema.ConstraintDecoder<M>;
  readonly messageKey?: (message: M) => string;
  readonly compareMessages?: (left: M, right: M) => number;
  readonly view?: ContextViewPolicy;
  readonly changes?: "none" | "durable-state";
  readonly initial?: {
    readonly state: S;
    readonly messages?: readonly M[];
    readonly description: string;
  };
  readonly persistence?: ContextSessionLayout;
}

export interface ContextSessionSnapshot<S, M> {
  readonly state: S;
  readonly messages: Readonly<Record<string, M>>;
  readonly revision: number;
  readonly description: string;
}

export interface ContextSessionChange<S, M> {
  readonly state?: S;
  readonly messages?: {
    readonly upsert?: readonly M[];
    readonly removeUnchanged?: readonly M[];
  };
}

type CommitOptions = Partial<ContextCommitOptions> & { readonly description?: string };

/** Acquire the Actor's durable documents, validate recovery, and bootstrap only new owners. */
const make = Effect.fn("ContextSession.make")(function* <S extends object, M>(
  options: ContextSessionOptions<S, M>,
) {
  const { path } = options;
  yield* Schema.decodeUnknownEffect(ContextPath)(path).pipe(
    Effect.mapError((cause) => new ContextValidationError({ path, cause })),
  );
  const backend = yield* DurableContext;
  const registry = yield* ContextRegistry;
  const messageKey = options.messageKey ?? ((message: M) => JSON.stringify(message));
  const compareMessages = options.compareMessages ?? (() => 0);
  const index = (messages: readonly M[]) => {
    const entries = messages.map((message) => [messageKey(message), message] as const);
    if (
      entries.some(([key]) => !key) ||
      new Set(entries.map(([key]) => key)).size !== entries.length
    )
      throw new Error("Messages require unique nonempty identities");
    return Object.fromEntries(entries);
  };
  const validate = (record: ContextInput): ContextInput => {
    const state = Schema.decodeUnknownSync(options.state)(record.state);
    const messages = Schema.decodeUnknownSync(Schema.Array(options.message))(record.messages);
    index(messages);
    return { path: record.path, description: record.description, state, messages };
  };
  const partition = yield* Schema.decodeUnknownEffect(ContextSessionLayout)(
    options.persistence ?? { layout: "single" },
  ).pipe(Effect.mapError((cause) => new ContextValidationError({ path, cause })));
  if (partition.layout === "daily") {
    const valid = yield* Effect.try({
      try: () =>
        new Date(`${partition.date}T00:00:00Z`).toISOString().slice(0, 10) === partition.date &&
        Boolean(new Intl.DateTimeFormat("en", { timeZone: partition.timeZone })) &&
        path.endsWith(`/days/${partition.date}`),
      catch: (cause) => new ContextValidationError({ path, cause }),
    });
    if (!valid)
      return yield* new ContextValidationError({
        path,
        cause: new Error("Daily Session requires a valid date and a date-qualified Context path"),
      });
  }
  yield* backend.acquireSession(path, {
    partition,
    messageKey: (message) => messageKey(Schema.decodeUnknownSync(options.message)(message)),
  });
  yield* backend.recover(path, validate);
  yield* registry.views.set(path, options.view);

  const gate = yield* Semaphore.make(1);
  const closed = yield* Ref.make(false);
  yield* Effect.addFinalizer(() => gate.withPermit(Ref.set(closed, true)));
  const available = Effect.gen(function* () {
    if (yield* Ref.get(closed)) return yield* new ContextSessionClosed({ path });
  });
  const read = Effect.fnUntraced(function* () {
    yield* available;
    const stored = backend.get(path);
    if (!stored)
      return yield* new ContextValidationError({
        path,
        cause: new Error("Missing Context Session"),
      });
    return yield* Effect.try({
      try: (): ContextSessionSnapshot<S, M> => ({
        state: Schema.decodeUnknownSync(options.state)(stored.state),
        messages: index(Schema.decodeUnknownSync(Schema.Array(options.message))(stored.messages)),
        revision: stored.revision,
        description: stored.description,
      }),
      catch: (cause) => new ContextValidationError({ path, cause }),
    });
  });
  const set = Effect.fn("ContextSession.set")(function* (
    value: { readonly state: S; readonly messages?: readonly M[]; readonly description?: string },
    commitOptions: Partial<ContextCommitOptions> = {},
  ) {
    yield* available;
    const previous = backend.get(path);
    const validated = yield* Effect.try({
      try: () => {
        const state = Schema.decodeUnknownSync(options.state)(value.state);
        const messages = [
          ...Schema.decodeUnknownSync(Schema.Array(options.message))(
            value.messages ?? previous?.messages ?? [],
          ),
        ].sort(compareMessages);
        index(messages);
        return {
          path,
          state,
          messages,
          description: value.description ?? previous?.description ?? path,
        };
      },
      catch: (cause) => new ContextValidationError({ path, cause }),
    });
    const expectedRevision = commitOptions.expectedRevision ?? previous?.revision ?? 0;
    const event =
      options.changes === "durable-state" && commitOptions.mode !== "bootstrap"
        ? registry.views.project({ ...validated, revision: expectedRevision + 1 })
        : undefined;
    return yield* backend.commit(validated, {
      ...commitOptions,
      expectedRevision,
      ...(event ? { event } : {}),
    });
  });
  if (!backend.get(path) && options.initial) {
    yield* set(
      { ...options.initial, messages: [...(options.initial.messages ?? [])].sort(compareMessages) },
      { expectedRevision: 0, mode: "bootstrap" },
    );
  }
  const commit = Effect.fn("ContextSession.commit")(function* (
    change: (current: ContextSessionSnapshot<S, M>) => ContextSessionChange<S, M>,
    commitOptions: CommitOptions = {},
  ) {
    return yield* gate.withPermit(
      Effect.gen(function* () {
        const current = yield* read();
        if (
          commitOptions.expectedRevision !== undefined &&
          commitOptions.expectedRevision !== current.revision
        )
          return yield* new ContextConflict({
            path,
            expectedRevision: commitOptions.expectedRevision,
            actualRevision: current.revision,
          });
        // User transformations are pure. A thrown programming error stays a defect.
        const next = change(structuredClone(current));
        const messages = yield* Effect.try({
          try: () => {
            const upsert = Schema.decodeUnknownSync(Schema.Array(options.message))(
              next.messages?.upsert ?? [],
            );
            const remove = Schema.decodeUnknownSync(Schema.Array(options.message))(
              next.messages?.removeUnchanged ?? [],
            );
            index([...upsert, ...remove]);
            const values = new Map(Object.entries(current.messages));
            for (const message of upsert) values.set(messageKey(message), message);
            for (const message of remove) {
              const id = messageKey(message);
              if (isDeepStrictEqual(values.get(id), message)) values.delete(id);
            }
            return [...values.values()].sort(compareMessages);
          },
          catch: (cause) => new ContextValidationError({ path, cause }),
        });
        yield* set(
          {
            description: commitOptions.description ?? current.description,
            state: next.state ?? current.state,
            messages,
          },
          { expectedRevision: current.revision, mode: commitOptions.mode },
        );
        return yield* read();
      }).pipe(Effect.uninterruptible),
    );
  });
  const snapshot = gate.withPermit(read());
  return {
    path,
    snapshot,
    current: gate.withPermit(available.pipe(Effect.map(() => backend.get(path)))),
    set: (value: Parameters<typeof set>[0], commitOptions?: Partial<ContextCommitOptions>) =>
      gate.withPermit(set(value, commitOptions).pipe(Effect.uninterruptible)),
    commit,
    state: {
      get: snapshot.pipe(Effect.map((value) => value.state)),
      update: (change: (state: S) => S, options?: CommitOptions) =>
        commit((current) => ({ state: change(current.state) }), options),
    },
    messages: {
      get: (id: string) =>
        snapshot.pipe(
          Effect.map((value) =>
            Option.fromNullishOr(
              Object.hasOwn(value.messages, id) ? value.messages[id] : undefined,
            ),
          ),
        ),
      list: snapshot.pipe(
        Effect.map((value) => Object.values(value.messages).sort(compareMessages)),
      ),
      upsert: (messages: readonly M[], options?: CommitOptions) =>
        commit(() => ({ messages: { upsert: messages } }), options),
      removeUnchanged: (messages: readonly M[], options?: CommitOptions) =>
        commit(() => ({ messages: { removeUnchanged: messages } }), options),
    },
  };
});

export const ContextSession = { make };

export type ContextSession<S extends object, M> = Effect.Success<ReturnType<typeof make<S, M>>>;
