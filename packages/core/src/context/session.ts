import { isDeepStrictEqual } from "node:util";
import { Data, Effect, Option, Ref, Schema, Semaphore } from "effect";
import { defineContext, type ContextDefinition } from "./definition.js";
import { ContextRegistry } from "./registry.js";
import { ContextConflict, ContextValidationError } from "./errors.js";
import { ContextSessionLayout } from "./session-storage.js";
import type { ContextCommitOptions } from "./store.js";

export class ContextSessionClosed extends Data.TaggedError("ContextSessionClosed")<{
  readonly path: string;
}> {}

export interface ContextSessionDefinition<S extends object, M> extends ContextDefinition {
  readonly stateSchema: Schema.ConstraintDecoder<S>;
  readonly messageSchema: Schema.ConstraintDecoder<M>;
  readonly messageKey: (message: M) => string;
  readonly compareMessages: (left: M, right: M) => number;
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

const define = <S extends object, M>(
  options: Parameters<typeof defineContext<S, M>>[0] & {
    readonly messageKey: (message: M) => string;
    readonly compareMessages: (left: M, right: M) => number;
  },
): ContextSessionDefinition<S, M> => ({
  ...defineContext(options),
  stateSchema: options.state,
  messageSchema: options.message,
  messageKey: options.messageKey,
  compareMessages: options.compareMessages,
});

const open = Effect.fn("ContextSession.open")(function* <S extends object, M>(options: {
  readonly path: string;
  readonly definition: ContextSessionDefinition<S, M>;
  readonly initial: {
    readonly state: S;
    readonly messages: readonly M[];
    readonly description: string;
  };
  readonly persistence?: ContextSessionLayout;
}) {
  const { path, definition } = options;
  const registry = yield* ContextRegistry;
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
  yield* registry.acquireSession(path, {
    partition,
    messageKey: (message) =>
      definition.messageKey(Schema.decodeUnknownSync(definition.messageSchema)(message)),
  });
  yield* registry.register(path, definition);
  const gate = yield* Semaphore.make(1);
  const closed = yield* Ref.make(false);
  yield* Effect.addFinalizer(() => gate.withPermit(Ref.set(closed, true)));
  const available = Effect.gen(function* () {
    if (yield* Ref.get(closed)) return yield* new ContextSessionClosed({ path });
  });
  const index = (messages: readonly M[]) => {
    const entries = messages.map((message) => [definition.messageKey(message), message] as const);
    if (
      entries.some(([key]) => !key) ||
      new Set(entries.map(([key]) => key)).size !== entries.length
    )
      throw new Error("Messages require unique nonempty identities");
    return Object.fromEntries(entries);
  };
  const read = Effect.fnUntraced(function* () {
    yield* available;
    const stored = registry.get(path);
    if (!stored)
      return yield* new ContextValidationError({
        path,
        cause: new Error("Missing Context Session"),
      });
    return yield* Effect.try({
      try: (): ContextSessionSnapshot<S, M> => ({
        state: Schema.decodeUnknownSync(definition.stateSchema)(stored.state),
        messages: index(
          Schema.decodeUnknownSync(Schema.Array(definition.messageSchema))(stored.messages),
        ),
        revision: stored.revision,
        description: stored.description,
      }),
      catch: (cause) => new ContextValidationError({ path, cause }),
    });
  });
  if (!registry.get(path)) {
    yield* Effect.try({
      try: () => index(options.initial.messages),
      catch: (cause) => new ContextValidationError({ path, cause }),
    });
    yield* registry.commit(
      {
        path,
        ...options.initial,
        messages: [...options.initial.messages].sort(definition.compareMessages),
      },
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
            const upsert = Schema.decodeUnknownSync(Schema.Array(definition.messageSchema))(
              next.messages?.upsert ?? [],
            );
            const remove = Schema.decodeUnknownSync(Schema.Array(definition.messageSchema))(
              next.messages?.removeUnchanged ?? [],
            );
            index([...upsert, ...remove]);
            const values = new Map(Object.entries(current.messages));
            for (const message of upsert) values.set(definition.messageKey(message), message);
            for (const message of remove) {
              const id = definition.messageKey(message);
              if (isDeepStrictEqual(values.get(id), message)) values.delete(id);
            }
            return [...values.values()].sort(definition.compareMessages);
          },
          catch: (cause) => new ContextValidationError({ path, cause }),
        });
        yield* registry.commit(
          {
            path,
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
    snapshot,
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
        Effect.map((value) => Object.values(value.messages).sort(definition.compareMessages)),
      ),
      upsert: (messages: readonly M[], options?: CommitOptions) =>
        commit(() => ({ messages: { upsert: messages } }), options),
      removeUnchanged: (messages: readonly M[], options?: CommitOptions) =>
        commit(() => ({ messages: { removeUnchanged: messages } }), options),
    },
  };
});

export const ContextSession = { define, open };
