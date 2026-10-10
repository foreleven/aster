import { isDeepStrictEqual } from "node:util";
import {
  Array as EffectArray,
  Cause,
  Clock,
  Context,
  Deferred,
  Effect,
  Option,
  PubSub,
  Schema,
  Semaphore,
  Stream,
  type Scope,
} from "effect";
import { publicJson } from "../json.js";
import { ContextText } from "./contracts.js";
import {
  ContextInput,
  ContextPath,
  ContextEvent,
  StoredContext,
  contextEventId,
  type ContextSnapshot,
  type ContextChange,
  type ContextEntry,
} from "./model.js";
import {
  ContextClosed,
  ContextCommitError,
  ContextConflict,
  ContextMessageConflict,
  ContextRecoveryError,
  ContextValidationError,
} from "./errors.js";
import type {
  ContextHandle,
  ContextOpenOptions,
  ContextOwnerSnapshot,
  ContextMutation,
  ContextCommitOptions,
  ContextError,
  ContextLayout,
} from "./handle.js";
export type {
  ContextHandle,
  ContextOpenOptions,
  ContextOwnerSnapshot,
  ContextMutation,
  ContextCommitOptions,
  ContextError,
  ContextLayout,
} from "./handle.js";

export type ContextPartition = typeof ContextPartition.Type;
export const ContextPartition = Schema.Union([
  Schema.Struct({ layout: Schema.Literal("single") }),
  Schema.Struct({
    layout: Schema.Literal("daily"),
    date: Schema.String.check(Schema.isPattern(/^\d{4}-\d{2}-\d{2}$/)),
    timeZone: Schema.NonEmptyString,
  }),
]);
/** Infrastructure routing; these callbacks never enter stored records. */
export interface ContextStorageOptions {
  readonly partition: ContextLayout;
  readonly keyOf: (message: unknown) => string;
}
export interface ContextPersistence {
  readonly load: Effect.Effect<readonly StoredContext[], ContextRecoveryError>;
  readonly save: (
    record: StoredContext,
    owner: ContextStorageOptions,
  ) => Effect.Effect<void, ContextCommitError>;
  readonly bind?: (
    path: string,
    owner: ContextStorageOptions,
  ) => Effect.Effect<void, ContextRecoveryError>;
}
const textOf = (snapshot: ContextSnapshot): ContextText => ({
  path: snapshot.path,
  revision: snapshot.revision,
  description: snapshot.description,
  text: snapshot.text,
});

/** One Context authority: scoped owners, canonical commits, text reads and publication. */
export class DurableContext extends Context.Service<
  DurableContext,
  {
    readonly open: <S extends object, M>(
      options: ContextOpenOptions<S, M>,
    ) => Effect.Effect<ContextHandle<S, M>, ContextError, Scope.Scope>;
    readonly get: (path: string) => ContextText | undefined;
    /** Internal domain coordination over committed data, independent of Actor startup. Not a transport read API. */
    readonly readState: <S>(
      path: string,
      schema: Schema.ConstraintDecoder<S>,
    ) => Effect.Effect<S, ContextValidationError>;
    readonly snapshot: () => Readonly<Record<string, ContextText>>;
    readonly directory: () => readonly ContextEntry[];
    readonly journal: () => readonly ContextEvent[];
    readonly changes: Stream.Stream<ContextChange>;
    readonly subscribe: Effect.Effect<Stream.Stream<ContextChange>, never, Scope.Scope>;
    readonly watch: (path: string) => Stream.Stream<ContextText | undefined>;
    readonly serializeState: (path: string) => Effect.Effect<string, ContextValidationError>;
  }
>()("context/DurableContext") {}

export const makeDurableContext = Effect.fn("DurableContext.make")(function* (
  persistence: ContextPersistence,
) {
  const load = persistence.load.pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(StoredContext))),
    Effect.mapError((cause) => new ContextRecoveryError({ path: "/", cause })),
  );
  const initial = yield* load;
  const records = new Map(initial.map((record) => [record.snapshot.path, structuredClone(record)]));
  if (records.size !== initial.length)
    return yield* new ContextRecoveryError({
      path: "/",
      cause: new Error("Duplicate Context paths in storage"),
    });
  const failed = new Map<string, ContextCommitError>();
  const owners = new Set<string>();
  const writers = new Map<string, Semaphore.Semaphore>();
  const writerFor = (path: string) => {
    let writer = writers.get(path);
    if (!writer) {
      writer = Semaphore.makeUnsafe(1);
      writers.set(path, writer);
    }
    return writer;
  };
  const publications = yield* PubSub.unbounded<{
    readonly snapshot: ContextSnapshot;
    readonly events: readonly ContextEvent[];
  }>();
  yield* Effect.addFinalizer(() => PubSub.shutdown(publications));
  const changeOf = ({
    snapshot,
    events,
  }: {
    snapshot: ContextSnapshot;
    events: readonly ContextEvent[];
  }): ContextChange => ({
    record: textOf(snapshot),
    ...(events.length ? { events: structuredClone(events) } : {}),
  });
  const publish = (stored: StoredContext, events: readonly ContextEvent[]) =>
    PubSub.publish(publications, structuredClone({ snapshot: stored.snapshot, events }));
  const subscribe = PubSub.subscribe(publications).pipe(
    Effect.map((subscription) =>
      Stream.fromEffectRepeat(PubSub.take(subscription)).pipe(Stream.map(changeOf)),
    ),
  );
  const watchOwner = (path: string) =>
    Stream.unwrap(
      writerFor(path).withPermit(
        Effect.gen(function* () {
          const subscription = yield* PubSub.subscribe(publications);
          const current = structuredClone(records.get(path)?.snapshot);
          return Stream.concat(
            Stream.succeed(current),
            Stream.fromEffectRepeat(PubSub.take(subscription)).pipe(
              Stream.filter((change) => change.snapshot.path === path),
              Stream.map((change) => structuredClone(change.snapshot)),
            ),
          );
        }),
      ),
    );

  // The path writer is held by open and every handle operation. There is one admission gate.
  const persist = Effect.fn("DurableContext.persist")(function* (
    input: ContextInput,
    text: string,
    options: ContextCommitOptions,
    owner: ContextStorageOptions,
    eventsEnabled: boolean,
  ) {
    const path = input.path;
    const failure = failed.get(path);
    if (failure) return yield* failure;
    const previous = records.get(path);
    const actual = previous?.snapshot.revision ?? 0;
    if (options.expectedRevision !== undefined && options.expectedRevision !== actual)
      return yield* new ContextConflict({
        path,
        expectedRevision: options.expectedRevision,
        actualRevision: actual,
      });
    const normalized = yield* Effect.try({
      try: () => publicJson(input),
      catch: (cause) => new ContextValidationError({ path, cause }),
    });
    const content = yield* Schema.decodeUnknownEffect(ContextInput)(normalized).pipe(
      Effect.mapError((cause) => new ContextValidationError({ path, cause })),
    );
    if (previous && isDeepStrictEqual({ ...content, text, revision: actual }, previous.snapshot))
      return structuredClone(previous.snapshot);
    const revision = actual + 1;
    const snapshot = { ...content, text, revision };
    const events = [...(previous?.events ?? [])];
    if (
      eventsEnabled &&
      options.mode !== "bootstrap" &&
      !isDeepStrictEqual(previous?.snapshot.state, content.state) &&
      previous?.snapshot.text !== text
    ) {
      events.push({
        id: contextEventId(path, revision),
        record: textOf(snapshot),
        createdAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
      });
    }
    const stored: StoredContext = structuredClone({ snapshot, events });
    yield* Effect.suspend(() => persistence.save(structuredClone(stored), owner)).pipe(
      Effect.tapCause((cause) =>
        Effect.sync(() => {
          const error = Cause.findError(cause);
          failed.set(
            path,
            error._tag === "Success" ? error.success : new ContextCommitError({ path, cause }),
          );
        }),
      ),
    );
    records.set(path, stored);
    yield* publish(stored, events.slice(previous?.events.length ?? 0));
    return structuredClone(snapshot);
  });

  const open: DurableContext["Service"]["open"] = Effect.fn("DurableContext.open")(function* <
    S extends object,
    M,
  >(options: ContextOpenOptions<S, M>) {
    const { path } = options;
    const gate = writerFor(path);
    yield* Schema.decodeUnknownEffect(ContextPath)(path).pipe(
      Effect.mapError((cause) => new ContextValidationError({ path, cause })),
    );
    const partition = yield* Schema.decodeUnknownEffect(ContextPartition)(
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
          cause: new Error("Daily Context requires a valid date and a date-qualified path"),
        });
    }
    const keyOf = options.keyOf ?? ((message: M) => JSON.stringify(publicJson(message)));
    const decodeMessages = Schema.decodeUnknownSync(Schema.Array(options.message));
    const decodeState = Schema.decodeUnknownSync(options.state);
    const validateKeys = (messages: readonly M[]) => {
      const keys = messages.map(keyOf);
      if (keys.some((key) => !key) || new Set(keys).size !== keys.length)
        throw new Error("Messages require unique nonempty keys");
      return keys;
    };
    const owner: ContextStorageOptions = {
      partition,
      keyOf: (message) => keyOf(Schema.decodeUnknownSync(options.message)(message)),
    };
    const closed = yield* Deferred.make<void>();
    const available = Effect.gen(function* () {
      if (yield* Deferred.isDone(closed)) return yield* new ContextClosed({ path });
    });
    const decode = (snapshot: ContextSnapshot): ContextOwnerSnapshot<S, M> => {
      const messages = decodeMessages(snapshot.messages);
      validateKeys(messages);
      return {
        path,
        state: decodeState(snapshot.state),
        messages,
        description: snapshot.description,
        revision: snapshot.revision,
      };
    };
    const readCurrent = available.pipe(
      Effect.flatMap(() =>
        Effect.try({
          try: () => {
            const stored = records.get(path);
            return stored ? decode(structuredClone(stored.snapshot)) : undefined;
          },
          catch: (cause) => new ContextValidationError({ path, cause }),
        }),
      ),
    );
    const read = readCurrent.pipe(
      Effect.flatMap((current) =>
        current
          ? Effect.succeed(current)
          : Effect.fail(
              new ContextValidationError({
                path,
                cause: new Error("Context has not been initialized"),
              }),
            ),
      ),
    );
    const save = Effect.fnUntraced(function* (
      state: S,
      messages: readonly M[],
      description: string,
      commitOptions: ContextCommitOptions,
    ) {
      const value = yield* Effect.try({
        try: () => {
          const stateValue = decodeState(state);
          const messageValues = decodeMessages(messages);
          validateKeys(messageValues);
          return { path, state: stateValue, messages: messageValues, description };
        },
        catch: (cause) => new ContextValidationError({ path, cause }),
      });
      // Formatting is domain code: a thrown programming error remains a defect.
      const text = options.serialize(structuredClone(value.state));
      yield* Schema.decodeUnknownEffect(Schema.String)(text).pipe(
        Effect.mapError((cause) => new ContextValidationError({ path, cause })),
      );
      return decode(yield* persist(value, text, commitOptions, owner, options.events === "state"));
    });
    yield* Effect.acquireRelease(
      gate.withPermit(
        Effect.gen(function* () {
          if (owners.has(path))
            return yield* new ContextRecoveryError({
              path,
              cause: new Error("Context already has an owner"),
            });
          if (failed.has(path)) {
            const previous = records.get(path);
            const restored = (yield* load).find((record) => record.snapshot.path === path);
            if (
              (previous && !restored) ||
              (restored && restored.snapshot.revision < (previous?.snapshot.revision ?? 0))
            )
              return yield* new ContextRecoveryError({
                path,
                cause: new Error("Context missing or regressed during recovery"),
              });
            if (restored) {
              yield* Effect.try({
                try: () => decode(restored.snapshot),
                catch: (cause) => new ContextRecoveryError({ path, cause }),
              });
              records.set(path, structuredClone(restored));
              if (!isDeepStrictEqual(previous, restored))
                yield* publish(
                  restored,
                  restored.events.filter(
                    (event) => event.record.revision > (previous?.snapshot.revision ?? 0),
                  ),
                );
            }
            failed.delete(path);
          }
          yield* persistence.bind?.(path, owner) ?? Effect.void;
          const previous = records.get(path);
          if (previous) {
            const restored = yield* Effect.try({
              try: () => decode(previous.snapshot),
              catch: (cause) => new ContextRecoveryError({ path, cause }),
            });
            yield* save(restored.state, restored.messages, restored.description, {
              mode: "bootstrap",
            });
          } else if (options.initial) {
            yield* save(
              options.initial.state,
              options.initial.messages ?? [],
              options.initial.description,
              { mode: "bootstrap" },
            );
          }
          owners.add(path);
        }).pipe(Effect.uninterruptible),
      ),
      () =>
        gate.withPermit(
          Effect.gen(function* () {
            yield* Deferred.succeed(closed, undefined);
            owners.delete(path);
          }),
        ),
    );

    const commit = Effect.fn("DurableContext.commit")(function* (
      change: (current: ContextOwnerSnapshot<S, M>) => ContextMutation<S, M>,
      commitOptions: ContextCommitOptions = {},
    ) {
      return yield* gate.withPermit(
        Effect.gen(function* () {
          const current = yield* read;
          if (
            commitOptions.expectedRevision !== undefined &&
            commitOptions.expectedRevision !== current.revision
          )
            return yield* new ContextConflict({
              path,
              expectedRevision: commitOptions.expectedRevision,
              actualRevision: current.revision,
            });
          const next = change(structuredClone(current));
          const append = yield* Effect.try({
            try: () => {
              const messages = decodeMessages(next.messages?.append ?? []);
              validateKeys(messages);
              return messages;
            },
            catch: (cause) => new ContextValidationError({ path, cause }),
          });
          const remove = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.NonEmptyString))(
            next.messages?.remove ?? [],
          ).pipe(Effect.mapError((cause) => new ContextValidationError({ path, cause })));
          const removed = new Set(remove);
          if (
            removed.size !== remove.length ||
            append.some((message) => removed.has(keyOf(message)))
          )
            return yield* new ContextValidationError({
              path,
              cause: new Error("Duplicate or overlapping message operations"),
            });
          const values = new Map(current.messages.map((message) => [keyOf(message), message]));
          for (const message of append) {
            const key = keyOf(message);
            if (values.has(key)) {
              if (!isDeepStrictEqual(values.get(key), message))
                return yield* new ContextMessageConflict({ path, key });
            } else values.set(key, message);
          }
          for (const key of remove) values.delete(key);
          return yield* save(
            next.state ?? current.state,
            [...values.values()],
            commitOptions.description ?? current.description,
            { ...commitOptions, expectedRevision: current.revision },
          );
        }).pipe(Effect.uninterruptible),
      );
    });
    const snapshot = gate.withPermit(read);
    const watch = Stream.unwrap(
      available.pipe(
        Effect.as(watchOwner(path).pipe(Stream.interruptWhen(Deferred.await(closed)))),
      ),
    );
    return {
      path,
      snapshot,
      current: gate.withPermit(readCurrent),
      commit,
      serializeState: gate.withPermit(
        available.pipe(
          Effect.flatMap(() => {
            const record = records.get(path);
            return record
              ? Effect.succeed(record.snapshot.text)
              : Effect.fail(
                  new ContextValidationError({
                    path,
                    cause: new Error("Context has not been initialized"),
                  }),
                );
          }),
        ),
      ),
      state: {
        get: snapshot.pipe(Effect.map((value) => value.state)),
        set: (state, commitOptions = {}) =>
          gate.withPermit(
            Effect.gen(function* () {
              const current = yield* readCurrent;
              return yield* save(
                state,
                current?.messages ?? [],
                commitOptions.description ?? current?.description ?? path,
                commitOptions,
              );
            }).pipe(Effect.uninterruptible),
          ),
        update: (change, commitOptions) =>
          commit((current) => ({ state: change(current.state) }), commitOptions),
        watch: watch.pipe(
          Stream.filter((value) => value !== undefined),
          Stream.mapEffect((value) =>
            Effect.try({
              try: () => decodeState(value.state),
              catch: (cause) => new ContextValidationError({ path, cause }),
            }),
          ),
          Stream.changesWith((left: S, right: S) => isDeepStrictEqual(left, right)),
        ),
      },
      messages: {
        get: (key) =>
          snapshot.pipe(
            Effect.map((value) =>
              EffectArray.findFirst(value.messages, (message) => keyOf(message) === key),
            ),
          ),
        list: snapshot.pipe(Effect.map((value) => value.messages)),
        append: (messages, commitOptions) =>
          commit(() => ({ messages: { append: messages } }), commitOptions),
        remove: (keys, commitOptions) =>
          commit(() => ({ messages: { remove: keys } }), commitOptions),
        watch: (key) =>
          watch.pipe(
            Stream.mapEffect((value) =>
              Effect.try({
                try: () =>
                  value
                    ? EffectArray.findFirst(
                        decodeMessages(value.messages),
                        (message) => keyOf(message) === key,
                      )
                    : Option.none(),
                catch: (cause) => new ContextValidationError({ path, cause }),
              }),
            ),
            Stream.changesWith((left: Option.Option<M>, right: Option.Option<M>) =>
              isDeepStrictEqual(left, right),
            ),
            Stream.takeUntil(Option.isNone),
          ),
      },
    } satisfies ContextHandle<S, M>;
  });
  return DurableContext.of({
    open,
    get: (path) => {
      const stored = records.get(path);
      return stored ? textOf(stored.snapshot) : undefined;
    },
    readState: (path, schema) =>
      Effect.suspend(() =>
        Schema.decodeUnknownEffect(schema)(structuredClone(records.get(path)?.snapshot.state)).pipe(
          Effect.mapError((cause) => new ContextValidationError({ path, cause })),
        ),
      ),
    snapshot: () =>
      Object.fromEntries([...records].map(([path, stored]) => [path, textOf(stored.snapshot)])),
    directory: () =>
      [...records.values()].map(({ snapshot }) => ({
        path: snapshot.path,
        description: snapshot.description,
      })),
    journal: () => structuredClone([...records.values()].flatMap((record) => record.events)),
    subscribe,
    changes: Stream.fromPubSub(publications).pipe(Stream.map(changeOf)),
    watch: (path) =>
      watchOwner(path).pipe(Stream.map((record) => (record ? textOf(record) : undefined))),
    serializeState: (path) =>
      Effect.suspend(() => {
        const stored = records.get(path);
        return stored
          ? Effect.succeed(stored.snapshot.text)
          : Effect.fail(
              new ContextValidationError({ path, cause: new Error("Context not found") }),
            );
      }),
  });
});
