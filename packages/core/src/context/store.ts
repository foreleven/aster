import { isDeepStrictEqual } from "node:util";
import type { PublicContext } from "./contracts.js";
import {
  Cause,
  Clock,
  Context,
  Effect,
  PubSub,
  Schema,
  Semaphore,
  Stream,
  type Scope,
} from "effect";
import { publicJson } from "../json.js";
import type { ContextSessionStorage } from "./session-storage.js";
import {
  ContextInput,
  ContextEvent,
  StoredContext,
  contextEventId,
  type ContextSnapshot,
  type ContextChange,
  type ContextEntry,
} from "./model.js";
import {
  ContextCommitError,
  ContextConflict,
  ContextRecoveryError,
  ContextValidationError,
} from "./errors.js";

/** Drivers own serialization and atomic recovery; the kernel owns revision/publication semantics. */
export interface ContextPersistence {
  readonly load: Effect.Effect<readonly StoredContext[], ContextRecoveryError>;
  readonly save: (
    record: StoredContext,
    session?: ContextSessionStorage,
  ) => Effect.Effect<void, ContextCommitError>;
  readonly configureSession?: (
    path: string,
    session: ContextSessionStorage,
  ) => Effect.Effect<void, ContextRecoveryError>;
}

export interface ContextCommitOptions {
  readonly expectedRevision: number;
  readonly mode?: "update" | "bootstrap";
}

export interface DurableCommitOptions extends ContextCommitOptions {
  /** A registry-owned public source snapshot, atomically retained on state changes. */
  readonly event?: PublicContext;
}

/** Canonical storage boundary. A commit contains the complete state, ordered
 * messages, and the owner's receipt/outbox state; these are never separate writes.
 * Agent execution is an optional consumer, not a requirement of this service. */
export class DurableContext extends Context.Service<
  DurableContext,
  {
    readonly acquireSession: (
      path: string,
      session: ContextSessionStorage,
    ) => Effect.Effect<void, ContextRecoveryError, Scope.Scope>;
    readonly commit: (
      record: ContextInput,
      options: DurableCommitOptions,
    ) => Effect.Effect<
      ContextSnapshot,
      ContextConflict | ContextValidationError | ContextCommitError
    >;
    /** Reconcile an uncertain commit before allowing the same owner to write again. */
    readonly recover: (
      path: string,
      validate: (record: ContextSnapshot) => ContextInput,
    ) => Effect.Effect<void, ContextRecoveryError>;
    /** Detached views of the last known committed snapshot; no file handles escape. */
    readonly get: (path: string) => ContextSnapshot | undefined;
    readonly snapshot: () => Readonly<Record<string, ContextSnapshot>>;
    readonly directory: () => readonly ContextEntry[];
    /** Durable evidence is available only to consumers and infrastructure. */
    readonly journal: () => readonly ContextEvent[];
    readonly exportRecords: () => readonly StoredContext[];
    readonly changes: Stream.Stream<ContextChange>;
    readonly subscribe: Effect.Effect<Stream.Stream<ContextChange>, never, Scope.Scope>;
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
  const failedCommits = new Map<string, ContextCommitError>();
  const changes = yield* PubSub.unbounded<ContextChange>();
  // Gates live as long as their registered paths. Only writes to the same Context serialize.
  const writers = new Map<string, Semaphore.Semaphore>();
  const owners = new Set<string>();
  const sessions = new Map<string, ContextSessionStorage>();
  const writerFor = (path: string) => {
    let writer = writers.get(path);
    if (!writer) {
      writer = Semaphore.makeUnsafe(1);
      writers.set(path, writer);
    }
    return writer;
  };
  const publish = (record: ContextSnapshot, events: readonly ContextEvent[] = []) =>
    PubSub.publish(changes, structuredClone({ record, ...(events.length ? { events } : {}) }));

  const commit = Effect.fn("DurableContext.commit")(function* (
    input: ContextInput,
    options: DurableCommitOptions,
  ) {
    return yield* writerFor(input.path).withPermit(
      Effect.gen(function* () {
        const failed = failedCommits.get(input.path);
        if (failed) return yield* failed;
        const previous = records.get(input.path);
        const actualRevision = previous?.snapshot.revision ?? 0;
        if (options.expectedRevision !== actualRevision)
          return yield* new ContextConflict({
            path: input.path,
            expectedRevision: options.expectedRevision,
            actualRevision,
          });
        const content = yield* Schema.decodeUnknownEffect(ContextInput)(input).pipe(
          Effect.mapError((cause) => new ContextValidationError({ path: input.path, cause })),
        );
        if (
          previous &&
          previous.snapshot.description === content.description &&
          isDeepStrictEqual(previous.snapshot.state, content.state) &&
          isDeepStrictEqual(previous.snapshot.messages, content.messages)
        )
          return structuredClone(previous.snapshot);
        const revision = actualRevision + 1;
        const stateChanged =
          !previous || !isDeepStrictEqual(previous.snapshot.state, content.state);
        const events = [...(previous?.events ?? [])];
        if (options.event && stateChanged && options.mode !== "bootstrap") {
          const record = yield* Effect.try({
            try: () =>
              Schema.decodeUnknownSync(ContextEvent.fields.record)(
                publicJson({ ...options.event, path: input.path, revision }),
              ),
            catch: (cause) => new ContextValidationError({ path: input.path, cause }),
          });
          events.push({
            id: contextEventId(input.path, revision),
            record,
            createdAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
          });
        }
        const stored: StoredContext = structuredClone({
          snapshot: { ...content, revision },
          events,
        });
        yield* Effect.suspend(() =>
          persistence.save(structuredClone(stored), sessions.get(input.path)),
        ).pipe(
          Effect.tapCause((cause) =>
            Effect.sync(() => {
              const failure = Cause.findError(cause);
              failedCommits.set(
                input.path,
                failure._tag === "Success"
                  ? failure.success
                  : new ContextCommitError({ path: input.path, cause }),
              );
            }),
          ),
        );
        records.set(input.path, stored);
        yield* publish(stored.snapshot, events.slice(previous?.events.length ?? 0));
        return structuredClone(stored.snapshot);
        // Waiting writers remain interruptible; admitted storage and publication drain together.
      }).pipe(Effect.uninterruptible),
    );
  });

  const recover = Effect.fn("DurableContext.recover")(function* (
    path: string,
    validate: (record: ContextSnapshot) => ContextInput,
  ) {
    return yield* writerFor(path).withPermit(
      Effect.gen(function* () {
        const previous = records.get(path);
        const failed = failedCommits.has(path);
        const encoded = failed
          ? (yield* load).find((record) => record.snapshot.path === path)
          : undefined;
        const restored = failed ? encoded : previous;
        if (!restored && previous)
          return yield* new ContextRecoveryError({
            path,
            cause: new Error(`Context missing during storage recovery: ${path}`),
          });
        if (restored) {
          const snapshot = yield* Effect.try({
            try: () => ({
              ...validate(structuredClone(restored.snapshot)),
              revision: restored.snapshot.revision,
            }),
            catch: (cause) => new ContextRecoveryError({ path, cause }),
          });
          if (snapshot.revision < (previous?.snapshot.revision ?? 0))
            return yield* new ContextRecoveryError({
              path,
              cause: new Error(`Context revision regressed during storage recovery: ${path}`),
            });
          if (failed) {
            const next = structuredClone({ snapshot, events: restored.events });
            records.set(path, next);
            if (!isDeepStrictEqual(previous, next))
              yield* publish(
                snapshot,
                restored.events.filter(
                  (event) => event.record.revision > (previous?.snapshot.revision ?? 0),
                ),
              );
          }
        }
        failedCommits.delete(path);
      }).pipe(Effect.uninterruptible),
    );
  });
  const acquireSession = (path: string, session: ContextSessionStorage) =>
    Effect.acquireRelease(
      writerFor(path).withPermit(
        Effect.gen(function* () {
          if (owners.has(path))
            return yield* new ContextRecoveryError({
              path,
              cause: new Error("Context Session already has an owner"),
            });
          const previous = sessions.get(path);
          if (previous && !isDeepStrictEqual(previous.partition, session.partition))
            return yield* new ContextRecoveryError({
              path,
              cause: new Error("Context Session partition cannot change"),
            });
          yield* persistence.configureSession?.(path, session) ?? Effect.void;
          sessions.set(path, session);
          owners.add(path);
        }),
      ),
      () =>
        Effect.sync(() => {
          owners.delete(path);
        }),
    );
  return DurableContext.of({
    acquireSession,
    commit,
    recover,
    get: (path) => {
      const stored = records.get(path);
      return stored ? structuredClone(stored.snapshot) : undefined;
    },
    snapshot: () =>
      Object.fromEntries(
        [...records].map(([path, stored]) => [path, structuredClone(stored.snapshot)]),
      ),
    journal: () => structuredClone([...records.values()].flatMap((stored) => stored.events)),
    exportRecords: () => structuredClone([...records.values()]),
    directory: () =>
      [...records.values()].map(({ snapshot }) => ({
        path: snapshot.path,
        description: snapshot.description,
      })),
    changes: Stream.fromPubSub(changes).pipe(Stream.map((change) => structuredClone(change))),
    subscribe: PubSub.subscribe(changes).pipe(
      Effect.map((subscription) =>
        Stream.fromEffectRepeat(PubSub.take(subscription)).pipe(
          Stream.map((change) => structuredClone(change)),
        ),
      ),
    ),
  });
});
