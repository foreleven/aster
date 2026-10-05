import { contextEventId } from "./model.js";
import { DurableContextSnapshot } from "./storage-format.js";
import type { ContextPersistence } from "./persistence.js";
import { publicJson } from "./json.js";
import { isDeepStrictEqual } from "node:util";
import { Cause, Clock, Effect, PubSub, Schema, Semaphore, Stream } from "effect";
import {
  DurableContext,
  restoreContext,
  contextStorageRecord,
  type DurableCommitOptions,
  type StoredContext,
} from "./persistence.js";
import { ContextInput, ContextEvent, type ContextSnapshot, type ContextChange } from "./model.js";
import {
  ContextCommitError,
  ContextConflict,
  ContextRecoveryError,
  ContextValidationError,
} from "./errors.js";

export const makeDurableContext = Effect.fn("DurableContext.make")(function* (
  persistence: ContextPersistence,
) {
  const load = persistence.load.pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(DurableContextSnapshot))),
    Effect.mapError((cause) => new ContextRecoveryError({ path: "/", cause })),
  );
  const initial = yield* load;
  const records = new Map(
    initial.map((record) => [record.path, restoreContext(structuredClone(record))]),
  );
  if (records.size !== initial.length)
    return yield* new ContextRecoveryError({
      path: "/",
      cause: new Error("Duplicate Context paths in storage"),
    });
  const failedCommits = new Map<string, ContextCommitError>();
  const changes = yield* PubSub.unbounded<ContextChange>();
  const writer = yield* Semaphore.make(1);
  const publish = (record: ContextSnapshot) =>
    PubSub.publish(changes, { record: structuredClone(record) });

  const commit = Effect.fn("DurableContext.commit")(function* (
    input: ContextInput,
    options: DurableCommitOptions,
  ) {
    return yield* writer.withPermit(
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
          persistence.save(contextStorageRecord(structuredClone(stored))),
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
        yield* publish(stored.snapshot);
        return structuredClone(stored.snapshot);
        // Waiting writers remain interruptible; admitted storage and publication drain together.
      }).pipe(Effect.uninterruptible),
    );
  });

  const recover = Effect.fn("DurableContext.recover")(function* (
    path: string,
    validate: (record: ContextSnapshot) => ContextInput,
  ) {
    return yield* writer.withPermit(
      Effect.gen(function* () {
        const previous = records.get(path);
        const failed = failedCommits.has(path);
        const encoded = failed ? (yield* load).find((record) => record.path === path) : undefined;
        const restored = failed ? (encoded ? restoreContext(encoded) : undefined) : previous;
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
            if (!isDeepStrictEqual(previous, next)) yield* publish(snapshot);
          }
        }
        failedCommits.delete(path);
      }).pipe(Effect.uninterruptible),
    );
  });
  return DurableContext.of({
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
    exportRecords: () =>
      [...records.values()].map((stored) => contextStorageRecord(structuredClone(stored))),
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
