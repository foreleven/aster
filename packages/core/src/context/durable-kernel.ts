import { reactionEventId } from "./reaction-event.js";
import { PublicContext } from "@aster/api-contracts";
import { publicJson } from "./json.js";
import { isDeepStrictEqual } from "node:util";
import { Cause, Clock, Effect, PubSub, Schema, Semaphore, Stream } from "effect";
import { DurableContext, DurableContextSnapshot, type ContextCommitOptions } from "./durable.js";
import { ContextRecord, type ContextChange } from "./model.js";
import {
  ContextCommitError,
  ContextConflict,
  ContextRecoveryError,
  ContextValidationError,
} from "./errors.js";

/** Storage drivers own serialization and atomic recovery. The canonical kernel
 * shares revision, publication and uncertainty rules across those drivers. */
export interface ContextPersistence {
  readonly load: Effect.Effect<readonly ContextRecord[], ContextRecoveryError>;
  readonly save: (record: ContextRecord) => Effect.Effect<void, ContextCommitError>;
}

export const makeDurableContext = Effect.fn("DurableContext.make")(function* (
  kind: "local" | "pi",
  persistence: ContextPersistence,
) {
  const loaded = yield* persistence.load;
  const initial = yield* Schema.decodeUnknownEffect(Schema.Array(DurableContextSnapshot))(
    loaded,
  ).pipe(Effect.mapError((cause) => new ContextRecoveryError({ path: "/", cause })));
  const records = new Map(initial.map((record) => [record.path, structuredClone(record)]));
  if (records.size !== initial.length)
    return yield* new ContextRecoveryError({
      path: "/",
      cause: new Error("Duplicate Context paths in storage"),
    });
  const failedCommits = new Map<string, ContextCommitError>();
  const changes = yield* PubSub.unbounded<ContextChange>();
  const writer = yield* Semaphore.make(1);

  const commit = Effect.fn("DurableContext.commit")(function* (
    input: ContextRecord,
    options: ContextCommitOptions,
  ) {
    return yield* writer.withPermit(
      Effect.gen(function* () {
        const failed = failedCommits.get(input.path);
        if (failed) return yield* failed;
        const previous = records.get(input.path);
        const actualRevision = previous?.revision ?? 0;
        if (options.expectedRevision !== actualRevision)
          return yield* new ContextConflict({
            path: input.path,
            expectedRevision: options.expectedRevision,
            actualRevision,
          });
        const validated = yield* Schema.decodeUnknownEffect(DurableContextSnapshot)(input).pipe(
          Effect.mapError((cause) => new ContextValidationError({ path: input.path, cause })),
        );
        if (
          previous &&
          previous.description === validated.description &&
          isDeepStrictEqual(previous.state, validated.state) &&
          isDeepStrictEqual(previous.messages, validated.messages)
        )
          return structuredClone(previous);
        const revision = actualRevision + 1;
        const stateChanged =
          previous === undefined || !isDeepStrictEqual(previous.state, validated.state);
        // Source owners cannot rewrite or drop accepted handoffs through ordinary state commits.
        const { reactionEvents: _untrustedEvents, ...content } = validated;
        const events = [...(previous?.reactionEvents ?? [])];
        if (options.reaction && stateChanged && options.evaluate !== false) {
          const source = yield* Effect.try({
            try: () =>
              Schema.decodeUnknownSync(PublicContext)(
                publicJson({ ...options.reaction, path: input.path, revision }),
              ),
            catch: (cause) => new ContextValidationError({ path: input.path, cause }),
          });
          const requestId = reactionEventId(input.path, revision);
          events.push({
            requestId,
            causationId: requestId,
            source: input.path,
            target: "/system-one",
            revision,
            record: source,
            createdAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
          });
        }
        const record = structuredClone({
          ...content,
          revision,
          ...(events.length ? { reactionEvents: events } : {}),
        });
        yield* Effect.suspend(() => persistence.save(structuredClone(record))).pipe(
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
        records.set(record.path, record);
        yield* PubSub.publish(changes, {
          path: record.path,
          created: previous === undefined,
          stateChanged,
          record: structuredClone(record),
          ...(options.evaluate === false ? { evaluate: false } : {}),
        });
        return structuredClone(record);
        // Once a commit starts, drain storage plus publication before releasing the
        // writer. Cancellation while waiting for the permit does not start a write.
      }).pipe(Effect.uninterruptible),
    );
  });

  const recover = Effect.fn("DurableContext.recover")(function* (
    path: string,
    validate: (record: ContextRecord) => ContextRecord,
  ) {
    return yield* writer.withPermit(
      Effect.gen(function* () {
        const previous = records.get(path);
        const failed = failedCommits.has(path);
        const restored = failed
          ? (yield* persistence.load).find((record) => record.path === path)
          : previous;
        if (!restored && previous)
          return yield* new ContextRecoveryError({
            path,
            cause: new Error(`Context missing during storage recovery: ${path}`),
          });
        if (restored) {
          const record = yield* Effect.try({
            try: () => {
              const canonical = Schema.decodeUnknownSync(DurableContextSnapshot)(restored);
              return structuredClone({
                ...validate(canonical),
                ...(canonical.reactionEvents === undefined
                  ? {}
                  : { reactionEvents: canonical.reactionEvents }),
              });
            },
            catch: (cause) => new ContextRecoveryError({ path, cause }),
          });
          if ((record.revision ?? 0) < (previous?.revision ?? 0))
            return yield* new ContextRecoveryError({
              path,
              cause: new Error(`Context revision regressed during storage recovery: ${path}`),
            });
          if (failed) {
            records.set(path, record);
            if (!isDeepStrictEqual(previous, record))
              yield* PubSub.publish(changes, {
                path,
                created: previous === undefined,
                stateChanged:
                  previous === undefined || !isDeepStrictEqual(previous.state, record.state),
                record: structuredClone(record),
                evaluate: false,
              });
          }
        }
        failedCommits.delete(path);
      }).pipe(Effect.uninterruptible),
    );
  });
  return DurableContext.of({
    kind,
    commit,
    recover,
    get: (path) => {
      const record = records.get(path);
      return record === undefined ? undefined : structuredClone(record);
    },
    snapshot: () =>
      Object.fromEntries([...records].map(([path, record]) => [path, structuredClone(record)])),
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
