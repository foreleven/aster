import {
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  HashSet,
  Layer,
  Queue,
  Ref,
  SynchronizedRef,
  Schema,
} from "effect";
import { LarkConfig } from "../../config.js";
import { ContextSession, validateConfig } from "@aster/core";
import { AgentAdmissionConfig } from "../config.js";
import { ChatSummaryError } from "../../shared/errors.js";

export interface AgentAdmission {
  readonly run: <A, E, R>(
    chat: string,
    execute: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ChatSummaryError, R>;
}

interface Request {
  readonly start: Deferred.Deferred<void, ChatSummaryError>;
  readonly finished: Deferred.Deferred<void>;
}

/** One integration-scoped FIFO and worker pool serve all Chat Actors. */
export const makeImAgentQueue = Effect.fn("ImAgentQueue.make")(function* (
  settings: { startIntervalMs: number; concurrency: number },
  checkpoint: {
    load: () => Effect.Effect<number | undefined, ChatSummaryError>;
    save: (at: number) => Effect.Effect<void, ChatSummaryError>;
  },
) {
  if (
    !Number.isSafeInteger(settings.startIntervalMs) ||
    settings.startIntervalMs <= 0 ||
    !Number.isSafeInteger(settings.concurrency) ||
    settings.concurrency <= 0
  )
    return yield* new ChatSummaryError({ message: "Invalid IM Agent admission limits" });
  const restoredStart = yield* checkpoint.load();
  if (restoredStart !== undefined && (!Number.isFinite(restoredStart) || restoredStart < 0))
    return yield* new ChatSummaryError({ message: "Invalid persisted Agent start time" });
  const scope = yield* Effect.scope;
  const requests = yield* Effect.acquireRelease(Queue.unbounded<Request>(), Queue.shutdown);
  const lastStart = yield* SynchronizedRef.make(restoredStart);
  const keys = yield* Ref.make(HashSet.empty<string>());
  // Taking the next chat and persisting its start share one lock, preserving global FIFO.
  // Workers release this lock before waiting for model execution to finish.
  const take = SynchronizedRef.modifyEffect(
    lastStart,
    Effect.fnUntraced(function* (previous) {
      const request = yield* Queue.take(requests);
      if (yield* Deferred.isDone(request.finished)) return [request, previous] as const;
      const admission = yield* Effect.raceFirst(
        Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          if (previous !== undefined)
            yield* Effect.sleep(Math.max(0, previous + settings.startIntervalMs - now));
          const at = yield* Clock.currentTimeMillis;
          // Persist before releasing the caller so a restart cannot bypass spacing.
          yield* checkpoint.save(at);
          return at;
        }).pipe(Effect.exit),
        Deferred.await(request.finished).pipe(Effect.as(undefined)),
      );
      if (admission === undefined) return [request, previous] as const;
      // Propagate failures and defects to the requesting Actor, without retiring a worker.
      yield* Deferred.done(
        request.start,
        Exit.map(admission, () => undefined),
      );
      return [request, Exit.isSuccess(admission) ? admission.value : previous] as const;
    }),
  );
  const worker = take.pipe(
    Effect.flatMap((request) => Deferred.await(request.finished)),
    Effect.forever,
  );
  for (let index = 0; index < settings.concurrency; index++) yield* Effect.forkIn(worker, scope);
  const run: AgentAdmission["run"] = (key, execute) =>
    Effect.gen(function* () {
      const work = Effect.acquireUseRelease(
        Effect.gen(function* () {
          const claimed = yield* Ref.modify(keys, (current) =>
            HashSet.has(current, key) ? [false, current] : [true, HashSet.add(current, key)],
          );
          if (!claimed)
            return yield* new ChatSummaryError({
              message: `Chat already has an Agent request: ${key}`,
            });
          return {
            start: yield* Deferred.make<void, ChatSummaryError>(),
            finished: yield* Deferred.make<void>(),
          };
        }),
        (request) =>
          Queue.offer(requests, request).pipe(
            Effect.andThen(Deferred.await(request.start)),
            Effect.andThen(execute),
          ),
        (request) =>
          Ref.update(keys, HashSet.remove(key)).pipe(
            Effect.andThen(Deferred.succeed(request.finished, undefined)),
          ),
      );
      // Register caller cleanup atomically with the fork, including cancellation before join starts.
      return yield* Effect.acquireUseRelease(
        Effect.forkIn(Effect.interruptible(work), scope),
        Fiber.join,
        Fiber.interrupt,
      );
    });
  return { run } satisfies AgentAdmission;
});

export class ImAgentQueue extends Context.Service<ImAgentQueue, AgentAdmission>()(
  "lark/ImAgentQueue",
) {
  static readonly layer = Layer.effect(
    ImAgentQueue,
    Effect.gen(function* () {
      const config = yield* LarkConfig;
      const settings = yield* validateConfig("Lark IM Agent admission", () =>
        Schema.decodeUnknownSync(AgentAdmissionConfig)(config.im?.config?.summary ?? {}),
      );
      const session = yield* ContextSession.make({
        path: "/lark/im/admission",
        state: Schema.Struct({ lastStart: Schema.optional(Schema.Number) }),
        message: Schema.Never,
        initial: { state: {}, messages: [], description: "Private IM Agent admission timing" },
      }).pipe(Effect.orDie);
      return yield* makeImAgentQueue(
        { startIntervalMs: settings.agentStartIntervalMs, concurrency: settings.agentConcurrency },
        {
          load: () =>
            session.state.get.pipe(
              Effect.map((state) => state.lastStart),
              Effect.orDie,
            ),
          save: (at) =>
            session.state
              .update(() => ({ lastStart: at }), { mode: "bootstrap" })
              .pipe(Effect.asVoid, Effect.orDie),
        },
      );
    }),
  );
}
