import {
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Queue,
  SynchronizedRef,
} from "effect";
import { LarkSettings } from "../../settings.js";
import { RuntimeConfigurationError } from "@aster/core";
import { ChatSummaryError } from "./errors.js";

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
export const makeSummaryAdmission = Effect.fn("SummaryAdmission.make")(function* (settings: {
  startIntervalMs: number;
  concurrency: number;
}) {
  if (
    !Number.isSafeInteger(settings.startIntervalMs) ||
    settings.startIntervalMs <= 0 ||
    !Number.isSafeInteger(settings.concurrency) ||
    settings.concurrency <= 0
  )
    return yield* new ChatSummaryError({ message: "Invalid IM Agent admission limits" });
  const scope = yield* Effect.scope;
  const requests = yield* Effect.acquireRelease(Queue.unbounded<Request>(), Queue.shutdown);
  const lastStart = yield* SynchronizedRef.make<number | undefined>(undefined);
  // FIFO dequeue and minimum start spacing share one lock. Capacity is released on completion.
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
  const run: AgentAdmission["run"] = (chat, execute) =>
    Effect.gen(function* () {
      const work = Effect.acquireUseRelease(
        Effect.gen(function* () {
          return {
            start: yield* Deferred.make<void, ChatSummaryError>(),
            finished: yield* Deferred.make<void>(),
          };
        }),
        (request) =>
          Queue.offer(requests, request).pipe(
            Effect.andThen(Deferred.await(request.start)),
            Effect.andThen(execute.pipe(Effect.annotateLogs({ chatId: chat }))),
          ),
        (request) => Deferred.succeed(request.finished, undefined),
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

export class SummaryAdmission extends Context.Service<SummaryAdmission, AgentAdmission>()(
  "lark/SummaryAdmission",
) {
  static readonly layer = Layer.effect(
    SummaryAdmission,
    Effect.gen(function* () {
      const config = yield* LarkSettings;
      if (!config.im)
        return yield* new RuntimeConfigurationError({ message: "Lark IM is not configured" });
      const settings = config.im.config.summary;
      return yield* makeSummaryAdmission({
        startIntervalMs: settings.agentStartIntervalMs,
        concurrency: settings.agentConcurrency,
      }).pipe(
        Effect.catchTag("ChatSummaryError", (error) =>
          Effect.fail(new RuntimeConfigurationError({ message: error.message })),
        ),
      );
    }),
  );
}
