import { type ContextCapture, MemoryCaptureError } from "@aster/core";
import {
  Clock,
  Cause,
  Effect,
  Exit,
  Fiber,
  FiberMap,
  Ref,
  Option,
  Scope,
  Semaphore,
  Schedule,
  Schema,
  Redacted,
} from "effect";
import { Observation, Observations, type MemoryConnection } from "./protocol.js";
import type { MemoryClient } from "./client.js";
import type { Provenance } from "./provenance.js";

export const makeCapture = Effect.fn("Memory.captureWorker")(function* (
  client: MemoryClient,
  provenance: Provenance,
  connection: MemoryConnection,
  processingTimeoutMs: number = 120000,
) {
  const workScope = yield* Scope.make();
  const fibers = yield* FiberMap.make<string, void, MemoryCaptureError>().pipe(
    Scope.provide(workScope),
  );
  const closed = yield* Ref.make(false);
  // FiberMap removes completed workers; retain defects after their observer leaves.
  const defect = yield* Ref.make<Option.Option<Cause.Cause<MemoryCaptureError>>>(Option.none());
  const admission = yield* Semaphore.make(1);
  const drain = Effect.gen(function* () {
    yield* Effect.forEach([...fibers], ([, fiber]) => Fiber.await(fiber), { discard: true });
    const retained = yield* Ref.get(defect);
    if (Option.isSome(retained)) return yield* Effect.failCause(retained.value).pipe(Effect.orDie);
  });
  yield* Effect.addFinalizer(() =>
    admission
      .withPermit(Ref.set(closed, true))
      .pipe(Effect.andThen(drain), Effect.ensuring(Scope.close(workScope, Exit.void))),
  );
  const perform = Effect.fn("Memory.capture")(
    function* (input: ContextCapture) {
      const { sessionId, records } = input;
      const identity = { sessionId, project: connection.project, cwd: connection.cwd };
      yield* client.request("session/start", {
        ...identity,
        title: records.map((record) => record.description).join("; "),
      });
      const ids = new Set<string>();
      for (const record of records) {
        const now = yield* Clock.currentTimeMillis;
        const observed = yield* client
          .request("observe", {
            ...identity,
            timestamp: new Date(now).toISOString(),
            hookType: "post_tool_use",
            data: {
              tool_name: `Context: ${record.description}`,
              tool_input: { path: record.path },
              tool_output: JSON.stringify(record),
            },
          })
          .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Observation)));
        ids.add(observed.observationId);
        yield* provenance.insert(sessionId, observed.observationId, [record.path]);
      }
      const poll = client.request(`observations?sessionId=${encodeURIComponent(sessionId)}`).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Observations)),
        Effect.map((response) => {
          for (const observed of response.observations) if (observed.title) ids.delete(observed.id);
          return ids.size === 0;
        }),
      );
      if (ids.size)
        yield* poll.pipe(
          Effect.repeat({ schedule: Schedule.spaced(100), until: (done) => done }),
          Effect.timeout(Math.max(1, processingTimeoutMs)),
        );
      yield* client.request("session/end", { sessionId });
    },
    Effect.mapError(
      (cause) =>
        new MemoryCaptureError({ message: "Memory capture failed", cause: Redacted.make(cause) }),
    ),
    Effect.onExit((exit) =>
      Exit.isFailure(exit) && Cause.hasDies(exit.cause)
        ? Ref.update(defect, (current) =>
            Option.isSome(current) ? current : Option.some(exit.cause),
          )
        : Effect.void,
    ),
  );
  return {
    capture: (input: ContextCapture) =>
      admission
        .withPermit(
          Effect.gen(function* () {
            if (yield* Ref.get(closed))
              return yield* new MemoryCaptureError({
                message: "Memory capture admission is closed",
              });
            const current = yield* FiberMap.get(fibers, input.sessionId);
            if (Option.isSome(current)) return current.value;
            const snapshot = structuredClone(input);
            return yield* FiberMap.run(fibers, input.sessionId, perform(snapshot), {
              onlyIfMissing: true,
            });
          }),
        )
        .pipe(Effect.flatMap(Fiber.join)),
    drain,
  };
});
