import { TestClock } from "effect/testing";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, ActorTestKit } from "@aster/actor";
import { Clock, Effect, Layer, Schema, Stream } from "effect";
import {
  defineContext,
  ContextRegistry,
  ExternalAgents,
  SignalDefinitions,
  SignalRootActor,
  type ContextRecord,
  type GoalCommand,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import type { SignalCommandReply } from "../src/signals/actors.js";
import type { GoalSignalInput } from "../src/signals/goal-command.js";
import { preparationLayer } from "./fixtures.js";
const input: GoalSignalInput = {
  requestId: "create",
  source: "/goals/release",
  target: "/signals/release--watch",
  change: {
    operation: "create",
    definition: {
      when: "Blockers change",
      task: "Notify the Goal",
      schedule: { type: "once", at: "2099-01-01T00:00:00Z" },
    },
  },
  causal: { rootRequestId: "user", remainingAgentTurns: 3 },
};
const setup = Effect.fnUntraced(function* (
  records: Map<string, ContextRecord>,
  clock?: Clock.Clock,
) {
  const registry = yield* makeContextRegistry({
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.path, structuredClone(record));
    },
  });
  yield* registry.register(
    input.source,
    defineContext({
      state: Schema.Record(Schema.String, Schema.Unknown),
      message: Schema.Unknown,
    }),
  );
  if (!registry.get(input.source))
    yield* registry.commit(
      { path: input.source, description: "Goal", state: { status: "active" }, messages: [] },
      { expectedRevision: 0 },
    );
  const system = yield* ActorSystem.make().pipe(
    ActorSystem.provide(
      Layer.succeed(Clock.Clock, clock ?? (yield* Clock.Clock)),
      Layer.succeed(ContextRegistry, registry),
      Layer.succeed(SignalDefinitions, []),
      Layer.succeed(ExternalAgents, {}),
      preparationLayer,
    ),
  );
  const root = yield* system.spawn("signals", SignalRootActor);
  const subscriber = yield* ActorTestKit.probe<GoalCommand>();
  const command = (value: GoalSignalInput) =>
    root.ask<SignalCommandReply>((replyTo) => ({
      _tag: "ApplyGoalCommand",
      input: value,
      subscriber: subscriber.ref,
      replyTo,
    }));
  const wait = (predicate: () => boolean) =>
    Effect.scoped(
      Effect.gen(function* () {
        const changes = yield* registry.subscribe;
        if (!predicate())
          yield* changes.pipe(Stream.filter(predicate), Stream.take(1), Stream.runDrain);
      }),
    );
  return { registry, command, subscriber, wait };
});
test("Signal owns direct command receipts across restarts, rejects stale changes and preserves ownership", async () => {
  const records = new Map<string, ContextRecord>();
  for (const restart of [false, true])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* setup(records);
          assert.deepEqual(yield* env.command(input), {
            _tag: "Accepted",
            receipt: { requestId: "create", revision: 1 },
          });
          const before = env.registry.get(input.target)!;
          assert.equal(
            (yield* env.command({
              ...input,
              change: { operation: "create", definition: { task: "Different" } },
            }))._tag,
            "Rejected",
          );
          assert.deepEqual(env.registry.get(input.target), before);
          if (!restart) return;
          const update: GoalSignalInput = {
            ...input,
            requestId: "update",
            change: {
              operation: "update",
              revision: 1,
              definition: { schedule: null, when: "New condition" },
            },
          };
          assert.equal((yield* env.command(update))._tag, "Accepted");
          assert.equal(
            (env.registry.get(input.target)!.state as { schedule?: unknown }).schedule,
            undefined,
          );
          assert.equal((yield* env.command({ ...update, requestId: "stale" }))._tag, "Rejected");
          assert.equal((yield* env.command({ ...input, source: "/goals/other" }))._tag, "Rejected");
          assert.deepEqual(yield* env.command(input), {
            _tag: "Accepted",
            receipt: { requestId: "create", revision: 1 },
          });
          assert.equal(
            (yield* env.command({
              ...input,
              requestId: "delete",
              change: { operation: "delete", revision: 2 },
            }))._tag,
            "Accepted",
          );
          assert.equal(
            (env.registry.get(input.target)!.state as { deleted: boolean }).deleted,
            true,
          );
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
});

test("Signal timer lives outside conversation, reschedules by revision and sends evidence without executing Tasks", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        const start = Date.parse("2026-10-01T00:00:00Z");
        yield* clock.adjust(start);
        const env = yield* setup(new Map(), clock);
        yield* env.command({
          ...input,
          change: {
            operation: "create",
            definition: { schedule: { type: "once", at: new Date(start + 10000).toISOString() } },
          },
        });
        yield* env.wait(
          () =>
            (env.registry.get(input.target)?.state as { nextDue?: number }).nextDue ===
            start + 10000,
        );
        yield* env.command({
          ...input,
          requestId: "reschedule",
          change: {
            operation: "update",
            revision: 1,
            definition: { schedule: { type: "once", at: new Date(start + 20000).toISOString() } },
          },
        });
        yield* clock.adjust(11000);
        assert.equal(
          (env.registry.get(input.target)!.state as { occurrences?: unknown[] }).occurrences
            ?.length ?? 0,
          0,
        );
        yield* clock.adjust(10000);
        const occurrence = yield* env.subscriber.take();
        assert.equal(occurrence._tag, "SubmitInput");
        if (occurrence._tag !== "SubmitInput") return;
        assert.equal(occurrence.input._tag, "SignalOccurrence");
        yield* occurrence.replyTo.tell({
          _tag: "Accepted",
          receipt: { requestId: occurrence.requestId, revision: 1 },
        });
        yield* env.wait(
          () =>
            (env.registry.get(input.target)!.state as { occurrences: { delivered: boolean }[] })
              .occurrences[0]?.delivered === true,
        );
        assert.equal(
          Object.keys(env.registry.snapshot()).some((path) => path.includes("/runs/")),
          false,
        );
        yield* clock.adjust(60000);
        assert.equal(
          (env.registry.get(input.target)!.state as { occurrences: unknown[] }).occurrences.length,
          1,
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
