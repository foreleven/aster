import assert from "node:assert/strict";
import { test } from "node:test";
import { AgentConversations } from "@aster/agent";
import { ApplicationError } from "@aster/api-contracts";
import { Clock, Context, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect";
import { TestClock } from "effect/testing";
import {
  ContextRegistry,
  SignalActor,
  SignalSnapshot,
  SignalTime,
  type ContextInput,
} from "../src/index.js";
import { SignalState, nextSignalTime } from "../src/signals/state/model.js";
import { readSignalHistory, signalMessage } from "../src/signals/state/store.js";
import { type SignalChangeInput } from "../src/signals/protocol.js";
import { type SignalDefinition } from "../src/config/schema.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { testConversations } from "./conversation-fixtures.js";

const path = "/signals/personal--watch";
const definition = {
  trigger: { _tag: "Context" as const, when: "Evidence changes" },
  task: { _tag: "Goal" as const, target: "/goals/personal", text: "Original task" },
};
const create: SignalChangeInput = {
  requestId: "create",
  source: "/goals/personal",
  target: path,
  causal: { rootRequestId: "user", remainingAgentTurns: 3 },
  change: { operation: "create", definition },
};
const open = (
  registry: ContextRegistry["Service"],
  messages: AgentConversations["Service"],
  configured?: SignalDefinition,
) =>
  Layer.build(SignalState.layer(path, configured)).pipe(
    Effect.map((services) => Context.get(services, SignalState)),
    Effect.provideService(ContextRegistry, registry),
    Effect.provideService(AgentConversations, messages),
  );
const setup = Effect.fnUntraced(function* () {
  const messages = testConversations();
  const registry = yield* makeContextRegistry({
    loadAll: () => [
      {
        snapshot: {
          revision: 0,
          path: "/goals/personal",
          description: "Assistant",
          state: { status: "active" },
          messages: [],
        },
        events: [],
      },
    ],
    save: () => {},
  });
  yield* registry.register(path, SignalActor.context);
  const state = yield* open(registry, messages);
  yield* state.change(create);
  const react = (id: string) =>
    state.react({
      requestId: id,
      causationId: id,
      source: "/system-one",
      target: path,
      expectedRevision: registry.get(path)!.revision,
      sourceContext: {
        path: "/source",
        description: "Evidence",
        state: { text: id },
        messages: [],
      },
    });
  const change = Effect.fnUntraced(function* (operation: "pause" | "resume" | "delete") {
    return yield* state.change({
      ...create,
      requestId: operation,
      change: { operation, version: (yield* state.snapshot)!.version },
    });
  });
  return { messages, registry, state, react, change };
});
const run = <A, E>(effect: Effect.Effect<A, E, import("effect").Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(effect).pipe(Effect.timeout("5 seconds")));

test("Signal pause preserves frozen work; edits and resume do not rewrite it; deletion only cancels unstarted deliveries", async () => {
  await run(
    Effect.gen(function* () {
      const env = yield* setup();
      yield* env.react("one");
      yield* env.react("two");
      const [one, two] = yield* env.state.deliveries;
      assert.equal(yield* signalMessage(env.messages, path, one!.message.requestId), undefined);
      yield* env.state.beginDelivery(one!.message.requestId);
      yield* env.change("pause");
      assert.equal(yield* env.state.beginDelivery(two!.message.requestId), undefined);
      assert.equal((yield* env.state.deliveries)[1]!.status, "pending");
      yield* env.state.change({
        ...create,
        requestId: "edit",
        change: {
          operation: "update",
          version: 2,
          definition: { ...definition, task: { ...definition.task, text: "New task" } },
        },
      });
      yield* env.change("resume");
      assert.deepEqual(yield* env.state.beginDelivery(two!.message.requestId), two!.message);
      yield* env.react("three");
      yield* env.change("delete");
      assert.deepEqual(
        (yield* env.state.deliveries).map((item) => item.status),
        ["sending", "sending", "cancelled"],
      );
      assert.deepEqual(
        yield* signalMessage(env.messages, path, one!.message.requestId),
        one!.message,
      );
      yield* env.state.settleDelivery(
        one!.message.requestId,
        new ApplicationError({ kind: "unavailable", message: "Lost acknowledgement" }),
      );
      assert.equal((yield* env.state.deliveries)[0]!.status, "sending");
      yield* env.state.settleDelivery(one!.message.requestId);
      const restored = yield* open(env.registry, env.messages);
      assert.deepEqual(
        (yield* restored.deliveries).map((item) => item.status),
        ["delivered", "sending", "cancelled"],
      );
      assert.equal((yield* restored.snapshot)!.status, "deleted");
      assert.equal((yield* restored.change(create)).requestId, "create");
    }),
  );
});

test("nextDue requires an offset, normalizes one-shot times, and respects cron timezone across DST", () => {
  for (const invalid of [123, "2026-10-06T09:00:00", "tomorrow", "2026-10-06Z"])
    assert.equal(Schema.decodeUnknownResult(SignalTime)(invalid)._tag, "Failure");
  assert.equal(Schema.decodeUnknownResult(SignalTime)("2026-10-06T09:00:00+08:00")._tag, "Success");
  assert.equal(
    nextSignalTime(
      { _tag: "Schedule", schedule: { type: "once", at: "2026-10-06T09:00:00+08:00" } },
      0,
    ),
    "2026-10-06T01:00:00.000Z",
  );
  const trigger = {
    _tag: "Schedule" as const,
    schedule: { type: "cron" as const, expression: "0 9 * * *", timeZone: "America/New_York" },
  };
  assert.equal(
    nextSignalTime(trigger, Date.parse("2026-03-07T00:00:00Z")),
    "2026-03-07T14:00:00.000Z",
  );
  assert.equal(
    nextSignalTime(trigger, Date.parse("2026-03-08T00:00:00Z")),
    "2026-03-08T13:00:00.000Z",
  );
  assert.equal(
    Schema.decodeUnknownResult(SignalSnapshot)({
      ...definition,
      status: "active",
      version: 1,
      nextDue: null,
    })._tag,
    "Failure",
  );
});

test("one-shot firing and exhaustion recover atomically without triggering again", async () => {
  await run(
    Effect.gen(function* () {
      const clock = yield* TestClock.make();
      const messages = testConversations();
      const registry = yield* makeContextRegistry();
      yield* registry.register(path, SignalActor.context);
      const configured: SignalDefinition = {
        ...definition,
        slug: "personal--watch",
        trigger: { _tag: "Schedule", schedule: { type: "once", at: "1970-01-01T08:00:01+08:00" } },
      };
      const state = yield* open(registry, messages, configured).pipe(
        Effect.provideService(Clock.Clock, clock),
      );
      assert.equal((yield* state.snapshot)?.nextDue, "1970-01-01T00:00:01.000Z");
      yield* clock.adjust(1000);
      yield* state
        .tick(1, "1970-01-01T00:00:01.000Z")
        .pipe(Effect.provideService(Clock.Clock, clock));
      const restored = yield* open(registry, messages, configured).pipe(
        Effect.provideService(Clock.Clock, clock),
      );
      assert.equal((yield* restored.snapshot)?.nextDue, null);
      yield* restored
        .tick(1, "1970-01-01T00:00:01.000Z")
        .pipe(Effect.provideService(Clock.Clock, clock));
      assert.equal((yield* restored.deliveries).length, 1);
    }),
  );
});

test("Signal Pi commit survives failed Context projection; recovery restores state and the original receipt", async () => {
  await run(
    Effect.gen(function* () {
      const env = yield* setup();
      const state = yield* open(
        { ...env.registry, commit: () => Effect.die(new Error("Injected projection failure")) },
        env.messages,
      );
      const input: SignalChangeInput = {
        ...create,
        requestId: "pause",
        change: { operation: "pause", version: 1 },
      };
      assert.ok(Exit.hasDies(yield* state.change(input).pipe(Effect.exit)));
      assert.equal((yield* state.snapshot)?.status, "active");
      assert.equal((yield* readSignalHistory(env.messages, path)).snapshot?.status, "paused");
      const restored = yield* open(env.registry, env.messages);
      assert.equal((yield* restored.snapshot)?.status, "paused");
      assert.equal(
        Schema.decodeUnknownSync(SignalSnapshot)(env.registry.get(path)!.state).status,
        "paused",
      );
      assert.equal((yield* restored.change(input)).requestId, "pause");
      assert.equal((yield* readSignalHistory(env.messages, path)).sequence, 2);
    }),
  );
});

test("Signal commit drains to the Ref after durable projection despite interruption", async () => {
  await run(
    Effect.gen(function* () {
      const env = yield* setup();
      const stored = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const state = yield* open(
        {
          ...env.registry,
          commit: (record: ContextInput, options) =>
            env.registry
              .commit(record, options)
              .pipe(
                Effect.tap(() =>
                  Deferred.succeed(stored, undefined).pipe(Effect.andThen(Deferred.await(release))),
                ),
              ),
        },
        env.messages,
      );
      const saving = yield* state.pause.pipe(Effect.forkScoped);
      yield* Deferred.await(stored);
      assert.equal((yield* state.snapshot)?.status, "active");
      const interrupting = yield* Fiber.interrupt(saving).pipe(Effect.forkScoped);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(interrupting);
      assert.equal((yield* state.snapshot)?.status, "paused");
    }),
  );
});
