import assert from "node:assert/strict";
import { test } from "node:test";
import { Models } from "@aster/agent";
import { Cause, ConfigProvider, Context, Effect, Exit, Layer } from "effect";
import {
  AsterRuntime,
  ContextCaptureSink,
  ContextStore,
  ExternalAgents,
  GoalHistoryStore,
  MemoryRecall,
  RuntimeIntegrations,
  SystemOneClient,
  defineIntegration,
  makeMemoryGoalHistory,
} from "../src/index.js";

const integration = (
  name: string,
  phase: "source" | "consumer",
  stop: Effect.Effect<void>,
  ready: Effect.Effect<void, Error> = Effect.void,
) =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const modules = yield* RuntimeIntegrations;
      yield* modules.register(
        defineIntegration({
          name,
          phase,
          services: Context.empty(),
          activate: () => Effect.succeed({ stop, ready }),
        }),
      );
    }),
  );

const infrastructure = (drain = Effect.void) =>
  Layer.mergeAll(
    Layer.succeed(ContextStore, { loadAll: () => [], save: () => {} }),
    Layer.sync(GoalHistoryStore, makeMemoryGoalHistory),
    Models.layer([
      {
        name: "test",
        provider: "openai",
        model: "test",
        url: "http://unused.invalid",
        apiKey: "test",
      },
    ]),
    Layer.succeed(SystemOneClient, {
      configured: false,
      systemOne: () => Effect.die(new Error("No model calls expected")),
    }),
    Layer.succeed(ExternalAgents, {}),
    Layer.succeed(MemoryRecall, {
      search: () => Effect.sync(() => []),
      expand: () => Effect.sync(() => []),
    }),
    Layer.succeed(ContextCaptureSink, { capture: () => Effect.void, drain }),
  );
const config = ConfigProvider.layer(
  ConfigProvider.fromUnknown({ config: { agent: { model: "test" } } }),
);

test("a readiness defect settles runtime.ready with its original cause", async () => {
  const defect = new Error("source readiness defect");
  const live = AsterRuntime.layer({
    integrations: [integration("source", "source", Effect.void, Effect.die(defect))],
  }).pipe(Layer.provide(infrastructure()), Layer.provide(config));
  await Effect.runPromise(
    Effect.gen(function* () {
      const runtime = yield* AsterRuntime;
      const exit = yield* Effect.exit(runtime.ready.pipe(Effect.timeout("200 millis")));
      assert.ok(Exit.isFailure(exit));
      assert.equal(Cause.squash(exit.cause), defect);
      assert.equal(((yield* runtime.api.dashboard).runtime as { phase: string }).phase, "failed");
    }).pipe(Effect.provide(live)),
  );
});

test("shutdown attempts every phase even when multiple integration stops defect", async () => {
  const events: string[] = [];
  const step = (name: string, fail = false) =>
    Effect.sync(() => {
      events.push(name);
      if (fail) throw new Error(name);
    });
  const live = AsterRuntime.layer({
    integrations: [
      integration("consumer", "consumer", step("consumer:stop", true)),
      integration("first", "source", step("first:stop")),
      integration("second", "source", step("second:stop", true)),
    ],
  }).pipe(Layer.provide(infrastructure(step("memory:drain"))), Layer.provide(config));
  const exit = await Effect.runPromiseExit(
    Effect.gen(function* () {
      yield* (yield* AsterRuntime).ready;
    }).pipe(Effect.provide(live)),
  );
  assert.ok(Exit.isFailure(exit));
  assert.deepEqual(events, ["second:stop", "first:stop", "consumer:stop", "memory:drain"]);
  const cause = Cause.pretty(exit.cause);
  assert.match(cause, /second:stop/);
  assert.match(cause, /consumer:stop/);
});

test("closing a runtime before readiness also settles later readiness callers", async () => {
  const live = AsterRuntime.layer({
    integrations: [integration("pending", "source", Effect.void, Effect.never)],
  }).pipe(Layer.provide(infrastructure()), Layer.provide(config));
  const runtime = await Effect.runPromise(AsterRuntime.pipe(Effect.provide(live)));
  const exit = await Effect.runPromiseExit(runtime.ready.pipe(Effect.timeout("200 millis")));
  assert.ok(Exit.isFailure(exit));
  assert.ok(Cause.hasInterruptsOnly(exit.cause));
});
