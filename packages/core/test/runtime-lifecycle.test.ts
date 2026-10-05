import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Models, PiStorageLease } from "@aster/agent";
import { Cause, ConfigProvider, Context, Effect, Exit, Layer } from "effect";
import {
  AsterRuntime,
  MemoryBackend,
  DurableContext,
  ExternalAgents,
  GoalHistoryStore,
  RuntimeIntegrations,
  SystemOneClient,
  defineIntegration,
  makeMemoryGoalHistory,
} from "../src/index.js";
import { makeDurableContext } from "../src/context/kernel.js";

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
    Layer.effect(
      DurableContext,
      makeDurableContext({ load: Effect.succeed([]), save: () => Effect.void }),
    ),
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
    Layer.succeed(MemoryBackend, {
      description: "Memory",
      retrieval: "bm25",
      capture: () => Effect.void,
      drain,
      recall: { search: () => Effect.succeed([]), expand: () => Effect.succeed([]) },
    }),
  );
const config = ConfigProvider.layer(
  ConfigProvider.fromUnknown({ config: { agent: { model: "test" } } }),
);

test("runtime readiness includes the Personal root and its durable command API", async () => {
  const live = AsterRuntime.layer({ integrations: [] }).pipe(
    Layer.provide(infrastructure()),
    Layer.provide(config),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const runtime = yield* AsterRuntime;
      yield* runtime.ready;
      const personal = yield* runtime.api.personal.get;
      assert.equal(personal.path, "/personal");
      assert.equal(personal.revision, 1);
      const request = {
        requestId: "runtime-input",
        causationId: "user",
        expectedRevision: 1,
        text: "Track my work",
      };
      const accepted = yield* runtime.api.personal.sendMessage(request);
      assert.equal(accepted.revision, 2);
      assert.deepEqual(yield* runtime.api.personal.sendMessage(request), accepted);
      assert.ok(
        (yield* runtime.api.inspect).actors.some((actor) => actor.path === "/user/personal"),
      );
    }).pipe(Effect.provide(live)),
  );
});

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

test("runtime inspection exposes current storage ownership without local filesystem paths", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-runtime-lease-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const live = AsterRuntime.layer({ integrations: [] }).pipe(
    Layer.provide(infrastructure()),
    Layer.provide(config),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const runtime = yield* AsterRuntime;
      yield* runtime.ready;
      const token = yield* Effect.scoped(
        Effect.gen(function* () {
          const lease = yield* PiStorageLease.acquire(directory, "goal:test");
          const view = (yield* runtime.api.inspect).storageOwners?.find(
            (owner) => owner.leaseId === lease.identity.token,
          );
          assert.equal(view?.ownerId, "goal:test");
          assert.equal(view?.status, "held");
          assert.equal(view?.pid, process.pid);
          assert.equal(JSON.stringify(view).includes(directory), false);
          return lease.identity.token;
        }),
      );
      assert.equal(
        (yield* runtime.api.inspect).storageOwners?.some((owner) => owner.leaseId === token),
        false,
      );
    }).pipe(Effect.provide(live)),
  );
});
