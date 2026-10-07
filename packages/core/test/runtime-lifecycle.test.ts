import { ContextRegistry } from "../src/context/registry.js";
import { submitGoal } from "./goal-command-fixtures.js";
import { testConversations } from "./conversation-fixtures.js";
import { AgentConversations, AgentRunner } from "@aster/agent";
import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Models, PiStorageLease } from "@aster/agent";
import { Cause, ConfigProvider, Context, Deferred, Effect, Exit, Fiber, Layer } from "effect";
import {
  AsterRuntime,
  MemoryBackend,
  DurableContext,
  ExternalAgents,
  RuntimeIntegrations,
  SystemOneClient,
  defineIntegration,
} from "../src/index.js";
import { makeDurableContext } from "../src/context/store.js";

const integration = (
  name: string,
  phase: "source" | "consumer",
  stop: Effect.Effect<void>,
  ready: Effect.Effect<void, Error> = Effect.void,
  runner?: AgentRunner["Service"],
) =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const modules = yield* RuntimeIntegrations;
      yield* modules.register(
        defineIntegration({
          name,
          phase,
          services: runner ? Context.make(AgentRunner, runner) : Context.empty(),
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
    Layer.sync(AgentConversations, testConversations),
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

test("runtime readiness includes the built-in Goal assistant and ordinary Goal command API", async () => {
  const live = AsterRuntime.layer({ integrations: [] }).pipe(
    Layer.provide(infrastructure()),
    Layer.provide(config),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const runtime = yield* AsterRuntime;
      yield* runtime.ready;
      const accepted = yield* submitGoal(
        runtime.actors,
        "personal",
        "Track my work",
        "runtime-input",
      );
      const personal = (yield* ContextRegistry).reader.get("/goals/personal")!;
      assert.equal(personal.path, "/goals/personal");
      assert.deepEqual(
        yield* submitGoal(runtime.actors, "personal", "Track my work", "runtime-input"),
        accepted,
      );
      const paths = (yield* runtime.inspect).actors.map((actor) => actor.path);
      assert.ok(paths.includes("/user/goals/personal"));
      assert.ok(!paths.includes("/user/personal") && !paths.includes("/user/notifications"));
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
      assert.equal((yield* runtime.inspect).phase, "failed");
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
          const view = (yield* runtime.inspect).storageOwners?.find(
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
        (yield* runtime.inspect).storageOwners?.some((owner) => owner.leaseId === token),
        false,
      );
    }).pipe(Effect.provide(live)),
  );
});

test("restored Goals admit input while integration readiness gates execution", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>();
        const called = yield* Deferred.make<void>();
        const live = AsterRuntime.layer({
          integrations: [
            integration(
              "pending",
              "source",
              Effect.void,
              Deferred.await(release),
              AgentRunner.make(() =>
                Deferred.succeed(called, undefined).pipe(Effect.as({ messages: [] })),
              ),
            ),
          ],
        }).pipe(Layer.provide(infrastructure()), Layer.provide(config));
        yield* Effect.gen(function* () {
          const runtime = yield* AsterRuntime;
          const readiness = yield* runtime.ready.pipe(Effect.forkScoped);
          yield* submitGoal(runtime.actors, "personal", "Hello", "before-ready");
          const goals = Object.values((yield* ContextRegistry).reader.snapshot()).filter((r) =>
            /^\/goals\/[^/]+$/.test(r.path),
          );
          assert.deepEqual(
            goals.map((goal) => goal.path),
            ["/goals/personal"],
          );
          assert.equal(yield* Deferred.isDone(called), false);
          assert.equal(readiness.pollUnsafe(), undefined);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(readiness);
          yield* Deferred.await(called);
        }).pipe(Effect.provide(live));
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("runtime becomes ready while a Goal restores; its mailbox resumes after restoration", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const reading = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const called = yield* Deferred.make<void>();
        const history = testConversations();
        const services = Context.make(AgentConversations, {
          ...history,
          read: (path) =>
            Deferred.succeed(reading, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(history.read(path)),
            ),
        }).pipe(
          Context.add(
            AgentRunner,
            AgentRunner.make(() =>
              Deferred.succeed(called, undefined).pipe(Effect.as({ messages: [] })),
            ),
          ),
        );
        const live = AsterRuntime.layer({
          integrations: [
            Layer.effectDiscard(
              RuntimeIntegrations.use((modules) =>
                modules.register(
                  defineIntegration({
                    name: "slow-goal-storage",
                    phase: "source",
                    services,
                    activate: () => Effect.succeed({ ready: Effect.void, stop: Effect.void }),
                  }),
                ),
              ),
            ),
          ],
        }).pipe(Layer.provide(infrastructure()), Layer.provide(config));
        yield* Effect.gen(function* () {
          const runtime = yield* AsterRuntime;
          yield* Deferred.await(reading);
          yield* runtime.ready;
          assert.equal(yield* Deferred.isDone(called), false);
          const sending = yield* submitGoal(
            runtime.actors,
            "personal",
            "Hello",
            "during-restore",
          ).pipe(Effect.forkScoped);
          yield* Effect.yieldNow;
          assert.equal(sending.pollUnsafe(), undefined);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(sending);
          yield* Deferred.await(called);
        }).pipe(Effect.provide(live));
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("a stopped core owner makes runtime health failed after readiness", async () => {
  const stopMemory = Deferred.makeUnsafe<Effect.Effect<void>>();
  const control = Layer.effectDiscard(
    Effect.gen(function* () {
      const modules = yield* RuntimeIntegrations;
      yield* modules.register(
        defineIntegration({
          name: "control",
          phase: "source",
          services: Context.empty(),
          activate: (system) =>
            Effect.gen(function* () {
              const memory = yield* system.select("/user/memory").resolve();
              yield* Deferred.succeed(stopMemory, system.stop(memory));
              return { ready: Effect.void, stop: Effect.void };
            }),
        }),
      );
    }),
  );
  const live = AsterRuntime.layer({ integrations: [control] }).pipe(
    Layer.provide(infrastructure()),
    Layer.provide(config),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const runtime = yield* AsterRuntime;
      yield* runtime.ready;
      yield* yield* Deferred.await(stopMemory);
      const snapshot = yield* runtime.inspect.pipe(
        Effect.repeat({ until: (snapshot) => snapshot.phase === "failed" }),
        Effect.timeout("2 seconds"),
      );
      assert.equal(snapshot.phase, "failed");
      assert.ok(
        snapshot.events.some(
          (event) => event._tag === "ActorStopped" && event.path === "/user/memory",
        ),
      );
    }).pipe(Effect.provide(live)),
  );
});
