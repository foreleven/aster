import { IntegrationError } from "@aster/core";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Context, Deferred, Effect, Fiber, Layer, Redacted, Schema } from "effect";
import {
  AsterRuntime,
  ConfigLocation,
  ContextActor,
  ContextRegistry,
  DurableContext,
  LocalDurableContext,
  GoalHistoryStore,
  SystemOneClient,
  ExternalAgents,
  RuntimeIntegrations,
  defineContext,
  contextView,
  defineIntegration,
  contextPath,
  makeMemoryGoalHistory,
} from "@aster/core";
import { Models } from "@aster/agent";
import { MailFetcher, MailIntegration, MailSettings } from "@aster/integrations";
import { MemoryBackend } from "@aster/core";

class Source extends ContextActor.Service<Source>()("test/Source", {
  command: Schema.TaggedStruct("Ping", {}),
  context: defineContext({
    identity: "source",
    view: contextView({ state: Schema.Struct({ value: Schema.Number }) }),
    state: Schema.Struct({ value: Schema.Number }),
    message: Schema.Never,
    capture: (record) => ({ sessionId: record.path, records: [record] }),
  }),
}) {
  static readonly layer = Layer.effect(
    Source,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      return Source.of({
        started: (actor) =>
          registry.commit(
            {
              path: contextPath(actor),
              description: "Source",
              state: { value: 1 },
              messages: [],
            },
            { expectedRevision: registry.get(contextPath(actor))?.revision ?? 0 },
          ),
        receive: () => Effect.void,
      });
    }),
  );
}

const sourceLayer = (
  name: string,
  events: string[],
  ready: Effect.Effect<void, Error> = Effect.void,
  fail = false,
) =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const modules = yield* RuntimeIntegrations;
      const registry = yield* ContextRegistry;
      yield* modules.register(
        defineIntegration({
          name,
          phase: "source",
          services: Context.make(ContextRegistry, registry),
          activate: (system) =>
            Effect.gen(function* () {
              events.push(`start:${name}`);
              if (fail)
                return yield* Effect.fail(
                  new IntegrationError({
                    integration: name,
                    message: "injected activation failure",
                  }),
                );
              const actor = yield* system.spawn(name, Source);
              return {
                ready,
                stop: system.stop(actor).pipe(
                  Effect.andThen(
                    Effect.sync(() => {
                      events.push(`stop:${name}`);
                    }),
                  ),
                ),
              };
            }),
        }),
      );
    }),
  );

const infrastructure = (events: string[], overrides: Partial<MemoryBackend["Service"]> = {}) =>
  Layer.mergeAll(
    Layer.succeed(ConfigLocation, { baseDir: "/tmp", projectRoot: "/tmp", envPath: "/tmp/.env" }),
    Layer.effect(DurableContext, LocalDurableContext.fromStore()),
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
      systemOne: () =>
        Effect.sync(() => {
          throw new Error("No model calls expected");
        }),
    }),
    Layer.succeed(ExternalAgents, {}),
    Layer.effect(
      MemoryBackend,
      Effect.acquireRelease(
        Effect.sync(() => {
          events.push("memory:acquired");
          return MemoryBackend.of({
            description: "Memory",
            retrieval: "bm25",
            capture: (input) =>
              Effect.sync(() => {
                events.push(`capture:${input.sessionId}`);
              }),
            recall: { search: () => Effect.succeed([]), expand: () => Effect.succeed([]) },
            drain: Effect.sync(() => {
              events.push("memory:drained");
            }),
            ...overrides,
          });
        }),
        () =>
          Effect.sync(() => {
            events.push("memory:released");
          }),
      ),
    ),
  );
const config = ConfigProvider.layer(
  ConfigProvider.fromUnknown({ config: { agent: { model: "test" } } }),
);

test("runtime installs multiple sources, captures first changes, exposes API before readiness and releases services last", async () => {
  const events: string[] = [];
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const ready = yield* Deferred.make<void>();
        const live = AsterRuntime.layer({
          integrations: [
            sourceLayer("one", events, Deferred.await(ready)),
            sourceLayer("two", events),
          ],
        }).pipe(Layer.provide(infrastructure(events)), Layer.provide(config));
        yield* Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* AsterRuntime;
            assert.equal(
              (yield* runtime.api.dashboard).runtime && events.includes("start:two"),
              true,
            );
            assert.equal((yield* runtime.api.inspect).phase, "starting");
            const pending = yield* runtime.ready.pipe(Effect.forkScoped);
            yield* Deferred.succeed(ready, undefined);
            yield* Fiber.join(pending);
            assert.equal((yield* runtime.api.inspect).phase, "ready");
            yield* Effect.gen(function* () {
              while (!events.includes("capture:/one") || !events.includes("capture:/two"))
                yield* Effect.sleep(1);
            }).pipe(Effect.timeout("2 seconds"));
            assert.deepEqual((yield* runtime.api.context("/one")).state, { value: 1 });
          }).pipe(Effect.provide(live)),
        );
      }),
    ),
  );
  assert.equal(events.filter((event) => event === "memory:acquired").length, 1);
  assert.ok(events.indexOf("stop:one") < events.indexOf("memory:drained"));
  assert.ok(events.indexOf("stop:two") < events.indexOf("memory:drained"));
  assert.equal(events.at(-1), "memory:released");
});

test("activation failure stops earlier integrations and releases the shared backend", async () => {
  const events: string[] = [];
  const live = AsterRuntime.layer({
    integrations: [sourceLayer("one", events), sourceLayer("broken", events, Effect.void, true)],
  }).pipe(Layer.provide(infrastructure(events)), Layer.provide(config));
  await assert.rejects(
    Effect.runPromise(Effect.scoped(AsterRuntime.pipe(Effect.provide(live)))),
    /injected activation failure/,
  );
  assert.ok(events.includes("stop:one"));
  assert.equal(events.at(-1), "memory:released");
});

test("cancelling while a source is not ready closes the runtime without waiting for readiness", async () => {
  const events: string[] = [];
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const acquired = yield* Deferred.make<void>();
        const live = AsterRuntime.layer({
          integrations: [sourceLayer("pending", events, Effect.never)],
        }).pipe(Layer.provide(infrastructure(events)), Layer.provide(config));
        const fiber = yield* Effect.gen(function* () {
          const runtime = yield* AsterRuntime;
          yield* Deferred.succeed(acquired, undefined);
          yield* runtime.ready;
        }).pipe(Effect.provide(live), Effect.forkScoped);
        yield* Deferred.await(acquired);
        yield* Fiber.interrupt(fiber).pipe(Effect.timeout("2 seconds"));
      }),
    ),
  );
  assert.ok(events.includes("stop:pending"));
  assert.equal(events.at(-1), "memory:released");
});

test("cancelling integration Layer acquisition releases resources before runtime activation", async () => {
  const events: string[] = [];
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const acquiring = yield* Deferred.make<void>();
        const pending = Layer.effectDiscard(
          Effect.gen(function* () {
            yield* MemoryBackend;
            yield* Effect.acquireRelease(
              Effect.sync(() => {
                events.push("module:acquired");
              }),
              () =>
                Effect.sync(() => {
                  events.push("module:released");
                }),
            );
            yield* Deferred.succeed(acquiring, undefined);
            yield* Effect.never;
          }),
        );
        const live = AsterRuntime.layer({ integrations: [pending] }).pipe(
          Layer.provide(infrastructure(events)),
          Layer.provide(config),
        );
        const fiber = yield* AsterRuntime.pipe(Effect.provide(live), Effect.forkScoped);
        yield* Deferred.await(acquiring);
        yield* Fiber.interrupt(fiber).pipe(Effect.timeout("2 seconds"));
      }),
    ),
  );
  assert.deepEqual(events, [
    "memory:acquired",
    "module:acquired",
    "module:released",
    "memory:released",
  ]);
});

test("runtime stops capture observers before draining admitted work and releases the backend last", async () => {
  const events: string[] = [];
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const draining = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const live = AsterRuntime.layer({ integrations: [sourceLayer("one", events)] }).pipe(
          Layer.provide(
            infrastructure(events, {
              capture: () =>
                Deferred.succeed(started, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.ensuring(
                    Effect.sync(() => {
                      events.push("capture:stopped");
                    }),
                  ),
                ),
              drain: Effect.gen(function* () {
                events.push("memory:draining");
                yield* Deferred.succeed(draining, undefined);
                yield* Deferred.await(release);
                events.push("memory:drained");
              }),
            }),
          ),
          Layer.provide(config),
        );
        const running = yield* Effect.gen(function* () {
          yield* (yield* AsterRuntime).ready;
          yield* Effect.never;
        }).pipe(Effect.provide(live), Effect.forkScoped);
        yield* Deferred.await(started);
        // Unblock finalization even if an assertion fails.
        yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined));
        const stopping = yield* Fiber.interrupt(running).pipe(Effect.forkScoped);
        yield* Deferred.await(draining);
        assert.ok(events.indexOf("stop:one") < events.indexOf("capture:stopped"));
        assert.ok(events.indexOf("capture:stopped") < events.indexOf("memory:draining"));
        assert.equal(events.includes("memory:released"), false);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(stopping);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
  assert.equal(events.at(-1), "memory:released");
  assert.ok(events.indexOf("memory:drained") < events.indexOf("memory:released"));
});

test("runtime activates generic mail and exposes its tree through ListContexts before retrieval finishes", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const mail = MailIntegration.installation.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.succeed(MailSettings, {
                mailboxes: [
                  {
                    id: "work",
                    host: "unused.invalid",
                    username: "test",
                    password: Redacted.make("secret"),
                  },
                ],
              }),
              Layer.succeed(MailFetcher, {
                pull: () =>
                  Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                    Effect.as([]),
                  ),
                pullAll: () => Effect.die("Unexpected batch pull"),
              }),
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const runtime = yield* AsterRuntime;
          yield* Deferred.await(entered);
          const contexts = yield* runtime.api.contexts;
          assert.ok(contexts.some((context) => context.path === "/mail"));
          assert.ok(contexts.some((context) => context.path === "/mail/work"));
          assert.ok(!JSON.stringify(contexts).includes("secret"));
          assert.equal((yield* runtime.api.inspect).phase, "starting");
          yield* Deferred.succeed(release, undefined);
          yield* runtime.ready;
          assert.equal((yield* runtime.api.inspect).phase, "ready");
        }).pipe(
          Effect.provide(
            AsterRuntime.layer({ integrations: [mail] }).pipe(
              Layer.provide(infrastructure([])),
              Layer.provide(config),
            ),
          ),
        );
      }),
    ).pipe(Effect.timeout("10 seconds")),
  );
});

test("runtime owns Apps activation and exposes query commands after readiness", async () => {
  const { AppsIntegration, AppsSettings, OpenCli } = await import("@aster/integrations");
  let calls = 0;
  const apps = AppsIntegration.installation.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(AppsSettings, {
          description: "Apps",
          apps: [{ name: "ctrip", description: "Travel queries" }],
        }),
        Layer.succeed(OpenCli, {
          run: () =>
            Effect.sync(() => {
              calls++;
              return [{ city: "Sanya" }];
            }),
        }),
      ),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* AsterRuntime;
        yield* runtime.ready;
        assert.equal(calls, 0);
        assert.ok((yield* runtime.api.contexts).some((record) => record.path === "/apps/ctrip"));
        const result = yield* runtime.api.queryContext({
          path: "/apps/ctrip",
          command: "search",
          args: { query: "Sanya" },
        });
        assert.deepEqual(result.data, [{ city: "Sanya" }]);
        assert.equal(calls, 1);
      }),
    ).pipe(
      Effect.provide(
        AsterRuntime.layer({ integrations: [apps] }).pipe(Layer.provide(infrastructure([]))),
      ),
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        ConfigProvider.fromUnknown({ config: { agent: { model: "test" } } }),
      ),
    ),
  );
});
