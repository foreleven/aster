import { IntegrationError } from "@aster/core";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Context, Deferred, Effect, Fiber, Layer, Schema } from "effect";
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
import { Models, MemoryIntegration, MemoryRuntime } from "@aster/integrations";

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

const infrastructure = (events: string[]) =>
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
      MemoryRuntime,
      Effect.acquireRelease(
        Effect.sync(() => {
          events.push("memory:acquired");
          return MemoryRuntime.of({
            config: { description: "Memory", dataDir: "/tmp", port: 3111, autoCompress: false },
            client: {
              capture: async (input) => {
                events.push(`capture:${input.sessionId}`);
              },
              search: async () => ({ mode: "compact", results: [] }),
              expand: async () => ({ mode: "expanded", results: [], truncated: false }),
              drain: async () => {
                events.push("memory:drained");
              },
              close: () => {},
            },
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
            MemoryIntegration.layer,
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
    integrations: [
      MemoryIntegration.layer,
      sourceLayer("one", events),
      sourceLayer("broken", events, Effect.void, true),
    ],
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
          integrations: [MemoryIntegration.layer, sourceLayer("pending", events, Effect.never)],
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
            yield* MemoryRuntime;
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
        const live = AsterRuntime.layer({ integrations: [MemoryIntegration.layer, pending] }).pipe(
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
