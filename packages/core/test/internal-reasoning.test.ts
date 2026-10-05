import { AgentConversations } from "@aster/agent";
import { testConversations } from "./conversation-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { AgentRunner, Agent, AgentError, Models } from "@aster/agent";
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Layer } from "effect";
import { TestClock } from "effect/testing";
import { makeDescriptionInitializer, makeStructuredReasoning } from "../src/index.js";

const identity = { path: "/test", identity: "Test", parentDescription: "Parent" };
const models = Layer.succeed(Models, {
  resolve: () => Effect.die(new Error("Agent.make is mocked")),
});

test("description run lazily with the caller Clock and reject malformed output as tagged failures", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(54321);
        let called = 0;
        let raw: unknown = {
          description: "  Fixed identity  ",
          triggeredSignalIds: ["test", "test", "unknown", 7],
        };
        const run = () =>
          Effect.gen(function* () {
            called++;
            assert.equal(yield* Clock.currentTimeMillis, 54321);
            return raw;
          });
        const describe = makeDescriptionInitializer(run);
        const description = describe(identity);
        assert.equal(called, 0);
        assert.equal(
          yield* description.pipe(Effect.provideService(Clock.Clock, clock)),
          "Fixed identity",
        );
        for (raw of [null, {}, { description: "   " }]) {
          const error = yield* describe(identity).pipe(
            Effect.provideService(Clock.Clock, clock),
            Effect.flip,
          );
          assert.equal(error._tag, "ContextDescriptionError");
          assert.equal(error.path, identity.path);
        }
        const defect = new Error("broken reasoning invariant");
        const exit = yield* Effect.exit(
          makeDescriptionInitializer(() => Effect.die(defect))(identity),
        );
        assert.ok(Exit.isFailure(exit));
        assert.equal(Cause.squash(exit.cause), defect);
      }),
    ),
  );
});

test("AgentRunner cancellation releases memory tools before SDK idle for description", async (t) => {
  {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          let released = false,
            idle = false;
          t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) =>
            Effect.succeed({
              run: () =>
                Effect.acquireUseRelease(
                  Effect.sync(() => {
                    const memoryTool = options.tools!.find(
                      (tool) => tool.name === "memory_search",
                    )!;
                    const work = memoryTool.execute("search", { query: "evidence" });
                    return {
                      work,
                      settled: work.then(
                        () => undefined,
                        () => undefined,
                      ),
                    };
                  }),
                  ({ work }) =>
                    Effect.tryPromise({
                      try: async () => {
                        await work;
                        return { messages: [] };
                      },
                      catch: (cause) => new AgentError(String(cause)),
                    }),
                  ({ settled }) =>
                    Effect.promise(async () => {
                      await settled;
                      idle = true;
                    }),
                ),
            } satisfies Agent),
          );
          const blocked = Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(
              Effect.sync(() => {
                released = true;
              }),
            ),
          );
          const memory = { search: () => blocked, expand: () => Effect.succeed({ results: [] }) };
          const run = yield* makeStructuredReasoning("test", memory).pipe(
            Effect.provide(
              AgentRunner.layer.pipe(
                Layer.provide(models),
                Layer.provide(Layer.succeed(AgentConversations, testConversations())),
              ),
            ),
          );
          const work = makeDescriptionInitializer((prompt, schema) => run(prompt, schema, {}))(
            identity,
          );
          const fiber = yield* work.pipe(Effect.forkScoped);
          yield* Deferred.await(entered).pipe(Effect.timeout("2 seconds"));
          yield* Fiber.interrupt(fiber).pipe(Effect.timeout("2 seconds"));
          assert.equal(released, true);
          assert.equal(idle, true);
        }),
      ),
    );
  }
});
