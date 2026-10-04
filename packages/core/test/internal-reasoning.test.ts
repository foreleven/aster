import assert from "node:assert/strict";
import { test } from "node:test";
import { AgentRunner, Agent, AgentError, Models } from "@aster/agent";
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Layer } from "effect";
import { TestClock } from "effect/testing";
import {
  makeDescriptionInitializer,
  makeStructuredReasoning,
  makeSignalExtractor,
} from "../src/index.js";

import { makeExecutionInputBuilder } from "../src/tasks/build-execution-input.js";

const identity = { path: "/test", identity: "Test", parentDescription: "Parent" };
const source = { path: "/test", description: "Test", state: {}, messages: [] };
const definition = {
  slug: "test",
  when: "now",
  task: "review",
  agent: "test",
  mode: "confirm",
} as const;
const models = Layer.succeed(Models, {
  resolve: () => Effect.die(new Error("Agent.make is mocked")),
});

test("description and extraction run lazily with the caller Clock and reject malformed output as tagged failures", async () => {
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
        const extract = makeSignalExtractor({ accessInstructions: [], run });
        const description = describe(identity);
        assert.equal(called, 0);
        assert.equal(
          yield* description.pipe(Effect.provideService(Clock.Clock, clock)),
          "Fixed identity",
        );
        assert.deepEqual(
          yield* extract(source.path, [definition], { [source.path]: source }).pipe(
            Effect.provideService(Clock.Clock, clock),
          ),
          ["test"],
        );
        for (raw of [null, {}, { description: "   " }]) {
          const error = yield* describe(identity).pipe(
            Effect.provideService(Clock.Clock, clock),
            Effect.flip,
          );
          assert.equal(error._tag, "ContextDescriptionError");
          assert.equal(error.path, identity.path);
          const extraction = yield* extract(source.path, [definition], {}).pipe(
            Effect.provideService(Clock.Clock, clock),
            Effect.flip,
          );
          assert.equal(extraction._tag, "SignalDetectionError");
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

test("AgentRunner cancellation releases memory tools before SDK idle for description, extraction and preparation", async (t) => {
  for (const kind of ["describe", "extract", "prepare"]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          let released = false,
            idle = false,
            searches = 0;
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
          const memory = {
            search: () =>
              Effect.suspend(() => {
                searches++;
                // Preparation does mandatory recall before invoking the internal Agent.
                return kind === "prepare" && searches === 1
                  ? Effect.succeed({ results: [] })
                  : blocked;
              }),
            expand: () => Effect.succeed({ results: [] }),
          };
          const run = yield* makeStructuredReasoning("test", memory).pipe(
            Effect.provide(AgentRunner.layer.pipe(Layer.provide(models))),
          );
          const work =
            kind === "describe"
              ? makeDescriptionInitializer((prompt, schema) => run(prompt, schema, {}))(identity)
              : kind === "extract"
                ? makeSignalExtractor({ run, accessInstructions: [] })(source.path, [definition], {
                    [source.path]: source,
                  })
                : makeExecutionInputBuilder({ memory, run, executorPrompt: () => "" })(
                    definition,
                    source,
                    {},
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
