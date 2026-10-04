import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Fiber, Layer } from "effect";
import { makeTaskExecution, ExternalAgents, SystemOneClient, DecisionError } from "../src/index.js";
import { AgentRunner } from "@aster/agent";
import { reasoningConfig, emptyRecall, agentResult } from "./workflow-fixtures.js";
const reasoning = Layer.mergeAll(
  reasoningConfig,
  emptyRecall,
  Layer.succeed(
    AgentRunner,
    AgentRunner.make(() =>
      Effect.succeed(
        agentResult("submit_result", { instructions: "Read-only analysis", input: [] }),
      ),
    ),
  ),
);

test("readiness preserves typed decision errors and cancellation of the injected Effect", async () => {
  for (const fails of [true, false]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          let finalized = false;
          const cause = new DecisionError({ message: "Decision service unavailable" });
          const layer = Layer.mergeAll(
            Layer.mergeAll(
              reasoning,
              Layer.succeed(ExternalAgents, {}),
              Layer.succeed(SystemOneClient, {
                systemOne: () =>
                  fails
                    ? Effect.fail(cause)
                    : Effect.gen(function* () {
                        yield* Deferred.succeed(entered, undefined);
                        return yield* Effect.never;
                      }).pipe(
                        Effect.ensuring(
                          Effect.sync(() => {
                            finalized = true;
                          }),
                        ),
                      ),
              }),
            ),
          );
          yield* Effect.gen(function* () {
            const preparation = yield* makeTaskExecution();
            const decision = preparation.checkReadiness(
              { slug: "test", when: "now", task: "Review", agent: "test", mode: "auto" },
              { path: "/test", description: "Test", state: {}, messages: [] },
              { instructions: "Review", input: [] },
            );
            if (fails) {
              const error = yield* Effect.flip(decision);
              assert.equal(error._tag, "TaskPreparationError");
              assert.equal(error.operation, "readiness");
              assert.equal(error.cause, cause);
            } else {
              const fiber = yield* decision.pipe(Effect.forkScoped);
              yield* Deferred.await(entered).pipe(Effect.timeout("2 seconds"));
              yield* Fiber.interrupt(fiber);
              assert.equal(finalized, true);
            }
          }).pipe(Effect.provide(layer));
        }),
      ),
    );
  }
});
test("Task execution uses injected AgentRunner and execution capabilities", async () => {
  let capabilities: unknown;
  const layer = Layer.mergeAll(
    Layer.mergeAll(
      reasoning,
      Layer.succeed(ExternalAgents, {}),
      Layer.succeed(SystemOneClient, {
        systemOne: (input) =>
          Effect.sync(() => {
            assert.equal(typeof input.state, "string");
            capabilities = JSON.parse(input.state as string).execution;
            return { answers: { executable: { type: "choice", choice: "no" } } };
          }),
      }),
    ),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const preparation = yield* makeTaskExecution();
      const definition = {
        slug: "test",
        when: "changed",
        task: "Read-only analysis",
        agent: "missing",
        mode: "confirm" as const,
      };
      const source = { path: "/test", description: "test", state: {}, messages: [] };
      const task = yield* preparation.buildExecutionInput(definition, source, {});
      assert.ok(task.instructions.endsWith("Read-only analysis"));
      assert.equal(yield* preparation.checkReadiness(definition, source, task), false);
      assert.deepEqual(capabilities, {
        supportedAgent: false,
        workspace: "Isolated local workspace",
        capabilities: "Unsupported executor",
      });
    }).pipe(Effect.provide(layer)),
  );
});
