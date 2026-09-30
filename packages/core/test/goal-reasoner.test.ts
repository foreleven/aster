import assert from "node:assert/strict";
import { test } from "node:test";
import { Agent, AgentError, Models, type AgentMessage } from "@aster/agent";
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Layer } from "effect";
import { TestClock } from "effect/testing";
import { GoalReasoningError, makeGoalReasoner } from "../src/index.js";

const input = {
  goal: { slug: "project", description: "Review" },
  current: { path: "/goals/project", description: "Review", state: {}, messages: [] },
  contexts: {},
  signals: [],
  reason: "test",
};
const result: AgentMessage = {
  role: "toolResult",
  toolCallId: "plan",
  toolName: "submit_plan",
  isError: false,
  content: [{ type: "text", text: "done" }],
  timestamp: 0,
  details: { progress: "done", completed: false, evidence: [], signals: [] },
};
const models = Layer.succeed(Models, {
  resolve: () => Effect.die(new Error("Agent.make is controlled by this test")),
});
const memory = { search: () => Effect.sync(() => []), expand: () => Effect.sync(() => []) };

test("Goal SDK callbacks retain the caller Clock and wait for transcript persistence before returning a plan", async (t) => {
  let toolTime: number | undefined;
  let transcriptTime: number | undefined;
  let returned = false;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const persisted = yield* Deferred.make<void>();
        t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) =>
          Effect.succeed({
            run: () =>
              Effect.tryPromise({
                try: async () => {
                  const taskList = options.tools!.find((tool) => tool.name === "task_list")!;
                  await taskList.execute("list", {});
                  await options.onMessage!(result);
                  return { messages: [result] };
                },
                catch: (cause) => new AgentError(String(cause)),
              }),
          } satisfies Agent),
        );
        const reasoner = yield* makeGoalReasoner("test", memory).pipe(Effect.provide(models));
        const clock = yield* TestClock.make();
        yield* clock.adjust(12345);
        const fiber = yield* reasoner
          .plan({
            ...input,
            tool: () =>
              Clock.currentTimeMillis.pipe(
                Effect.tap((time) =>
                  Effect.sync(() => {
                    toolTime = time;
                  }),
                ),
              ),
            onMessage: () =>
              Effect.gen(function* () {
                transcriptTime = yield* Clock.currentTimeMillis;
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(persisted);
              }),
          })
          .pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                returned = true;
              }),
            ),
            Effect.provideService(Clock.Clock, clock),
            Effect.forkScoped,
          );
        yield* Deferred.await(entered).pipe(Effect.timeout("2 seconds"));
        assert.equal(toolTime, 12345);
        assert.equal(transcriptTime, 12345);
        assert.equal(returned, false);
        yield* Deferred.succeed(persisted, undefined);
        assert.equal((yield* Fiber.join(fiber)).progress, "done");
        assert.equal(returned, true);
      }),
    ),
  );
});

test("cancelling a Goal releases SDK callback waits before the Agent idle finalizer", async (t) => {
  for (const kind of ["transcript", "tool", "memory"]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          let released = false;
          let idle = false;
          t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) =>
            Effect.succeed({
              run: () =>
                Effect.acquireUseRelease(
                  Effect.sync(() => {
                    const tool = options.tools!.find(
                      (value) => value.name === (kind === "memory" ? "memory_search" : "task_list"),
                    )!;
                    const work = Promise.resolve().then<unknown>(() =>
                      kind === "transcript"
                        ? options.onMessage!(result)
                        : tool.execute("call", kind === "memory" ? { query: "test" } : {}),
                    );
                    // Like Agent.run, teardown waits until the SDK callback has settled.
                    const settled = work.then(
                      () => undefined,
                      () => undefined,
                    );
                    return { work, settled };
                  }),
                  ({ work }) =>
                    Effect.tryPromise({
                      try: async () => {
                        await work;
                        return { messages: [result] };
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
          const reasoner = yield* makeGoalReasoner(
            "test",
            kind === "memory"
              ? {
                  ...memory,
                  search: () => blocked,
                }
              : memory,
          ).pipe(Effect.provide(models));
          const fiber = yield* reasoner
            .plan({ ...input, tool: () => blocked, onMessage: () => blocked })
            .pipe(Effect.forkScoped);
          yield* Deferred.await(entered).pipe(Effect.timeout("2 seconds"));
          yield* Fiber.interrupt(fiber).pipe(Effect.timeout("2 seconds"));
          assert.equal(idle, true);
          assert.equal(released, true);
        }),
      ),
    );
  }
});

test("model failures are tagged while a reasoning defect retains its original cause", async (t) => {
  const defect = new Error("broken invariant");
  for (const broken of [false, true]) {
    t.mock.method(Agent, "make", () =>
      broken ? Effect.die(defect) : Effect.fail(new AgentError("provider unavailable")),
    );
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const reasoner = yield* makeGoalReasoner("test", memory);
        return yield* Effect.exit(reasoner.plan(input));
      }).pipe(Effect.provide(models)),
    );
    assert.ok(Exit.isFailure(exit));
    if (broken) assert.equal(Cause.squash(exit.cause), defect);
    else {
      const error = Cause.squash(exit.cause);
      assert.ok(error instanceof GoalReasoningError);
      assert.equal(error.operation, "plan");
      assert.equal(error.message, "provider unavailable");
    }
  }
});

test("SDK tool error handling cannot turn an Effect callback defect into a successful plan", async (t) => {
  const defect = new Error("tool invariant broken");
  t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) =>
    Effect.succeed({
      run: () =>
        Effect.promise(async () => {
          const tool = options.tools!.find((value) => value.name === "task_list")!;
          // SDKs report a rejected tool as a model-visible error and may keep planning.
          await tool.execute("call", {}).catch(() => undefined);
          return { messages: [result] };
        }),
    } satisfies Agent),
  );
  const exit = await Effect.runPromise(
    Effect.gen(function* () {
      const reasoner = yield* makeGoalReasoner("test", memory);
      return yield* Effect.exit(reasoner.plan({ ...input, tool: () => Effect.die(defect) }));
    }).pipe(Effect.provide(models)),
  );
  assert.ok(Exit.isFailure(exit));
  assert.equal(Cause.squash(exit.cause), defect);
});
