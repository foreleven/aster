import assert from "node:assert/strict";
import { test } from "node:test";
import { Agent, AgentError, Models, type AgentMessage } from "@aster/agent";
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Layer } from "effect";
import { TestClock } from "effect/testing";
import { GoalReasoningError, GoalToolError, makeGoalReasoner } from "../src/index.js";

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

test("Goal tools distinguish committed rejection from missing acknowledgement", async (t) => {
  for (const unknown of [false, true]) {
    t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) =>
      Effect.succeed({
        run: () =>
          Effect.promise(async () => {
            assert.equal(options.tools!.find((tool) => tool.name === "task_list")!.replay, "safe");
            assert.equal(
              options.tools!.find((tool) => tool.name === "submit_plan")!.replay,
              "safe",
            );
            const mutate = options.tools!.find((tool) => tool.name === "signal_get")!;
            const call = mutate.execute("test", {
              operation: "task_create",
              id: "work",
              definition: { when: "Changed", task: "Review" },
            });
            if (unknown) await assert.rejects(Promise.resolve(call), /Acknowledgement missing/);
            else {
              const rejected = await call;
              assert.equal(rejected.isError, true);
              assert.deepEqual(rejected.details, { aster: { outcome: "rejected" } });
            }
            return { messages: [result] };
          }),
      } satisfies Agent),
    );
    await Effect.runPromise(
      Effect.gen(function* () {
        const reasoner = yield* makeGoalReasoner("test", memory);
        yield* reasoner.plan({
          ...input,
          tool: (request) => {
            assert.equal(
              request.operation,
              "signal_get",
              "the host owns the operation discriminator",
            );
            return Effect.fail(
              new GoalToolError({
                message: unknown ? "Acknowledgement missing" : "Stale revision",
                ...(unknown ? { outcome: "unknown" as const } : {}),
              }),
            );
          },
        });
      }).pipe(Effect.provide(models)),
    );
  }
});

test("Goal Agent returns Task proposals without calling the mutation boundary", async (t) => {
  const taskChanges = [
    {
      operation: "task_create" as const,
      id: "review",
      title: "Review",
      instructions: "Review evidence",
      evidence: [],
    },
  ];
  let calls = 0;
  t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) =>
    Effect.succeed({
      run: () =>
        Effect.promise(async () => {
          for (const name of [
            "task_create",
            "task_update",
            "task_delete",
            "task_execute",
            "signal_create",
            "signal_update",
            "signal_delete",
          ])
            assert.equal(
              options.tools!.some((tool) => tool.name === name),
              false,
            );
          const submit = options.tools!.find((tool) => tool.name === "submit_plan")!;
          const oversized = await submit.execute("oversized", {
            progress: "Review",
            completed: false,
            evidence: [],
            taskChanges: [{ ...taskChanges[0], instructions: "x".repeat(20000) }],
          });
          assert.equal(oversized.isError, true);
          assert.notEqual(oversized.terminate, true);
          for (const disposition of ["ignored", "no_change"]) {
            const rejected = await submit.execute(`invalid-${disposition}`, {
              disposition,
              progress: "No action",
              completed: false,
              evidence: [],
              taskChanges,
            });
            assert.equal(rejected.isError, true);
            assert.notEqual(rejected.terminate, true);
          }
          const proposal = await submit.execute("result", {
            progress: "Proposed review",
            completed: false,
            evidence: [],
            taskChanges,
            signalChanges: [
              {
                operation: "signal_create",
                id: "watch",
                definition: { when: "Changes", task: "Review" },
              },
            ],
          });
          assert.equal(proposal.terminate, true);
          return { messages: [{ ...result, details: proposal.details }] };
        }),
    } satisfies Agent),
  );
  const proposal = await Effect.runPromise(
    Effect.gen(function* () {
      const reasoner = yield* makeGoalReasoner("test", memory);
      return yield* reasoner.plan({
        ...input,
        tool: () =>
          Effect.sync(() => {
            calls++;
            return {};
          }),
      });
    }).pipe(Effect.provide(models)),
  );
  assert.equal(calls, 0);
  assert.deepEqual(proposal.taskChanges, taskChanges);
  assert.equal(proposal.signalChanges?.[0]?.operation, "signal_create");
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
