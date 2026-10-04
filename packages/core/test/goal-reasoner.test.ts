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
  durable: { sessionId: "project", requestId: "test-turn" },
};
const result: AgentMessage = {
  role: "toolResult",
  toolCallId: "plan",
  toolName: "finish_turn",
  isError: false,
  content: [{ type: "text", text: "done" }],
  timestamp: 0,
  details: {
    version: 2,
    turnId: "test-turn",
    resultId: "test-turn",
    disposition: "advance",
    progress: "done",
    nextStep: { _tag: "WaitForEvent", references: ["/source"] },
    evidence: [],
  },
};
const models = Layer.succeed(Models, {
  resolve: () => Effect.die(new Error("Agent.make is controlled by this test")),
});
const memory = { search: () => Effect.sync(() => []), expand: () => Effect.sync(() => []) };

test("Goal planning receives an independent relevance check and can ignore a high-scored intent", async (t) => {
  const progress = "No DataAgent project link is established; ignore the dataset project's bugs.";
  t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) =>
    Effect.succeed({
      run: ({ messages }) =>
        Effect.promise(async () => {
          const prompt = messages.find((message) => message.role === "system")!.content;
          assert.equal(typeof prompt, "string");
          assert.match(String(prompt), /Independently verify each admitted input/);
          assert.match(String(prompt), /screening score and rationale are fallible routing hints/);
          assert.match(
            String(prompt),
            /If no input has a verified Goal link, use disposition ignored/,
          );
          assert.match(String(prompt), /Do not turn unrelated facts into Goal progress/);
          assert.match(
            String(prompt),
            /In a mixed batch, advance only the verified relevant inputs/,
          );
          assert.ok(messages.some((message) => JSON.stringify(message).includes("0.779")));
          const submit = options.tools!.find((tool) => tool.name === "finish_turn")!;
          const proposal = await submit.execute("ignore-unrelated", {
            disposition: "ignored",
            progress,
            nextStep: { _tag: "WaitForEvent", references: ["/source"] },
            evidence: [],
            taskChanges: [],
            signalChanges: [],
          });
          assert.equal(proposal.terminate, true);
          assert.notEqual(proposal.isError, true);
          return { messages: [{ ...result, details: proposal.details }] };
        }),
    } satisfies Agent),
  );
  const plan = await Effect.runPromise(
    Effect.gen(function* () {
      const reasoner = yield* makeGoalReasoner("test", memory);
      return yield* reasoner.plan({
        ...input,
        goal: { slug: "data-agent", description: "Track DataAgent (iDA) project progress." },
        messages: [
          {
            role: "user",
            content:
              '[Goal intent] {"summary":"The high-quality dataset project has overdue P0 labeling permission bugs, P1 agent node bugs and a daily standup.","relevance":{"score":0.779}}',
            timestamp: 0,
          },
        ],
      });
    }).pipe(Effect.provide(models)),
  );
  assert.equal(plan.disposition, "ignored");
  assert.equal(plan.progress, progress);
  assert.deepEqual(plan.taskChanges, []);
  assert.deepEqual(plan.signalChanges, []);
});

test("Goal planning can complete after three minutes without a whole-run deadline", async (t) => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const clock = yield* TestClock.make();
        const durable = { sessionId: "project", requestId: "long-planning-request" };
        t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) => {
          assert.equal(options.durable?.requestId, durable.requestId);
          return Effect.succeed({
            run: () =>
              Effect.gen(function* () {
                yield* Deferred.succeed(entered, undefined);
                yield* Effect.sleep("4 minutes");
                return { messages: [result] };
              }),
          } satisfies Agent);
        });
        const reasoner = yield* makeGoalReasoner("test", memory).pipe(Effect.provide(models));
        const fiber = yield* reasoner
          .plan({ ...input, durable })
          .pipe(Effect.provideService(Clock.Clock, clock), Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* clock.adjust("4 minutes");
        assert.equal((yield* Fiber.join(fiber)).progress, "done");
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Goal read callbacks retain the caller Clock and cancellation", async (t) => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let observed = 0;
        t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) =>
          Effect.succeed({
            run: () =>
              Effect.promise(async () => {
                assert.equal(options.onMessage, undefined);
                await options
                  .tools!.find((tool) => tool.name === "memory_search")!
                  .execute("read", { query: "project" });
                return { messages: [result] };
              }),
          } satisfies Agent),
        );
        const clock = yield* TestClock.make();
        yield* clock.adjust(12345);
        const reasoner = yield* makeGoalReasoner("test", {
          ...memory,
          search: () =>
            Effect.gen(function* () {
              observed = yield* Clock.currentTimeMillis;
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
              return [];
            }),
        }).pipe(Effect.provide(models));
        const fiber = yield* reasoner
          .plan(input)
          .pipe(Effect.provideService(Clock.Clock, clock), Effect.forkScoped);
        yield* Deferred.await(entered);
        assert.equal(observed, 12345);
        yield* Deferred.succeed(release, undefined);
        assert.equal((yield* Fiber.join(fiber)).progress, "done");
      }),
    ),
  );
});

test("cancelling a Goal releases SDK callback waits before the Agent idle finalizer", async (t) => {
  for (const kind of ["memory"]) {
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
                      tool.execute("call", { query: "test" }),
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
          const fiber = yield* reasoner.plan(input).pipe(Effect.forkScoped);
          yield* Deferred.await(entered).pipe(Effect.timeout("2 seconds"));
          yield* Fiber.interrupt(fiber).pipe(Effect.timeout("2 seconds"));
          assert.equal(idle, true);
          assert.equal(released, true);
        }),
      ),
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
          const submit = options.tools!.find((tool) => tool.name === "finish_turn")!;
          for (const operation of ["signal_create", "signal_update", "signal_delete"]) {
            for (const id of ["/signals/watch", "Watch", "watch_progress", "", "监控进展"]) {
              const invalidSignal = await submit.execute("invalid-signal-id", {
                disposition: "advance",
                progress: "Watch progress",
                nextStep: { _tag: "WaitForEvent", references: ["/source"] },
                evidence: [],
                taskChanges: [],
                signalChanges: [{ operation, id, revision: 1, definition: {} }],
              });
              assert.equal(invalidSignal.isError, true);
              assert.notEqual(invalidSignal.terminate, true);
              assert.match(JSON.stringify(invalidSignal.content), /Invalid Signal ID/);
            }
          }
          const oversized = await submit.execute("oversized", {
            disposition: "advance",
            progress: "Review",
            nextStep: { _tag: "WaitForEvent", references: ["/source"] },
            evidence: [],
            taskChanges: [{ ...taskChanges[0], instructions: "x".repeat(20000) }],
          });
          assert.equal(oversized.isError, true);
          assert.notEqual(oversized.terminate, true);
          for (const disposition of ["ignored", "no_change"]) {
            const rejected = await submit.execute(`invalid-${disposition}`, {
              disposition,
              progress: "No action",
              nextStep: { _tag: "WaitForEvent", references: ["/source"] },
              evidence: [],
              taskChanges,
            });
            assert.equal(rejected.isError, true);
            assert.notEqual(rejected.terminate, true);
          }
          const proposal = await submit.execute("result", {
            disposition: "advance",
            progress: "Proposed review",
            nextStep: { _tag: "WaitForEvent", references: ["/source"] },
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
      });
    }).pipe(Effect.provide(models)),
  );
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
          const tool = options.tools!.find((value) => value.name === "memory_search")!;
          // SDKs report a rejected tool as a model-visible error and may keep planning.
          await tool.execute("call", { query: "test" }).catch(() => undefined);
          return { messages: [result] };
        }),
    } satisfies Agent),
  );
  const exit = await Effect.runPromise(
    Effect.gen(function* () {
      const reasoner = yield* makeGoalReasoner("test", {
        ...memory,
        search: () => Effect.die(defect),
      });
      return yield* Effect.exit(reasoner.plan(input));
    }).pipe(Effect.provide(models)),
  );
  assert.ok(Exit.isFailure(exit));
  assert.equal(Cause.squash(exit.cause), defect);
});

test("Task and Signal tools read frozen snapshots including deleted entries", async (t) => {
  const deletedTask = {
    id: "retired",
    title: "Previous work",
    instructions: "Keep evidence",
    revision: 3,
    status: "deleted",
    evidence: [],
    createdAt: "2026-01-01",
    updatedAt: "2026-01-02",
  };
  const deletedSignal = { slug: "project--retired", goal: "project", deleted: true, revision: 2 };
  t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) =>
    Effect.succeed({
      run: () =>
        Effect.promise(async () => {
          const read = async (name: string, args: object) => {
            const response = await options
              .tools!.find((tool) => tool.name === name)!
              .execute("read", args);
            assert.notEqual(response.isError, true);
            return response.details;
          };
          assert.deepEqual(await read("task_list", {}), []);
          assert.deepEqual(await read("task_get", { id: "retired" }), deletedTask);
          assert.deepEqual(await read("signal_list", {}), []);
          assert.deepEqual(await read("signal_get", { id: "retired" }), deletedSignal);
          return { messages: [result] };
        }),
    } satisfies Agent),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const reasoner = yield* makeGoalReasoner("test", memory);
      yield* reasoner.plan({
        ...input,
        current: { ...input.current, state: { tasks: [deletedTask] } },
        contexts: {
          "/signals/project--retired": {
            path: "/signals/project--retired",
            description: "Archived monitor",
            messages: [],
            state: deletedSignal,
          },
        },
      });
    }).pipe(Effect.provide(models)),
  );
});

test("replay-only inspection decodes a saved legacy result without the reconcile flag", async (t) => {
  const legacy = {
    progress: "Recovered old findings",
    evidence: [],
    signals: [],
    completed: false,
  };
  t.mock.method(Agent, "make", () =>
    Effect.succeed({
      run: () =>
        Effect.succeed({ messages: [{ ...result, toolName: "submit_plan", details: legacy }] }),
    } satisfies Agent),
  );
  const restored = await Effect.runPromise(
    Effect.gen(function* () {
      const reasoner = yield* makeGoalReasoner("test", memory);
      return yield* reasoner.plan({
        ...input,
        durable: { ...input.durable, replayOnly: true, reconcile: false },
      });
    }).pipe(Effect.provide(models)),
  );
  assert.deepEqual(restored, legacy);
});
