import assert from "node:assert/strict";
import { test } from "node:test";
import { Agent, AgentError, Models, type AgentMessage, type AgentTool } from "@aster/agent";
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
          assert.match(String(prompt), /Verify how each external update relates to this Goal/);
          assert.match(String(prompt), /Screening scores and rationales are routing hints/);
          assert.match(String(prompt), /ignored when admitted evidence is unrelated/);
          assert.match(String(prompt), /Do not turn unrelated facts into Goal progress/);
          assert.match(String(prompt), /For mixed inputs, work only from the relevant evidence/);
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

test("Goal reasoner can query a Context and cite its path without delegated execution", async (t) => {
  const path = "/apps/ctrip";
  let queried = false;
  t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) =>
    Effect.succeed({
      run: () =>
        Effect.promise(async () => {
          const query = options.tools!.find((tool) => tool.name === "query_context")!;
          assert.ok(query);
          const response = await query.execute("query", {
            path,
            command: "search",
            args: { query: "Sanya" },
          });
          assert.match(JSON.stringify(response), /Sanya/);
          const submit = options.tools!.find((tool) => tool.name === "finish_turn")!;
          const proposal = await submit.execute("plan", {
            disposition: "advance",
            progress: "Sanya is an option; confirm dates and budget.",
            nextStep: { _tag: "WaitForEvent", references: ["/source"] },
            evidence: [path],
            taskChanges: [],
            signalChanges: [],
          });
          return { messages: [{ ...result, details: proposal.details }] };
        }),
    } satisfies Agent),
  );
  const plan = await Effect.runPromise(
    Effect.gen(function* () {
      const reasoner = yield* makeGoalReasoner("test", memory, {
        queries: {
          register: () => Effect.void,
          query: (input) =>
            Effect.sync(() => {
              queried = true;
              return {
                path: input.path,
                command: input.command,
                queriedAt: "2026-10-04T00:00:00.000Z",
                data: [{ name: "Sanya" }],
              };
            }),
        },
      });
      return yield* reasoner.plan({
        ...input,
        contexts: {
          [path]: { path, description: "Ctrip travel queries", state: {}, messages: [] },
        },
      });
    }).pipe(Effect.provide(models)),
  );
  assert.ok(queried);
  assert.deepEqual(plan.evidence, [path]);
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

test("invalid Goal proposals are recoverable tool rejections before a valid result", async (t) => {
  t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) =>
    Effect.succeed({
      run: () =>
        Effect.promise(async () => {
          const submit = options.tools!.find((tool) => tool.name === "finish_turn")!;
          const valid = {
            disposition: "advance",
            progress: "Findings recorded",
            nextStep: { _tag: "WaitForEvent", references: ["/source"] },
            evidence: [],
            taskChanges: [],
            signalChanges: [],
          };
          const invalid = [
            { args: { ...valid, progress: "中".repeat(2001) }, message: /Summary must fit/ },
            {
              args: { ...valid, evidence: ["/missing"] },
              message: /Evidence must reference existing Context paths/,
            },
            {
              args: {
                ...valid,
                taskChanges: [
                  {
                    operation: "task_create",
                    id: "review",
                    title: "Review",
                    instructions: "Read evidence",
                    evidence: ["/missing"],
                  },
                ],
              },
              message: /Task evidence must reference existing Context paths/,
            },
            {
              args: { ...valid, nextStep: { _tag: "Complete", evidence: ["Findings"] } },
              message: /Goal completion requires criteria and evidence/,
            },
          ];
          for (const { args, message } of invalid) {
            const rejected = await submit.execute("invalid", args);
            assert.equal(rejected.isError, true);
            assert.notEqual(rejected.terminate, true);
            assert.match(JSON.stringify(rejected.content), message);
          }
          const accepted = await submit.execute("valid", valid);
          assert.equal(accepted.terminate, true);
          assert.notEqual(accepted.isError, true);
          return { messages: [{ ...result, details: accepted.details }] };
        }),
    } satisfies Agent),
  );
  const plan = await Effect.runPromise(
    Effect.gen(function* () {
      const reasoner = yield* makeGoalReasoner("test", memory);
      return yield* reasoner.plan(input);
    }).pipe(Effect.provide(models)),
  );
  assert.equal(plan.progress, "Findings recorded");
});

test("fresh Goal evaluations reject legacy, missing and malformed results", async (t) => {
  const legacy = { progress: "Old result", evidence: [], signals: [], completed: false };
  for (const messages of [
    [],
    [{ ...result, toolName: "submit_plan", details: legacy }],
    [{ ...result, details: legacy }],
    [{ ...result, details: null }],
    [{ ...result, isError: true }],
  ]) {
    t.mock.method(Agent, "make", () =>
      Effect.succeed({
        run: () => Effect.succeed({ messages }),
      } satisfies Agent),
    );
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const reasoner = yield* makeGoalReasoner("test", memory);
        return yield* Effect.exit(reasoner.plan(input));
      }).pipe(Effect.provide(models)),
    );
    assert.ok(Exit.isFailure(exit));
    const error = Cause.squash(exit.cause);
    assert.ok(error instanceof GoalReasoningError);
    assert.equal(error.outcome, "failed");
  }
});

const readCurrentGoal = async (tool: AgentTool) => {
  let text = "";
  let offset = 0;
  let pages = 0;
  while (true) {
    const response = await tool.execute("current", { offset });
    assert.notEqual(response.isError, true);
    assert.ok(Buffer.byteLength(JSON.stringify(response.content), "utf8") <= 14000);
    const content = response.content[0]!;
    assert.equal(content.type, "text");
    if (content.type !== "text") throw new Error("Expected a text tool result");
    const page = JSON.parse(content.text);
    text += page.content;
    pages++;
    if (page.nextOffset === null) {
      assert.equal(text.length, page.totalCharacters);
      return { snapshot: JSON.parse(text), pages };
    }
    assert.ok(page.nextOffset > offset);
    offset = page.nextOffset;
  }
};

test("Goal system policy and tool definitions stay stable while current facts change", async (t) => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const clock = yield* TestClock.make();
      yield* clock.adjust(12345);
      const prompts: string[] = [];
      const catalogues: string[] = [];
      const snapshots: unknown[] = [];
      t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) => {
        assert.equal(options.durable?.sessionId, "project");
        assert.deepEqual(JSON.parse(options.durable!.catalogueId!), [
          "aster.goal.v10",
          200000,
          8192,
        ]);
        catalogues.push(
          JSON.stringify(
            options.tools!.map(({ name, description, parameters, replay }) => ({
              name,
              description,
              parameters,
              replay,
            })),
          ),
        );
        return Effect.succeed({
          run: ({ messages }) =>
            Effect.promise(async () => {
              const system = messages.find((message) => message.role === "system")!;
              assert.equal(system.timestamp, 12345);
              const prompt = String(system.content);
              assert.match(prompt, /You are the user's personal assistant/);
              assert.match(prompt, /At the start of every turn, call goal_current/);
              assert.doesNotMatch(prompt, /PRIVATE_/);
              prompts.push(prompt);
              const current = options.tools!.find((tool) => tool.name === "goal_current")!;
              assert.equal(current.replay, "safe");
              const { snapshot } = await readCurrentGoal(current);
              snapshots.push(snapshot);
              const submit = options.tools!.find((tool) => tool.name === "finish_turn")!;
              const proposal = await submit.execute("continue", {
                disposition: "advance",
                progress: "Useful findings",
                nextStep: {
                  _tag: "Continue",
                  objective: "Inspect the remaining evidence",
                  previousResultId: snapshot.turnId,
                },
                evidence: [],
                taskChanges: [],
                signalChanges: [],
              });
              assert.equal(proposal.terminate, true);
              return { messages: [{ ...result, details: proposal.details }] };
            }),
        } satisfies Agent);
      });
      const reasoner = yield* makeGoalReasoner("test", memory);
      for (const index of [0, 1]) {
        const path = `/PRIVATE_source_${index}/record`;
        const turnId = `PRIVATE_turn_${index}`;
        const plan = yield* reasoner
          .plan({
            ...input,
            goal: {
              ...input.goal,
              description: `PRIVATE_goal_${index}`,
              completionCriteria: `PRIVATE_criteria_${index}`,
            },
            current: {
              ...input.current,
              state:
                index === 0
                  ? { progress: "PRIVATE_previous_progress" }
                  : { summary: "PRIVATE_current_summary", progress: "PRIVATE_old_progress" },
            },
            contexts: { [path]: { path, description: "Evidence", state: {}, messages: [] } },
            reason: `PRIVATE_reason_${index}`,
            durable: { ...input.durable, requestId: turnId },
          })
          .pipe(Effect.provideService(Clock.Clock, clock));
        assert.equal(plan.version, 2);
        if (plan.version === 2) {
          assert.equal(plan.turnId, turnId);
          assert.deepEqual(plan.nextStep, {
            _tag: "Continue",
            objective: "Inspect the remaining evidence",
            previousResultId: turnId,
          });
        }
        assert.deepEqual(snapshots[index], {
          turnId,
          admittedPurpose: `PRIVATE_reason_${index}`,
          goal: {
            ...input.goal,
            description: `PRIVATE_goal_${index}`,
            completionCriteria: `PRIVATE_criteria_${index}`,
          },
          summary: index === 0 ? "PRIVATE_previous_progress" : "PRIVATE_current_summary",
          availableContexts: {
            count: 1,
            roots: [`/PRIVATE_source_${index}`],
            instructions:
              "Use search_contexts to find relevant paths, then read_context. Both tools are paginated; no Context list is embedded here.",
          },
        });
      }
      assert.equal(prompts[0], prompts[1]);
      assert.equal(catalogues[0], catalogues[1]);
    }).pipe(Effect.provide(models), Effect.scoped),
  );
});

test("goal_current paginates large Unicode and escaped snapshots without losing facts", async (t) => {
  const description = '旅行计划😀\n"\\\u0000'.repeat(3000);
  const summary = "Existing findings: " + "证据".repeat(2500);
  t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) =>
    Effect.succeed({
      run: () =>
        Effect.promise(async () => {
          const current = options.tools!.find((tool) => tool.name === "goal_current")!;
          const { snapshot, pages } = await readCurrentGoal(current);
          assert.ok(pages > 1);
          assert.equal(snapshot.goal.description, description);
          assert.equal(snapshot.summary, summary);
          assert.equal(snapshot.turnId, input.durable.requestId);
          assert.deepEqual(snapshot.availableContexts.roots, []);
          return { messages: [result] };
        }),
    } satisfies Agent),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const reasoner = yield* makeGoalReasoner("test", memory);
      yield* reasoner.plan({
        ...input,
        goal: { ...input.goal, description },
        current: { ...input.current, state: { summary } },
      });
    }).pipe(Effect.provide(models)),
  );
});
