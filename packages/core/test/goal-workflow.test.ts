import { TaskPreparationError } from "../src/index.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, ActorTestKit, type ActorRef } from "@aster/actor";
import { Cause, Clock, Effect, Exit, Layer, Stream } from "effect";
import { TestClock } from "effect/testing";
import {
  ContextRegistry,
  makeContextRegistry,
  GoalRuntime,
  GoalReasoningError,
  GoalsRootActor,
  SignalRootActor,
  SignalDefinitions,
  ExternalAgents,
  ApprovalQueueActor,
  approvalEntries,
  makeGoalRuntime,
  makeMemoryGoalHistory,
  type GoalReasoner,
  type GoalTask,
  type GoalToolRequest,
  type SignalRootCommand,
  type ContextStore,
  TaskPreparation,
  type Task,
  type ExecutionStatus,
  type ExternalAgent,
} from "../src/index.js";
import { preparationLayer, fakeAgent } from "./fixtures.js";

const until = (condition: () => boolean) =>
  Effect.gen(function* () {
    while (!condition()) yield* Effect.sleep(2);
  }).pipe(Effect.timeout("5 seconds"));
const setup = (
  reasoner: GoalReasoner,
  options: {
    store?: ContextStore;
    history?: ReturnType<typeof makeMemoryGoalHistory>;
    clock?: Clock.Clock;
    submit?: ExternalAgent["submit"];
    prepare?: () => Promise<Task>;
    wait?: ExternalAgent["wait"];
    contextTokens?: number;
  } = {},
) =>
  Effect.gen(function* () {
    const registry = yield* makeContextRegistry(options.store);
    const history = options.history ?? makeMemoryGoalHistory();
    const proxy: ActorRef<SignalRootCommand> = {
      path: "/signals",
      incarnation: "test",
      tell: (cmd) => root.tell(cmd),
      ask: (cmd, timeout) => root.ask(cmd, timeout),
    };
    const runtime = makeGoalRuntime(
      {
        reasoning: { model: "test", contextTokens: options.contextTokens },
        definitions: [{ slug: "project", description: "Observe the project" }],
      },
      registry,
      proxy,
      reasoner,
      history,
    );
    const clock = options.clock ?? (yield* Clock.Clock);
    const base = ActorSystem.make().pipe(
      ActorSystem.provide(
        Layer.succeed(Clock.Clock, clock),
        Layer.succeed(ContextRegistry, registry),
        Layer.succeed(GoalRuntime, runtime),
        Layer.succeed(SignalDefinitions, []),
        options.prepare
          ? Layer.succeed(TaskPreparation, {
              prepare: () =>
                Effect.tryPromise({
                  try: options.prepare!,
                  catch: (cause) =>
                    new TaskPreparationError({
                      operation: "prepare",
                      cause,
                      message: String(cause),
                    }),
                }),
              ready: () => Effect.sync(() => true),
            })
          : preparationLayer,
        Layer.succeed(ExternalAgents, {
          "doubao-delegate": fakeAgent({
            ...(options.submit ? { submit: options.submit } : {}),
            ...(options.wait ? { wait: options.wait } : {}),
          }),
        }),
      ),
    );
    const system = yield* base;
    const approvals = yield* system.spawn("approvals", ApprovalQueueActor);
    const root = yield* system.spawn("signals", SignalRootActor);
    const goals = yield* system.spawn("goals", GoalsRootActor);
    const operate = (request: GoalToolRequest) =>
      goals.ask<{ value?: unknown; error?: string }>((replyTo) => ({
        _tag: "Route",
        slug: "project",
        command: { _tag: "Tool", request, replyTo },
      }));
    return { registry, history, goals, root, approvals, operate, system };
  });
const plan = {
  progress: "Record observations and continue monitoring",
  completed: false,
  signals: [],
  evidence: [],
};

test("Goal-owned Signal wakes assessment without delegating and task tools enforce revision confirmation", async () => {
  let calls = 0,
    submissions = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* setup(
          {
            plan: (input) =>
              Effect.sync(() => {
                calls++;
                assert.ok(input.messages?.every((m) => "role" in m));
                return plan;
              }),
          },
          {
            submit: () =>
              Effect.sync(() => {
                submissions++;
                return { sessionId: "one" };
              }),
          },
        );
        yield* env.goals.tell({ _tag: "Initialize" });
        yield* until(() => calls === 1);
        const signal = yield* env.operate({
          operation: "signal_create",
          id: "watch",
          definition: { when: "A new issue appears" },
        });
        assert.equal(signal.error, undefined);
        const trigger = {
          _tag: "Trigger" as const,
          slug: "project--watch",
          sourceContext: {
            path: "/chat/a",
            description: "Project chat",
            state: {},
            messages: ["New issue"],
          },
        };
        yield* env.root.tell(trigger);
        yield* until(() => calls >= 2);
        yield* env.root.tell(trigger);
        yield* Effect.sleep(15);
        assert.equal(calls, 2);
        assert.equal(submissions, 0);
        assert.equal(
          Object.keys(env.registry.snapshot()).some((p) => p.includes("/runs/")),
          false,
        );
        assert.match(JSON.stringify(yield* env.history.read("project")), /Signal matched/);
        yield* env.operate({
          operation: "task_create",
          id: "analysis",
          title: "Analyze the issue",
          instructions: "Read-only analysis",
          evidence: [],
        });
        const first = yield* env.operate({
          operation: "task_execute",
          id: "analysis",
          revision: 1,
        });
        const again = yield* env.operate({
          operation: "task_execute",
          id: "analysis",
          revision: 1,
        });
        assert.equal((again.value as { reused?: boolean }).reused, true);
        yield* until(() => approvalEntries(env.registry).some((e) => e.status === "pending"));
        const old = approvalEntries(env.registry)[0]!;
        const updated = yield* env.operate({
          operation: "task_update",
          id: "analysis",
          revision: 1,
          instructions: "Add compatibility evidence",
        });
        assert.equal((updated.value as GoalTask).revision, 2);
        yield* until(() => approvalEntries(env.registry)[0]!.status === "revoked");
        const rejectOld = yield* env.approvals.ask<{ error?: string }>((replyTo) => ({
          _tag: "Resolve",
          id: old.id,
          response: { decision: "approve" },
          replyTo,
        }));
        assert.ok(rejectOld.error);
        assert.equal(submissions, 0);
        const next = yield* env.operate({ operation: "task_execute", id: "analysis", revision: 2 });
        assert.notDeepEqual(next.value, first.value);
        yield* until(
          () => approvalEntries(env.registry).filter((e) => e.status === "pending").length === 1,
        );
        const current = approvalEntries(env.registry).find((e) => e.status === "pending")!;
        yield* env.approvals.ask((replyTo) => ({
          _tag: "Resolve",
          id: current.id,
          response: { decision: "approve" },
          replyTo,
        }));
        yield* env.approvals.tell({ _tag: "Deliver" });
        yield* until(
          () =>
            submissions === 1 &&
            (env.registry.get("/goals/project")!.state as { tasks: GoalTask[] }).tasks[0]!.execution
              ?.status === "completed",
        );
        const task = (env.registry.get("/goals/project")!.state as { tasks: GoalTask[] }).tasks[0]!;
        assert.equal(
          task.status,
          "open",
          "execution success does not overwrite business completion",
        );
        yield* env.operate({
          operation: "task_update",
          id: task.id,
          revision: 2,
          status: "completed",
        });
        assert.equal(
          (env.registry.get("/signals/project--watch")!.state as { active: boolean }).active,
          true,
        );
        yield* env.operate({ operation: "task_delete", id: task.id, revision: 3 });
        assert.equal((yield* env.operate({ operation: "task_list" })).value instanceof Array, true);
        assert.deepEqual((yield* env.operate({ operation: "task_list" })).value, []);
        assert.equal(
          (env.registry.get("/signals/project--watch")!.state as { active: boolean }).active,
          true,
        );
        assert.ok((yield* env.operate({ operation: "task_get", id: task.id })).value);
      }),
    ),
  );
});

test("once timer waits, survives restart overdue once, and deleted/revised timers cannot fire", async () => {
  const records = new Map<string, any>();
  const store: ContextStore = {
    loadAll: () => structuredClone([...records.values()]),
    save: (record) => {
      records.set(record.path, structuredClone(record));
    },
  };
  const history = makeMemoryGoalHistory();
  let calls = 0;
  const start = Date.parse("2026-09-30T10:00:00+08:00");
  for (const restart of [false, true, true])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const clock = yield* TestClock.make();
          yield* clock.adjust(start + (restart ? 3600000 : 0));
          const env = yield* setup(
            {
              plan: () =>
                Effect.sync(() => {
                  calls++;
                  return plan;
                }),
            },
            { store, history, clock },
          );
          if (!restart) {
            yield* env.operate({
              operation: "signal_create",
              id: "timer",
              definition: { schedule: { type: "once", at: new Date(start + 10000).toISOString() } },
            });
            yield* Effect.sleep(10);
            assert.equal(calls, 0, "must not run immediately like Effect.repeat");
            yield* env.operate({
              operation: "signal_create",
              id: "deleted",
              definition: { schedule: { type: "once", at: new Date(start + 5000).toISOString() } },
            });
            yield* env.operate({ operation: "signal_delete", id: "deleted", revision: 1 });
            yield* clock.adjust(6000);
            yield* Effect.sleep(10);
            assert.equal(calls, 0);
          } else {
            yield* until(
              () =>
                (env.registry.get("/signals/project--timer")?.state as { timerDone?: boolean })
                  ?.timerDone === true,
            );
            yield* until(() => calls >= 1);
            yield* Effect.sleep(15);
            assert.equal(calls, 1);
            assert.equal(
              (env.registry.get("/signals/project--timer")!.state as { occurrences: unknown[] })
                .occurrences.length,
              1,
            );
          }
        }),
      ),
    );
});

test("compaction retains original history and restart loads summary plus a bounded native window", async () => {
  const history = makeMemoryGoalHistory(),
    records = new Map<string, any>();
  const store: ContextStore = {
    loadAll: () => structuredClone([...records.values()]),
    save: (record) => {
      records.set(record.path, structuredClone(record));
    },
  };
  for (let i = 0; i < 20; i++)
    await Effect.runPromise(
      history.append("project", {
        role: "user",
        content: `Evidence ${i} ${"x".repeat(1200)}`,
        timestamp: i,
      }),
    );
  let calls = 0,
    compacted = 0;
  const reasoner: GoalReasoner = {
    compact: (_summary, messages) =>
      Effect.sync(() => {
        compacted++;
        assert.ok(messages.length);
        return "Key evidence and conclusions retained";
      }),
    plan: (input) =>
      Effect.sync(() => {
        calls++;
        assert.ok(JSON.stringify(input.messages).length < 14000);
        assert.ok(input.current.state);
        return plan;
      }),
  };
  for (let i = 0; i < 2; i++)
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* setup(reasoner, { store, history });
          yield* env.goals.tell({ _tag: "Initialize" });
          yield* until(() => calls > i);
          yield* until(
            () =>
              (env.registry.get("/goals/project")!.state as { summary: string }).summary ===
              plan.progress,
          );
          assert.ok(
            (env.registry.get("/goals/project")!.state as { historyThrough: number })
              .historyThrough > 0,
          );
        }),
      ),
    );
  assert.ok(compacted > 0);
  assert.equal(
    (await Effect.runPromise(history.read("project", { limit: 1 })))[0]!.message.role,
    "user",
  );
  assert.ok((await Effect.runPromise(history.count("project"))) > 20);
});

test("deleting during preparation ignores late results; deleting during execution preserves its result", async () => {
  for (const running of [false, true])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          let prepared: ((task: Task) => void) | undefined,
            finished: ((status: ExecutionStatus) => void) | undefined,
            submitted = 0;
          const env = yield* setup(
            { plan: () => Effect.sync(() => plan) },
            {
              ...(!running
                ? {
                    prepare: () =>
                      new Promise<Task>((resolve) => {
                        prepared = resolve;
                      }),
                  }
                : {}),
              submit: () =>
                Effect.sync(() => {
                  submitted++;
                  return { sessionId: "running" };
                }),
              wait: () =>
                Effect.callback<ExecutionStatus>((resume) => {
                  finished = (status) => resume(Effect.succeed(status));
                }),
            },
          );
          yield* env.operate({
            operation: "task_create",
            id: "work",
            title: "Work",
            instructions: "Read-only inspection",
          });
          yield* env.operate({ operation: "task_execute", id: "work", revision: 1 });
          if (running) {
            yield* until(() => approvalEntries(env.registry).some((e) => e.status === "pending"));
            yield* env.approvals.ask((replyTo) => ({
              _tag: "Resolve",
              id: approvalEntries(env.registry)[0]!.id,
              response: { decision: "approve" },
              replyTo,
            }));
            yield* env.approvals.tell({ _tag: "Deliver" });
            yield* until(() => !!finished);
          } else yield* until(() => !!prepared);
          yield* env.operate({ operation: "task_delete", id: "work", revision: 1 });
          if (running)
            finished!({ state: "completed", result: { text: "Read-only inspection completed" } });
          else prepared!({ instructions: "Late plan", input: [] });
          yield* until(() => {
            const task = (env.registry.get("/goals/project")!.state as { tasks: GoalTask[] })
              .tasks[0]!;
            return (
              task.status === "deleted" &&
              task.execution?.status === (running ? "completed" : "cancelled")
            );
          });
          assert.equal(submitted, running ? 1 : 0);
          if (running)
            assert.match(
              JSON.stringify(env.registry.get("/goals/project")!.state),
              /Read-only inspection completed/,
            );
          else assert.equal(approvalEntries(env.registry).length, 0);
        }),
      ),
    );
});

test("Cron respects its time zone and a schedule edit invalidates the old deadline", async () => {
  const start = Date.parse("2026-09-30T19:59:00+08:00");
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(start);
        let calls = 0;
        const env = yield* setup(
          {
            plan: () =>
              Effect.sync(() => {
                calls++;
                return plan;
              }),
          },
          { clock },
        );
        yield* env.operate({
          operation: "signal_create",
          id: "daily",
          definition: {
            schedule: { type: "cron", expression: "0 20 * * *", timeZone: "Asia/Shanghai" },
          },
        });
        yield* until(
          () =>
            typeof (env.registry.get("/signals/project--daily")!.state as { nextDue?: number })
              .nextDue === "number",
        );
        assert.equal(
          (env.registry.get("/signals/project--daily")!.state as { nextDue: number }).nextDue,
          start + 60000,
        );
        yield* env.operate({
          operation: "signal_update",
          id: "daily",
          revision: 1,
          definition: {
            schedule: { type: "cron", expression: "0 21 * * *", timeZone: "Asia/Shanghai" },
          },
        });
        yield* clock.adjust(61000);
        yield* Effect.sleep(10);
        assert.equal(calls, 0);
        yield* clock.adjust(3600000);
        yield* until(() => calls === 1);
        const state = env.registry.get("/signals/project--daily")!.state as {
          nextDue: number;
          occurrences: unknown[];
        };
        assert.equal(state.occurrences.length, 1);
        assert.ok(state.nextDue > start + 61000 + 3600000);
      }),
    ),
  );
});

test("Cron restart coalesces several missed days into one occurrence then waits for the next deadline", async () => {
  const records = new Map<string, any>(),
    history = makeMemoryGoalHistory();
  const store: ContextStore = {
    loadAll: () => structuredClone([...records.values()]),
    save: (record) => {
      records.set(record.path, structuredClone(record));
    },
  };
  let calls = 0;
  for (const now of ["2026-09-30T19:00:00+08:00", "2026-10-03T22:00:00+08:00"])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const clock = yield* TestClock.make();
          yield* clock.adjust(Date.parse(now));
          const env = yield* setup(
            {
              plan: () =>
                Effect.sync(() => {
                  calls++;
                  return plan;
                }),
            },
            { clock, store, history },
          );
          if (now.startsWith("2026-09-30")) {
            yield* env.operate({
              operation: "signal_create",
              id: "daily",
              definition: {
                schedule: { type: "cron", expression: "0 20 * * *", timeZone: "Asia/Shanghai" },
              },
            });
            assert.equal(
              (env.registry.get("/signals/project--daily")!.state as { nextDue: number }).nextDue,
              Date.parse("2026-09-30T20:00:00+08:00"),
            );
          } else {
            yield* until(() => calls >= 1);
            yield* Effect.sleep(20);
            assert.equal(calls, 1, "startup reconciliation must not enqueue a second assessment");
            const s = env.registry.get("/signals/project--daily")!.state as {
              nextDue: number;
              occurrences: unknown[];
            };
            assert.equal(s.occurrences.length, 1);
            assert.equal(s.nextDue, Date.parse("2026-10-04T20:00:00+08:00"));
          }
        }),
      ),
    );
});

test("recurring timer snapshots grow linearly and never embed occurrence bookkeeping", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(Date.parse("2026-09-30T00:00:00Z"));
        const env = yield* setup({ plan: () => Effect.sync(() => plan) }, { clock });
        yield* env.operate({
          operation: "signal_create",
          id: "minute",
          definition: {
            schedule: { type: "cron", expression: "* * * * *", timeZone: "UTC" },
          },
        });
        const record = () => env.registry.get("/signals/project--minute")!;
        const occurrences = () =>
          (record().state as { occurrences?: { source: { state: object } }[] }).occurrences ?? [];
        for (let i = 1; i <= 10; i++) {
          yield* clock.adjust(60000);
          yield* until(() => occurrences().length === i);
        }
        for (const occurrence of occurrences())
          assert.equal("occurrences" in occurrence.source.state, false);
        assert.ok(Buffer.byteLength(JSON.stringify(record())) < 20000);
      }),
    ),
  );
});

test("a tool storage defect restarts Goal once and reuses its pending Run", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        let fail = false;
        const env = yield* setup(
          { plan: () => Effect.sync(() => plan) },
          {
            store: {
              loadAll: () => [],
              save: (record) => {
                if (fail && record.path === "/goals/project") {
                  fail = false;
                  throw new Error("one disk failure");
                }
              },
            },
          },
        );
        const events: { _tag: string; path?: string }[] = [];
        yield* Stream.runForEach(env.system.events, (e) =>
          Effect.sync(() => {
            events.push(e);
          }),
        ).pipe(Effect.forkScoped);
        const probe = yield* ActorTestKit.probe<{ value?: unknown; error?: string }>();
        yield* env.operate({
          operation: "task_create",
          id: "work",
          title: "work",
          instructions: "read only",
        });
        yield* env.operate({ operation: "task_execute", id: "work", revision: 1 });
        yield* until(() => approvalEntries(env.registry).length === 1);
        const run = Object.values(env.registry.snapshot()).find((r) =>
          r.path.startsWith("/goals/project/runs/"),
        )!.path;
        const invalid = yield* env.operate({ operation: "task_update", id: "work", revision: 42 });
        assert.match(invalid.error!, /revision/);
        fail = true;
        yield* env.goals.tell({
          _tag: "Route",
          slug: "project",
          command: {
            _tag: "Tool",
            request: {
              operation: "task_create",
              id: "failed",
              title: "failed",
              instructions: "read",
            },
            replyTo: probe.ref,
          },
        });
        yield* until(() =>
          events.some((e) => e._tag === "ActorRestarting" && e.path === "/user/goals/project"),
        );
        const response = yield* env.operate({ operation: "task_list" });
        assert.equal(response.error, undefined);
        assert.equal(
          events.filter((e) => e._tag === "ActorRestarting" && e.path === "/user/goals/project")
            .length,
          1,
        );
        assert.equal(
          events.some((e) => e._tag === "ActorStopped" && e.path === "/user/goals/project"),
          false,
        );
        assert.equal(
          Object.values(env.registry.snapshot()).filter((r) =>
            r.path.startsWith("/goals/project/runs/"),
          ).length,
          1,
        );
        assert.equal(
          (env.registry.get(run)!.state as { status: string }).status,
          "awaiting-confirmation",
        );
        yield* probe.expectNoMessage(10);
      }),
    ),
  );
});

test("old Goal callbacks and queued generation messages cannot mutate a restarted evaluation", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const plans: { input: Parameters<GoalReasoner["plan"]>[0]; cancelled: boolean }[] = [];
        let fail = false;
        const env = yield* setup(
          {
            plan: (input) =>
              Effect.suspend(() => {
                const pending = { input, cancelled: false };
                plans.push(pending);
                return Effect.never.pipe(
                  Effect.ensuring(
                    Effect.sync(() => {
                      pending.cancelled = true;
                    }),
                  ),
                );
              }),
          },
          {
            store: {
              loadAll: () => [],
              save: (r) => {
                if (fail && r.path === "/goals/project") {
                  fail = false;
                  throw new Error("restart");
                }
              },
            },
          },
        );
        yield* env.goals.tell({ _tag: "Initialize" });
        yield* until(() => plans.length === 1);
        fail = true;
        yield* env.goals.tell({
          _tag: "Route",
          slug: "project",
          command: { _tag: "UserMessage", text: "restart" },
        });
        yield* until(() => plans.length === 2);
        assert.equal(plans[0]!.cancelled, true);
        const lateTool = yield* Effect.exit(
          plans[0]!.input.tool!({
            operation: "task_create",
            id: "late",
            title: "late",
            instructions: "late",
          }),
        );
        const lateMessage = yield* Effect.exit(
          plans[0]!.input.onMessage!({
            role: "user",
            content: "stale transcript",
            timestamp: 0,
          }),
        );
        assert.ok(Exit.isFailure(lateTool));
        assert.ok(Cause.hasInterruptsOnly(lateTool.cause));
        assert.ok(Exit.isFailure(lateMessage));
        assert.ok(Cause.hasInterruptsOnly(lateMessage.cause));
        const stale = yield* env.goals.ask<{ error?: string }>((replyTo) => ({
          _tag: "Route",
          slug: "project",
          command: {
            _tag: "Tool",
            generation: "retired-generation",
            request: {
              operation: "task_create",
              id: "queued",
              title: "queued",
              instructions: "queued",
            },
            replyTo,
          },
        }));
        assert.match(stale.error!, /no longer active/);
        yield* env.goals.ask<void>((replyTo) => ({
          _tag: "Route",
          slug: "project",
          command: {
            _tag: "Compacted",
            generation: "retired-generation",
            summary: "stale",
            through: 99999,
            replyTo,
          },
        }));
        assert.deepEqual((yield* env.operate({ operation: "task_list" })).value, []);
        assert.notEqual(
          (env.registry.get("/goals/project")!.state as { summary: string }).summary,
          "stale",
        );
        assert.doesNotMatch(JSON.stringify(yield* env.history.read("project")), /stale transcript/);
      }),
    ),
  );
});

test("Run restart reattaches to a surviving delegation and delivers its saved result without resubmitting", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        let finish: ((value: ExecutionStatus) => void) | undefined;
        let submissions = 0,
          failed = false;
        const env = yield* setup(
          { plan: () => Effect.sync(() => plan) },
          {
            submit: () =>
              Effect.sync(() => {
                submissions++;
                return { sessionId: "one-session" };
              }),
            wait: () =>
              Effect.callback<ExecutionStatus>((resume) => {
                finish = (status) => resume(Effect.succeed(status));
              }),
            store: {
              loadAll: () => [],
              save: (record) => {
                if (
                  !failed &&
                  record.path.startsWith("/goals/project/runs/") &&
                  (record.state as { status?: string }).status === "completed"
                ) {
                  failed = true;
                  throw new Error("one Run result storage failure");
                }
              },
            },
          },
        );
        yield* env.operate({
          operation: "task_create",
          id: "work",
          title: "work",
          instructions: "read",
        });
        yield* env.operate({ operation: "task_execute", id: "work", revision: 1 });
        yield* until(() => approvalEntries(env.registry).length === 1);
        yield* env.approvals.ask((replyTo) => ({
          _tag: "Resolve",
          id: approvalEntries(env.registry)[0]!.id,
          response: { decision: "approve" },
          replyTo,
        }));
        yield* env.approvals.tell({ _tag: "Deliver" });
        yield* until(() => !!finish);
        finish!({ state: "completed", result: { text: "durable external result" } });
        yield* until(
          () =>
            (env.registry.get("/goals/project")!.state as { tasks: GoalTask[] }).tasks[0]?.execution
              ?.status === "completed",
        );
        assert.equal(failed, true);
        assert.equal(submissions, 1);
        assert.match(
          JSON.stringify(env.registry.get("/goals/project")!.state),
          /durable external result/,
        );
      }),
    ),
  );
});

test("Goal End interrupts in-flight reasoning without waiting for its callback", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        let entered = false,
          cancelled = false;
        const env = yield* setup({
          plan: () =>
            Effect.sync(() => {
              entered = true;
            }).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Effect.sync(() => {
                  cancelled = true;
                }),
              ),
            ),
        });
        yield* env.goals.tell({ _tag: "Initialize" });
        yield* until(() => entered);
        yield* env.goals.tell({ _tag: "Route", slug: "project", command: { _tag: "End" } });
        yield* until(
          () =>
            cancelled &&
            (env.registry.get("/goals/project")?.state as { status?: string } | undefined)
              ?.status === "completed",
        );
        assert.doesNotMatch(
          JSON.stringify(yield* env.history.read("project")),
          /Evaluation failed/,
        );
      }),
    ),
  );
});

test("failed compaction keeps the previous history boundary and does not start planning", async () => {
  const history = makeMemoryGoalHistory();
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        for (let i = 0; i < 4; i++)
          yield* history.append("project", {
            role: "user",
            content: "evidence".repeat(150),
            timestamp: i,
          });
        let plans = 0;
        const env = yield* setup(
          {
            plan: () =>
              Effect.sync(() => {
                plans++;
                return plan;
              }),
            compact: () =>
              Effect.fail(
                new GoalReasoningError({ operation: "compact", message: "compaction failed" }),
              ),
          },
          { history, contextTokens: 12000 },
        );
        yield* env.goals.tell({ _tag: "Initialize" });
        yield* until(
          () =>
            (env.registry.get("/goals/project")?.state as { lastError?: string } | undefined)
              ?.lastError === "compaction failed",
        );
        const state = env.registry.get("/goals/project")!.state as {
          historyThrough: number;
          summary: string;
        };
        assert.equal(state.historyThrough, 0);
        assert.equal(state.summary, "Not yet evaluated");
        assert.equal(plans, 0);
        assert.ok((yield* history.count("project")) >= 4);
      }),
    ),
  );
});

test("a reasoning defect enters supervision; an expected failure stays in Goal history", async () => {
  for (const broken of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const defect = new Error("reasoning invariant broken");
          let calls = 0;
          const env = yield* setup({
            plan: () =>
              Effect.suspend(() => {
                calls++;
                if (calls > 1) return Effect.succeed(plan);
                return broken
                  ? Effect.die(defect)
                  : Effect.fail(
                      new GoalReasoningError({
                        operation: "plan",
                        message: "provider unavailable",
                      }),
                    );
              }),
          });
          const restarts: unknown[] = [];
          yield* Stream.runForEach(env.system.events, (event) =>
            Effect.sync(() => {
              if (event._tag === "ActorRestarting") restarts.push(event);
            }),
          ).pipe(Effect.forkScoped);
          yield* env.goals.tell({ _tag: "Initialize" });
          if (broken) {
            yield* until(() => calls === 2);
            assert.equal(restarts.length, 1);
            assert.doesNotMatch(
              JSON.stringify(yield* env.history.read("project")),
              /Evaluation failed.*reasoning invariant/,
            );
          } else {
            yield* until(
              () =>
                (env.registry.get("/goals/project")?.state as { lastError?: string } | undefined)
                  ?.lastError === "provider unavailable",
            );
            assert.equal(restarts.length, 0);
            assert.match(
              JSON.stringify(yield* env.history.read("project")),
              /provider unavailable/,
            );
          }
        }),
      ),
    );
  }
});
