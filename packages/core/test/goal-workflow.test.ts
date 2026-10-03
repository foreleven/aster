import { TaskPreparationError } from "../src/index.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, ActorTestKit, type ActorRef } from "@aster/actor";
import { Cause, Clock, Deferred, Effect, Exit, Layer, Schema, Stream } from "effect";
import { ApplicationError, GoalDelivery, PersonalState } from "@aster/api-contracts";
import { TestClock } from "effect/testing";
import {
  ContextRegistry,
  makeContextRegistry,
  GoalRuntime,
  GoalReasoningError,
  GoalsRootActor,
  PersonalActions,
  PersonalProcessor,
  PersonalAgentActor,
  makeApplicationApi,
  type GoalDeliveryReply,
  SignalRootActor,
  SignalDefinitions,
  ExternalAgents,
  ApprovalQueueActor,
  approvalEntries,
  makeGoalRuntime,
  makeMemoryGoalHistory,
  type GoalReasoner,
  type GoalPlan,
  type GoalTask,
  type GoalToolRequest,
  type SignalRootCommand,
  type ContextStore,
  type ContextRecord,
  TaskPreparation,
  type Task,
  type ExecutionStatus,
  type ExternalAgent,
} from "../src/index.js";
import { preparationLayer, fakeAgent } from "./fixtures.js";
import { GoalState } from "../src/goals/state.js";
import type { GoalCommandReply } from "../src/goals/actors.js";

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
    personalActions?: PersonalActions["Service"];
    loseSignalAck?: boolean;
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
        options.personalActions
          ? Layer.succeed(PersonalActions, options.personalActions)
          : PersonalActions.unavailable,
        PersonalProcessor.disabled,
        Layer.succeed(Clock.Clock, clock),
        Layer.succeed(ContextRegistry, registry),
        Layer.succeed(GoalRuntime, {
          ...runtime,
          applySignal: (input, subscriber) =>
            runtime.applySignal!(input, subscriber).pipe(
              Effect.flatMap((receipt) =>
                options.loseSignalAck
                  ? Effect.fail(
                      new ApplicationError({
                        kind: "unavailable",
                        message: "Lost Signal acknowledgement",
                      }),
                    )
                  : Effect.succeed(receipt),
              ),
            ),
        }),
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

test("Goal rejects the whole Task proposal batch when a later revision conflicts", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* setup({
          durableSessions: true,
          plan: () =>
            Effect.succeed({
              ...plan,
              taskChanges: [
                { operation: "task_create", id: "review", title: "Review", instructions: "Review" },
                { operation: "task_update", id: "review", revision: 9, title: "Changed" },
              ],
            }),
        });
        yield* env.goals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
        yield* env.goals.tell({ _tag: "Initialize" });
        const state = () =>
          Schema.decodeUnknownSync(GoalState)(env.registry.get("/goals/project")!.state);
        yield* until(() => state().evaluations?.at(-1)?.status === "failed");
        assert.deepEqual(state().tasks, []);
        assert.equal(state().summary, "Not yet evaluated");
        assert.equal(state().pendingHandoff, undefined);
        assert.match(state().lastError!, /revision changed/);
        const rejected = state().evaluations!.at(-1)!;
        assert.equal(rejected.status, "failed");
        if (rejected.status === "failed") assert.equal(rejected.result?.taskChanges?.length, 2);
        assert.equal(
          Object.keys(env.registry.snapshot()).some((path) => path.includes("/runs/")),
          false,
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Task result reservations survive lost Goal commit acknowledgement and create one Run requiring confirmation", async () => {
  const records = new Map<string, ContextRecord>();
  let fail = true;
  let generations = 0;
  const proposal: GoalPlan = {
    ...plan,
    taskChanges: [
      { operation: "task_create", id: "review", title: "Review", instructions: "Review evidence" },
      { operation: "task_execute", id: "review", revision: 1 },
    ],
  };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* setup(
          {
            durableSessions: true,
            plan: () =>
              Effect.sync(() => {
                generations++;
                return proposal;
              }),
          },
          {
            store: {
              loadAll: () => [...records.values()],
              save: (record) => {
                records.set(record.path, structuredClone(record));
                if (record.path !== "/goals/project") return;
                const state = Schema.decodeUnknownSync(GoalState)(record.state);
                if (state.tasks.length) {
                  assert.equal(state.summary, plan.progress);
                  assert.equal(state.evaluations?.[0].status, "completed");
                  assert.ok(state.tasks[0].execution?.runPath);
                  if (fail) {
                    fail = false;
                    throw new Error("Lost result acknowledgement before Run creation");
                  }
                }
              },
            },
          },
        );
        yield* env.goals.tell({ _tag: "Initialize" });
        yield* until(() => approvalEntries(env.registry).length === 1);
        const runs = Object.values(env.registry.snapshot()).filter((record) =>
          record.path.startsWith("/goals/project/runs/"),
        );
        assert.equal(runs.length, 1);
        assert.equal((runs[0].state as { status: string }).status, "awaiting-confirmation");
        const state = Schema.decodeUnknownSync(GoalState)(
          env.registry.get("/goals/project")!.state,
        );
        assert.equal(state.tasks[0].execution?.runPath, runs[0].path);
        assert.equal(state.evaluations?.length, 1);
        assert.equal(generations, 1);
        assert.equal(state.pendingHandoff, undefined);
        const api = makeApplicationApi({
          registry: env.registry,
          goals: env.goals,
          inspect: Effect.succeed(null),
        });
        const timeline = yield* api.goals.timeline("project");
        assert.equal(timeline.groups[0].outputs.length, 2);
        assert.equal(timeline.groups[0].outputs[1].runPath, runs[0].path);
        assert.equal(timeline.groups[0].outputs[1].status, "applied");
        assert.equal(state.tasks[0].execution?.evaluationId, timeline.groups[0].evaluationId);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

for (const running of [false, true]) {
  test(`Task result updates ${running ? "preserve started execution" : "revoke pending confirmation"}`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          let plans = 0;
          const executing = yield* Deferred.make<void>();
          const env = yield* setup(
            {
              durableSessions: true,
              plan: () =>
                Effect.suspend(() =>
                  ++plans === 1
                    ? Effect.succeed({
                        ...plan,
                        taskChanges: [
                          {
                            operation: "task_update",
                            id: "review",
                            revision: 1,
                            instructions: "Revised evidence",
                          },
                        ],
                      })
                    : Effect.never,
                ),
            },
            {
              wait: () => Deferred.succeed(executing, undefined).pipe(Effect.andThen(Effect.never)),
            },
          );
          yield* env.operate({
            operation: "task_create",
            id: "review",
            title: "Review",
            instructions: "Original evidence",
          });
          yield* env.operate({ operation: "task_execute", id: "review", revision: 1 });
          yield* until(() => approvalEntries(env.registry).length === 1);
          const approval = approvalEntries(env.registry)[0];
          const runPath = Schema.decodeUnknownSync(GoalState)(
            env.registry.get("/goals/project")!.state,
          ).tasks[0].execution!.runPath;
          if (running) {
            yield* env.approvals.ask((replyTo) => ({
              _tag: "Resolve",
              id: approval.id,
              response: { decision: "approve" },
              replyTo,
            }));
            yield* env.approvals.tell({ _tag: "Deliver" });
            yield* Deferred.await(executing);
          }
          yield* env.goals.tell({ _tag: "Initialize" });
          yield* until(
            () =>
              Schema.decodeUnknownSync(GoalState)(env.registry.get("/goals/project")!.state)
                .tasks[0].revision === 2,
          );
          if (!running) yield* until(() => approvalEntries(env.registry)[0].status === "revoked");
          const run = env.registry.get(runPath)!.state as { status: string; task: Task };
          assert.equal(run.status, running ? "running" : "cancelled");
          assert.equal(run.task.instructions, "Original evidence");
          assert.equal(
            Object.keys(env.registry.snapshot()).filter((path) =>
              path.startsWith("/goals/project/runs/"),
            ).length,
            1,
          );
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
}

for (const outcome of ["failed", "unknown"] as const) {
  test(`Goal ${outcome} evaluations retain structured evidence and the correct retry identity`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const inputs: Parameters<GoalReasoner["plan"]>[0][] = [];
          const env = yield* setup({
            durableSessions: true,
            plan: (input) =>
              Effect.suspend(() => {
                inputs.push(input);
                return inputs.length === 1
                  ? Effect.fail(
                      new GoalReasoningError({
                        operation: "plan",
                        outcome,
                        message: "Retained failure",
                      }),
                    )
                  : Effect.succeed(plan);
              }),
          });
          yield* env.goals.tell({ _tag: "Initialize" });
          yield* env.goals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
          const state = () =>
            Schema.decodeUnknownSync(GoalState)(env.registry.get("/goals/project")!.state);
          yield* until(() => state().lastError === "Retained failure");
          const first = inputs[0].durable!.requestId;
          assert.equal(
            state().evaluations?.[0].status,
            outcome === "failed" ? "failed" : "reconciliation_required",
          );
          assert.equal(state().pendingRequestId, outcome === "failed" ? undefined : first);
          yield* env.goals.tell({
            _tag: "Route",
            slug: "project",
            command: { _tag: "Evaluate", reason: "Retry admitted work" },
          });
          yield* until(() => state().evaluations?.at(-1)?.status === "completed");
          const completed = state().evaluations!.at(-1)!;
          assert.equal(inputs.length, 2);
          assert.equal(inputs[1].durable!.requestId === first, outcome === "unknown");
          assert.equal(completed.retryOf, outcome === "failed" ? first : undefined);
          assert.deepEqual(completed.inputIds, state().evaluations![0].inputIds);
          assert.equal(state().inputs?.length, 1);
          assert.equal(state().evaluations!.length, outcome === "failed" ? 2 : 1);
          assert.equal(state().summary, plan.progress);
          assert.equal(state().pendingRequestId, undefined);
          assert.equal(completed.status, "completed");
          if (completed.status === "completed") {
            assert.deepEqual(completed.result, plan);
            assert.equal(completed.resultId, inputs[1].durable!.requestId);
          }
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
}

for (const committed of [false, true]) {
  test(`Goal result application recovers ${committed ? "lost acknowledgement" : "rejected persistence"} without another model result`, async () => {
    const records = new Map<string, ContextRecord>();
    const results = new Map<string, typeof plan>();
    let fail = true;
    let generations = 0;
    const store: ContextStore = {
      loadAll: () => [...records.values()],
      save: (record) => {
        const applied =
          record.path === "/goals/project" &&
          Schema.decodeUnknownSync(GoalState)(record.state).evaluations?.at(-1)?.status ===
            "completed";
        if (applied && fail) {
          fail = false;
          if (committed) records.set(record.path, structuredClone(record));
          throw new Error("Result persistence interrupted");
        }
        records.set(record.path, structuredClone(record));
      },
    };
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* setup(
            {
              durableSessions: true,
              plan: (input) =>
                Effect.sync(() => {
                  const id = input.durable!.requestId;
                  if (!results.has(id)) {
                    generations++;
                    results.set(id, plan);
                  }
                  return results.get(id)!;
                }),
            },
            { store },
          );
          yield* env.goals.tell({ _tag: "Initialize" });
          yield* until(() => {
            const record = records.get("/goals/project");
            return (
              !!record &&
              Schema.decodeUnknownSync(GoalState)(record.state).evaluations?.at(-1)?.status ===
                "completed"
            );
          });
          // A mailbox query crosses the recovery boundary after the failed commit.
          yield* env.operate({ operation: "task_list" });
          const state = Schema.decodeUnknownSync(GoalState)(
            env.registry.get("/goals/project")!.state,
          );
          assert.equal(state.summary, plan.progress);
          assert.equal(state.pendingHandoff, undefined);
          assert.equal(state.evaluations?.length, 1);
          assert.equal(state.evaluations?.[0].status, "completed");
          assert.equal(generations, 1);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
}

test("Goal Task feedback consumes its frozen causal budget and restart does not replenish it", async () => {
  const records = new Map<string, ContextRecord>();
  const history = makeMemoryGoalHistory();
  let calls = 0;
  const reasoner: GoalReasoner = {
    plan: (input) =>
      Effect.gen(function* () {
        calls++;
        const id = `work-${calls}`;
        yield* input.tool!({
          operation: "task_create",
          id,
          title: `Review ${calls}`,
          instructions: "Review the evidence",
        }).pipe(Effect.orDie);
        yield* input.tool!({
          operation: "signal_create",
          id,
          definition: { when: `Evidence for ${id} changes`, task: "Read the evidence" },
        }).pipe(Effect.orDie);
        yield* input.tool!({ operation: "task_execute", id, revision: 1 }).pipe(Effect.orDie);
        return { ...plan, progress: `Reviewed evidence ${calls}` };
      }),
  };
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* setup(reasoner, {
            history,
            store: {
              loadAll: () => [...records.values()],
              save: (record) => {
                records.set(record.path, structuredClone(record));
              },
            },
            prepare: () => Promise.reject(new Error("No executable evidence")),
            submit: () => Effect.die(new Error("Unprepared work must never reach an executor")),
          });
          const changes = yield* env.registry.subscribe;
          const current = () =>
            Schema.decodeUnknownSync(GoalState)(env.registry.get("/goals/project")!.state);
          const waitForLimit = () => {
            const exhausted = () => current().lastError?.includes("reached its limit") === true;
            return exhausted()
              ? Effect.void
              : changes.pipe(
                  Stream.filter(() => exhausted()),
                  Stream.take(1),
                  Stream.runDrain,
                );
          };
          yield* env.goals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
          if (!restart) {
            yield* env.goals.tell({ _tag: "Initialize" });
            yield* waitForLimit();
          }
          assert.equal(calls, 4);
          assert.equal(current().causal?.remainingAgentTurns, 0);
          assert.equal(current().agentAdmissions?.[0]?.count, 4);
          const runs = Object.values(env.registry.snapshot()).filter((record) =>
            record.path.startsWith("/goals/project/runs/"),
          );
          const causalRun = Schema.Struct({
            causal: Schema.Struct({ remainingAgentTurns: Schema.Number }),
          });
          assert.deepEqual(
            runs
              .map(
                (record) =>
                  Schema.decodeUnknownSync(causalRun)(record.state).causal.remainingAgentTurns,
              )
              .sort(),
            [0, 1, 2, 3],
          );
          const signals = Object.values(env.registry.snapshot()).filter((record) =>
            /^\/signals\/project--/.test(record.path),
          );
          assert.deepEqual(
            signals
              .map(
                (record) =>
                  Schema.decodeUnknownSync(causalRun)(record.state).causal.remainingAgentTurns,
              )
              .sort(),
            [0, 1, 2, 3],
          );
          if (restart) {
            const priorRoot = current().causal!.rootRequestId;
            yield* env.goals.ask<GoalCommandReply>((replyTo) => ({
              _tag: "Route",
              slug: "project",
              command: {
                _tag: "UserMessage",
                text: "Continue the review with new evidence",
                replyTo,
              },
            }));
            yield* changes.pipe(
              Stream.filter(
                () => calls === 8 && current().lastError?.includes("reached its limit") === true,
              ),
              Stream.take(1),
              Stream.runDrain,
            );
            assert.notEqual(current().causal!.rootRequestId, priorRoot);
            assert.equal(current().agentAdmissions?.length, 2);
          }
        }),
      ).pipe(Effect.timeout("10 seconds")),
    );
  }
});

test("Personal outbox recovers a lost Goal acknowledgement without duplicating its input or history", async () => {
  const records = new Map<string, ContextRecord>();
  const history = makeMemoryGoalHistory();
  let sends = 0;
  let request:
    Parameters<ReturnType<typeof makeApplicationApi>["personal"]["sendGoalMessage"]>[0] | undefined;
  let acceptedRevision = 0;
  for (let restart = 0; restart < 2; restart++) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* setup(
            { plan: () => Effect.never },
            {
              history,
              store: {
                loadAll: () => [...records.values()],
                save: (record) => {
                  records.set(record.path, structuredClone(record));
                },
              },
              personalActions: {
                executors: Effect.succeed([]),
                applySignal: () => Effect.die(new Error("Unexpected Signal command")),
                resumeRun: () => Effect.die(new Error("Unexpected Run command")),
                startTask: () => Effect.die(new Error("Unexpected Task command")),
                requestApproval: () => Effect.die(new Error("Unexpected approval request")),
                respondApproval: () => Effect.die(new Error("Unexpected Approval command")),
                bind: () => Effect.succeed(true),
                sendGoalMessage: (input) => send(input),
              },
            },
          );
          // The real receiving Actor persists and projects this command before the
          // test transport drops its first acknowledgement.
          const send: PersonalActions["Service"]["sendGoalMessage"] = (input) =>
            Effect.gen(function* () {
              sends++;
              assert.ok(
                Schema.decodeUnknownSync(PersonalState)(
                  records.get("/personal")!.state,
                ).outbox?.some((item) => item.input.requestId === input.requestId),
              );
              const reply = yield* env.goals
                .ask<GoalDeliveryReply>((replyTo) => ({
                  _tag: "Route",
                  slug: "project",
                  command: { _tag: "Deliver", input, replyTo },
                }))
                .pipe(Effect.orDie);
              if (reply._tag === "Rejected") return yield* reply.error;
              if (sends === 1)
                return yield* new ApplicationError({
                  kind: "unavailable",
                  message: "Injected lost acknowledgement",
                });
              return reply.receipt;
            });
          // Wait for startup metadata/reconciliation writes before selecting a CAS revision.
          yield* env.operate({ operation: "task_list" });
          const personal = yield* env.system.spawn("personal", PersonalAgentActor);
          const api = makeApplicationApi({
            registry: env.registry,
            personal,
            inspect: Effect.succeed(null),
          });
          const changes = yield* env.registry.subscribe;
          if (!request) {
            const current = yield* api.personal.get;
            request = {
              requestId: "send-release",
              causationId: "user-release",
              expectedRevision: current.revision!,
              goalSlug: "project",
              goalRevision: env.registry.get("/goals/project")!.revision!,
              text: "Focus on the release blocker",
            };
            acceptedRevision = (yield* api.personal.sendGoalMessage(request)).revision;
          }
          const status = restart === 0 ? "unknown" : "delivered";
          yield* changes.pipe(
            Stream.filter(
              (change) =>
                change.path === "/personal" &&
                Schema.decodeUnknownSync(PersonalState)(change.record.state).outbox?.[0]?.status ===
                  status,
            ),
            Stream.take(1),
            Stream.runDrain,
          );
          if (restart > 0)
            assert.equal((yield* api.personal.sendGoalMessage(request)).revision, acceptedRevision);
          const goal = env.registry.get("/goals/project")!;
          const deliveries = Schema.decodeUnknownSync(Schema.Array(GoalDelivery))(
            (goal.state as { deliveries: unknown }).deliveries,
          );
          assert.equal(deliveries.length, 1);
          const entries = yield* history.read("project", { limit: 100 });
          assert.equal(entries.filter((entry) => entry.requestId).length, 1);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
  assert.equal(sends, 2);
});

test("Goal inbox recovers after history append but before projection acknowledgement and rejects conflicting deliveries", async () => {
  const records = new Map<string, ContextRecord>();
  let failProjection = true;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* setup(
          { plan: () => Effect.never },
          {
            store: {
              loadAll: () => [...records.values()],
              save: (record) => {
                const deliveries = (record.state as { deliveries?: { historySequence?: number }[] })
                  .deliveries;
                if (
                  record.path === "/goals/project" &&
                  deliveries?.[0]?.historySequence !== undefined &&
                  failProjection
                ) {
                  failProjection = false;
                  throw new Error("Injected projection acknowledgement loss");
                }
                records.set(record.path, structuredClone(record));
              },
            },
          },
        );
        yield* env.operate({ operation: "task_list" });
        const input = {
          requestId: "inbox-one",
          causationId: "user-one",
          source: "/personal" as const,
          target: "/goals/project",
          expectedRevision: env.registry.get("/goals/project")!.revision!,
          createdAt: "2026-10-02T00:00:00.000Z",
          text: "Review the blocker",
        };
        const deliver = (value = input) =>
          env.goals.ask<GoalDeliveryReply>((replyTo) => ({
            _tag: "Route",
            slug: "project",
            command: { _tag: "Deliver", input: value, replyTo },
          }));
        const changes = yield* env.registry.subscribe;
        yield* deliver().pipe(Effect.forkScoped);
        yield* changes.pipe(
          Stream.filter(
            (change) =>
              change.path === "/goals/project" &&
              (change.record.state as { deliveries?: { historySequence?: number }[] })
                .deliveries?.[0]?.historySequence !== undefined,
          ),
          Stream.take(1),
          Stream.runDrain,
        );
        const replay = yield* deliver();
        assert.equal(replay._tag, "Accepted");
        const conflict = yield* deliver({ ...input, text: "Changed content" });
        assert.ok(conflict._tag === "Rejected" && conflict.error.kind === "conflict");
        const stale = yield* deliver({ ...input, requestId: "new-stale-input" });
        assert.ok(stale._tag === "Rejected" && stale.error.kind === "conflict");
        const messages = yield* env.history.read("project", { limit: 100 });
        assert.equal(messages.filter((entry) => entry.requestId).length, 1);
        const deliveries = (env.registry.get("/goals/project")!.state as { deliveries: unknown[] })
          .deliveries;
        assert.equal(deliveries.length, 1);
        assert.equal(failProjection, false);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("a durable Goal applies its answer and consumed cursor in one Context commit", async () => {
  const records = new Map<string, ContextRecord>();
  const commits: ContextRecord[] = [];
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* setup(
          {
            durableSessions: true,
            plan: () => Effect.succeed(plan),
          },
          {
            store: {
              loadAll: () => [...records.values()],
              save: (record) => {
                const saved = structuredClone(record);
                records.set(saved.path, saved);
                if (saved.path === "/goals/project") commits.push(saved);
              },
            },
          },
        );
        const changes = yield* env.registry.subscribe;
        yield* env.goals.tell({ _tag: "Initialize" });
        yield* changes.pipe(
          Stream.filter(
            (change) =>
              change.path === "/goals/project" &&
              (change.record.state as { summary?: string }).summary === plan.progress,
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.timeout("5 seconds"),
        );
        const applied = commits.filter(
          (record) => (record.state as { agentThrough?: number }).agentThrough! > 0,
        );
        assert.ok(applied.length > 0);
        for (const record of applied) {
          const state = record.state as { summary: string; pendingRequestId?: string };
          assert.equal(state.summary, plan.progress);
          assert.equal(state.pendingRequestId, undefined);
        }
        assert.ok(
          commits.some(
            (record) =>
              typeof (record.state as { pendingRequestId?: string }).pendingRequestId === "string",
          ),
        );
      }),
    ),
  );
});

test("a recovered durable Goal replays only its frozen input prefix before consuming later input", async () => {
  const records = new Map<string, ContextRecord>();
  const history = makeMemoryGoalHistory();
  const store: ContextStore = {
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.path, structuredClone(record));
    },
  };
  type Input = Parameters<GoalReasoner["plan"]>[0];
  await Effect.runPromise(
    Effect.gen(function* () {
      const firstStarted = yield* Deferred.make<Input>();
      const original = yield* Effect.scoped(
        Effect.gen(function* () {
          const env = yield* setup(
            {
              durableSessions: true,
              plan: (input) =>
                Deferred.succeed(firstStarted, input).pipe(Effect.andThen(Effect.never)),
            },
            { store, history },
          );
          yield* env.goals.tell({ _tag: "Initialize" });
          const input = yield* Deferred.await(firstStarted);
          const accepted = yield* env.goals.ask<GoalCommandReply>((replyTo) => ({
            _tag: "Route",
            slug: "project",
            command: { _tag: "UserMessage", text: "New input received after the handoff", replyTo },
          }));
          assert.equal(accepted._tag, "Accepted");
          const state = Schema.decodeUnknownSync(GoalState)(records.get("/goals/project")!.state);
          assert.equal(state.pendingRequestId, input.durable!.requestId);
          assert.deepEqual(state.pendingHandoff?.input?.current, input.current);
          assert.deepEqual(state.pendingHandoff?.input?.contexts, input.contexts);
          assert.equal(state.pendingEvaluation, true);
          return input;
        }),
      );
      const recoveredStarted = yield* Deferred.make<Input>();
      const nextStarted = yield* Deferred.make<Input>();
      yield* Effect.scoped(
        Effect.gen(function* () {
          let calls = 0;
          const env = yield* setup(
            {
              durableSessions: true,
              plan: (input) =>
                Effect.gen(function* () {
                  calls++;
                  if (calls === 1) {
                    yield* Deferred.succeed(recoveredStarted, input);
                    return plan;
                  }
                  yield* Deferred.succeed(nextStarted, input);
                  return yield* Effect.never;
                }),
            },
            { store, history },
          );
          const recovered = yield* Deferred.await(recoveredStarted);
          assert.equal(recovered.durable!.requestId, original.durable!.requestId);
          assert.equal(recovered.reason, original.reason);
          assert.deepEqual(recovered.messages, original.messages);
          assert.deepEqual(recovered.current, original.current);
          assert.deepEqual(recovered.contexts, original.contexts);
          assert.deepEqual(recovered.signals, original.signals);
          assert.deepEqual(recovered.goal, original.goal);
          const next = yield* Deferred.await(nextStarted);
          assert.notEqual(next.durable!.requestId, original.durable!.requestId);
          assert.ok(
            next.messages?.some(
              (message) =>
                message.role === "user" &&
                message.content === "New input received after the handoff",
            ),
          );
          assert.equal(
            Schema.decodeUnknownSync(GoalState)(env.registry.get("/goals/project")!.state).summary,
            plan.progress,
          );
          assert.equal(calls, 2);
        }),
      );
    }).pipe(Effect.timeout("5 seconds")),
  );
});

test("durable compaction cannot absorb inputs received after its frozen handoff", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const history = makeMemoryGoalHistory();
        for (let index = 0; index < 205; index++)
          yield* history.append("project", {
            role: "user",
            content: `Original input ${index}`,
            timestamp: 1,
          });
        const compacting = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const nextStarted = yield* Deferred.make<void>();
        let calls = 0;
        const env = yield* setup(
          {
            durableSessions: true,
            compact: (_summary, messages) =>
              Effect.gen(function* () {
                assert.ok(
                  !messages.some(
                    (message) => message.role === "user" && message.content === "Later input",
                  ),
                );
                yield* Deferred.succeed(compacting, undefined);
                yield* Deferred.await(release);
                return "Compacted original inputs";
              }),
            plan: (input) =>
              Effect.gen(function* () {
                calls++;
                const containsLater = input.messages?.some(
                  (message) => message.role === "user" && message.content === "Later input",
                );
                if (calls === 1) {
                  assert.equal(containsLater, false);
                  return plan;
                }
                assert.equal(containsLater, true);
                yield* Deferred.succeed(nextStarted, undefined);
                return yield* Effect.never;
              }),
          },
          { history },
        );
        yield* env.goals.tell({ _tag: "Initialize" });
        yield* Deferred.await(compacting);
        yield* env.goals.ask<GoalCommandReply>((replyTo) => ({
          _tag: "Route",
          slug: "project",
          command: { _tag: "UserMessage", text: "Later input", replyTo },
        }));
        yield* Deferred.succeed(release, undefined);
        yield* Deferred.await(nextStarted);
        assert.equal(calls, 2);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("legacy unresolved Goal handoffs are fenced instead of guessing a new input range", async () => {
  const record: ContextRecord = {
    path: "/goals/project",
    description: "Observe the project",
    revision: 1,
    messages: [],
    state: {
      slug: "project",
      description: "Observe the project",
      status: "active",
      summary: "Previous summary",
      progress: "Previous summary",
      tasks: [],
      historyThrough: 0,
      historyCount: 0,
      agentThrough: 0,
      pendingEvaluation: true,
      pendingRequestId: "legacy-request",
      receivedEvents: [],
    },
  };
  const records = new Map([[record.path, record]]);
  let calls = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* setup(
          {
            durableSessions: true,
            plan: () =>
              Effect.sync(() => {
                calls++;
                return plan;
              }),
          },
          {
            store: {
              loadAll: () => [...records.values()],
              save: (saved) => {
                records.set(saved.path, structuredClone(saved));
              },
            },
          },
        );
        const changes = yield* env.registry.subscribe;
        const state = () =>
          Schema.decodeUnknownSync(GoalState)(env.registry.get(record.path)!.state);
        if (!state().lastError)
          yield* changes.pipe(
            Stream.filter(
              (change) =>
                change.path === record.path &&
                !!Schema.decodeUnknownSync(GoalState)(change.record.state).lastError,
            ),
            Stream.take(1),
            Stream.runDrain,
          );
        assert.match(state().lastError!, /frozen input range; reconciliation required/);
        assert.equal(state().pendingRequestId, "legacy-request");
        assert.equal(state().agentThrough, 0);
        assert.equal(calls, 0);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

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
        const durable = new Map<string, ContextRecord>();
        const env = yield* setup(
          { plan: () => Effect.sync(() => plan) },
          {
            store: {
              loadAll: () => [...durable.values()],
              save: (record) => {
                if (fail && record.path === "/goals/project") {
                  fail = false;
                  throw new Error("one disk failure");
                }
                durable.set(record.path, structuredClone(record));
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
        const durable = new Map<string, ContextRecord>();
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
              loadAll: () => [...durable.values()],
              save: (r) => {
                if (fail && r.path === "/goals/project") {
                  fail = false;
                  throw new Error("restart");
                }
                durable.set(r.path, structuredClone(r));
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
        const durable = new Map<string, ContextRecord>();
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
              loadAll: () => [...durable.values()],
              save: (record) => {
                if (
                  !failed &&
                  record.path.startsWith("/goals/project/runs/") &&
                  (record.state as { status?: string }).status === "completed"
                ) {
                  failed = true;
                  throw new Error("one Run result storage failure");
                }
                durable.set(record.path, structuredClone(record));
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

test("Goal Signal proposals commit with Tasks and reconcile a lost receipt after restart without new reasoning", async () => {
  const records = new Map<string, ContextRecord>();
  const history = makeMemoryGoalHistory();
  let generations = 0;
  const reasoner: GoalReasoner = {
    durableSessions: true,
    plan: () =>
      Effect.sync(() => {
        generations++;
        return {
          ...plan,
          taskChanges: [
            {
              operation: "task_create" as const,
              id: "review",
              title: "Review",
              instructions: "Inspect",
            },
          ],
          signalChanges: [
            {
              operation: "signal_create" as const,
              id: "watch",
              definition: { taskId: "review", when: "Blockers change", task: "Review blockers" },
            },
          ],
        };
      }),
  };
  const store: ContextStore = {
    loadAll: () => [...records.values()],
    save: (record) => {
      if (record.path === "/signals/project--watch") {
        const goal = Schema.decodeUnknownSync(GoalState)(records.get("/goals/project")!.state);
        assert.equal(goal.tasks[0]?.id, "review");
        assert.equal(goal.summary, plan.progress);
        assert.equal(goal.signalOutbox?.[0].status, "sending");
        assert.equal(goal.evaluations?.[0].status, "partially_applied");
      }
      records.set(record.path, structuredClone(record));
    },
  };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* setup(reasoner, { store, history, loseSignalAck: true });
        const changes = yield* env.registry.subscribe;
        yield* env.goals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
        yield* env.goals.tell({ _tag: "Initialize" });
        const state = () =>
          Schema.decodeUnknownSync(GoalState)(env.registry.get("/goals/project")!.state);
        if (state().signalOutbox?.[0]?.status !== "unknown")
          yield* changes.pipe(
            Stream.filter(() => state().signalOutbox?.[0]?.status === "unknown"),
            Stream.take(1),
            Stream.runDrain,
          );
        assert.equal(state().evaluations?.[0].status, "partially_applied");
        assert.equal(state().signalOutbox?.[0].attempts, 1);
        const timeline = yield* makeApplicationApi({
          registry: env.registry,
          goals: env.goals,
          inspect: Effect.succeed(null),
        }).goals.timeline("project");
        assert.equal(timeline.groups[0].status, "partially_applied");
        assert.equal(
          timeline.groups[0].outputs.find((output) => output.kind === "signal")?.status,
          "unknown",
        );
        const projected = JSON.stringify(env.registry.project(env.registry.get("/goals/project")!));
        assert.equal(projected.includes('"remainingAgentTurns"'), false);
        assert.equal(projected.includes('"expectedRevision"'), false);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
  const signal = structuredClone(records.get("/signals/project--watch")!);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* setup(reasoner, { store, history });
        const changes = yield* env.registry.subscribe;
        yield* env.goals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
        const state = () =>
          Schema.decodeUnknownSync(GoalState)(env.registry.get("/goals/project")!.state);
        if (state().signalOutbox?.[0]?.status !== "delivered")
          yield* changes.pipe(
            Stream.filter(() => state().signalOutbox?.[0]?.status === "delivered"),
            Stream.take(1),
            Stream.runDrain,
          );
        assert.equal(generations, 1);
        assert.equal(state().evaluations?.[0].status, "completed");
        assert.equal(state().signalOutbox?.[0].attempts, 2);
        assert.equal(state().signalOutbox?.[0].receipt?.revision, 1);
        assert.deepEqual(env.registry.get(signal.path), signal);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Invalid Signal proposal rejects the entire Goal result before applying its Task changes", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* setup({
          durableSessions: true,
          plan: () =>
            Effect.succeed({
              ...plan,
              taskChanges: [
                {
                  operation: "task_create",
                  id: "review",
                  title: "Review",
                  instructions: "Inspect",
                },
              ],
              signalChanges: [
                { operation: "signal_create", id: "watch", definition: { taskId: "missing" } },
              ],
            }),
        });
        const changes = yield* env.registry.subscribe;
        yield* env.goals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
        yield* env.goals.tell({ _tag: "Initialize" });
        const state = () =>
          Schema.decodeUnknownSync(GoalState)(env.registry.get("/goals/project")!.state);
        if (state().evaluations?.[0]?.status !== "failed")
          yield* changes.pipe(
            Stream.filter(() => state().evaluations?.[0]?.status === "failed"),
            Stream.take(1),
            Stream.runDrain,
          );
        assert.deepEqual(state().tasks, []);
        assert.deepEqual(state().signalOutbox ?? [], []);
        assert.equal(state().summary, "Not yet evaluated");
        assert.match(state().lastError!, /missing or deleted/);
        assert.equal(env.registry.get("/signals/project--watch"), undefined);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Signal outbox survives lost Goal result acknowledgement and stops replay after three unknown sends", async () => {
  const records = new Map<string, ContextRecord>();
  const history = makeMemoryGoalHistory();
  let generations = 0;
  let loseCommit = true;
  const reasoner: GoalReasoner = {
    durableSessions: true,
    plan: () =>
      Effect.sync(() => {
        generations++;
        return {
          ...plan,
          signalChanges: [
            {
              operation: "signal_create" as const,
              id: "watch",
              definition: { when: "Changed", task: "Review" },
            },
          ],
        };
      }),
  };
  const store: ContextStore = {
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.path, structuredClone(record));
      if (
        record.path === "/goals/project" &&
        loseCommit &&
        Schema.decodeUnknownSync(GoalState)(record.state).signalOutbox?.length
      ) {
        loseCommit = false;
        throw new Error("Lost result acknowledgement before Signal dispatch");
      }
    },
  };
  for (const attempt of [1, 2, 3, 3]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* setup(reasoner, { store, history, loseSignalAck: true });
          const changes = yield* env.registry.subscribe;
          yield* env.goals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
          if (generations === 0) yield* env.goals.tell({ _tag: "Initialize" });
          const state = () =>
            Schema.decodeUnknownSync(GoalState)(env.registry.get("/goals/project")!.state);
          const done = () =>
            state().signalOutbox?.[0]?.status === "unknown" &&
            state().signalOutbox?.[0]?.attempts === attempt;
          if (!done()) yield* changes.pipe(Stream.filter(done), Stream.take(1), Stream.runDrain);
          assert.equal(state().evaluations?.[0]?.status, "partially_applied");
          assert.equal(env.registry.get("/signals/project--watch")?.revision, 1);
          assert.equal(generations, 1);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
  const state = Schema.decodeUnknownSync(GoalState)(records.get("/goals/project")!.state);
  assert.equal(state.signalOutbox?.[0]?.attempts, 3);
  const operation = state.signalOutbox![0];
  const retryInput = {
    slug: "project",
    operationId: operation.input.requestId,
    requestId: "operator-retry",
    expectedAttempts: 3,
  };
  let loseRetryAck = true;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* setup(reasoner, {
          history,
          store: {
            loadAll: store.loadAll,
            save: (record) => {
              store.save(record);
              if (
                record.path === "/goals/project" &&
                loseRetryAck &&
                Schema.decodeUnknownSync(GoalState)(record.state).signalOutbox?.[0]?.retries?.length
              ) {
                loseRetryAck = false;
                throw new Error("Lost retry authorization acknowledgement");
              }
            },
          },
        });
        yield* env.goals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
        const reply =
          yield* ActorTestKit.probe<import("../src/goals/actors.js").GoalDeliveryReply>();
        const changes = yield* env.registry.subscribe;
        yield* env.goals.tell({
          _tag: "Route",
          slug: "project",
          command: { _tag: "RetrySignal", input: retryInput, replyTo: reply.ref },
        });
        const current = () =>
          Schema.decodeUnknownSync(GoalState)(env.registry.get("/goals/project")!.state);
        const done = () => current().signalOutbox?.[0]?.status === "delivered";
        if (!done()) yield* changes.pipe(Stream.filter(done), Stream.take(1), Stream.runDrain);
        const api = makeApplicationApi({
          registry: env.registry,
          goals: env.goals,
          inspect: Effect.succeed(null),
        });
        const receipt = yield* api.goals.retrySignal(retryInput);
        assert.deepEqual(receipt, current().signalOutbox![0].retries![0].receipt);
        assert.deepEqual(yield* api.goals.retrySignal(retryInput), receipt);
        assert.equal(
          (yield* api.goals.retrySignal({ ...retryInput, expectedAttempts: 4 }).pipe(Effect.flip))
            .kind,
          "conflict",
        );
        assert.equal(
          (yield* api.goals
            .retrySignal({ ...retryInput, requestId: "stale-operator" })
            .pipe(Effect.flip)).kind,
          "conflict",
        );
        assert.equal(current().signalOutbox![0].attempts, 4);
        assert.equal(current().signalOutbox![0].retries!.length, 1);
        assert.equal(current().evaluations![0].status, "completed");
        assert.deepEqual(current().signalOutbox![0].input, operation.input);
        assert.equal(env.registry.get("/signals/project--watch")?.revision, 1);
        assert.equal(generations, 1);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Goal Timeline freezes ordered inputs, keeps pending arrivals separate and retains ignored conclusions", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = [yield* Deferred.make<void>(), yield* Deferred.make<void>()];
        const release = [yield* Deferred.make<void>(), yield* Deferred.make<void>()];
        const calls: Parameters<GoalReasoner["plan"]>[0][] = [];
        const env = yield* setup({
          durableSessions: true,
          plan: (input) =>
            Effect.gen(function* () {
              const index = calls.length;
              calls.push(input);
              yield* Deferred.succeed(entered[index]!, undefined);
              yield* Deferred.await(release[index]!);
              return {
                ...plan,
                disposition: index === 0 ? ("ignored" as const) : ("no_change" as const),
                progress:
                  index === 0
                    ? "This evidence is outside the release scope"
                    : "Noted the new requirements",
              };
            }),
        });
        yield* env.goals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
        const api = makeApplicationApi({
          registry: env.registry,
          goals: env.goals,
          history: env.history,
          inspect: Effect.succeed(null),
        });
        yield* api.goals.sendMessage("project", "Unrelated evidence", "user-one");
        yield* Deferred.await(entered[0]!);
        yield* api.goals.sendMessage("project", "New requirement", "user-two");
        yield* api.goals.sendMessage("project", "Supporting evidence", "user-three");
        yield* api.goals.sendMessage("project", "Supporting evidence", "user-three");
        const conflict = yield* api.goals
          .sendMessage("project", "Changed payload", "user-three")
          .pipe(Effect.flip);
        assert.equal(conflict.kind, "conflict");
        const during = yield* api.goals.timeline("project");
        assert.equal(during.groups.length, 1);
        assert.equal(during.groups[0].inputs.length, 1);
        assert.deepEqual(
          during.pendingInputs.map((input) =>
            input.payload._tag === "UserInput" ? input.payload.text : "",
          ),
          ["New requirement", "Supporting evidence"],
        );
        assert.equal(JSON.stringify(calls[0].messages).includes("New requirement"), false);
        yield* Deferred.succeed(release[0]!, undefined);
        yield* Deferred.await(entered[1]!);
        const next = yield* api.goals.timeline("project");
        assert.equal(next.groups[0].disposition, "ignored");
        assert.equal(next.groups[0].conclusion?.applied, true);
        assert.equal(next.groups[0].outputs.length, 0);
        assert.equal(next.groups[1].inputs.length, 2);
        assert.equal(next.pendingInputs.length, 0);
        assert.deepEqual(next.groups[0].inputs, during.groups[0].inputs);
        assert.equal(JSON.stringify(next).includes("historySequence"), false);
        yield* Deferred.succeed(release[1]!, undefined);
        const changes = yield* env.registry.subscribe;
        const finished = () =>
          Schema.decodeUnknownSync(GoalState)(
            env.registry.get("/goals/project")!.state,
          ).evaluations?.at(-1)?.status === "completed";
        if (!finished())
          yield* changes.pipe(Stream.filter(finished), Stream.take(1), Stream.runDrain);
        const latest = yield* api.goals.timeline("project", { limit: 1 });
        assert.equal(latest.groups[0].evaluationId, next.groups[1].evaluationId);
        const older = yield* api.goals.timeline("project", {
          limit: 1,
          before: latest.nextBefore!,
        });
        assert.equal(older.groups[0].evaluationId, during.groups[0].evaluationId);
        assert.equal(older.nextBefore, null);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("New user evidence after a failed evaluation gets its own group instead of being hidden in a retry", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        let count = 0;
        const env = yield* setup({
          durableSessions: true,
          plan: () =>
            ++count === 1
              ? Effect.fail(
                  new GoalReasoningError({
                    operation: "plan",
                    outcome: "failed",
                    message: "Needs correction",
                  }),
                )
              : Effect.succeed(plan),
        });
        yield* env.goals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
        const api = makeApplicationApi({
          registry: env.registry,
          goals: env.goals,
          inspect: Effect.succeed(null),
        });
        const changes = yield* env.registry.subscribe;
        yield* api.goals.sendMessage("project", "Original input", "first");
        yield* changes.pipe(
          Stream.filter(
            () =>
              Schema.decodeUnknownSync(GoalState)(env.registry.get("/goals/project")!.state)
                .evaluations?.[0]?.status === "failed",
          ),
          Stream.take(1),
          Stream.runDrain,
        );
        const later = yield* env.registry.subscribe;
        yield* api.goals.sendMessage("project", "Corrected instructions", "correction");
        yield* later.pipe(
          Stream.filter(
            () =>
              Schema.decodeUnknownSync(GoalState)(
                env.registry.get("/goals/project")!.state,
              ).evaluations?.at(-1)?.status === "completed",
          ),
          Stream.take(1),
          Stream.runDrain,
        );
        const page = yield* api.goals.timeline("project");
        assert.equal(page.groups.length, 2);
        assert.equal(page.groups[0].status, "failed");
        assert.equal(page.groups[1].retryOf, undefined);
        assert.equal(page.groups[1].inputs[0].payload._tag, "UserInput");
        assert.notEqual(page.groups[0].inputs[0].inputId, page.groups[1].inputs[0].inputId);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Task execution feedback names the evaluation that reserved its Run", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        let calls = 0;
        const env = yield* setup({
          durableSessions: true,
          plan: () =>
            Effect.sync(() =>
              ++calls === 1
                ? {
                    ...plan,
                    taskChanges: [
                      {
                        operation: "task_create" as const,
                        id: "review",
                        title: "Review",
                        instructions: "Inspect evidence",
                      },
                      { operation: "task_execute" as const, id: "review", revision: 1 },
                    ],
                  }
                : { ...plan, disposition: "no_change" as const },
            ),
        });
        const api = makeApplicationApi({
          registry: env.registry,
          goals: env.goals,
          inspect: Effect.succeed(null),
        });
        yield* env.goals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
        const changes = yield* env.registry.subscribe;
        yield* api.goals.sendMessage("project", "Review the current evidence", "start-review");
        const approved = () => approvalEntries(env.registry).length > 0;
        if (!approved())
          yield* changes.pipe(Stream.filter(approved), Stream.take(1), Stream.runDrain);
        const before = yield* api.goals.timeline("project");
        const runPath = before.groups[0].outputs.find((output) => output.runPath)?.runPath;
        assert.ok(runPath);
        yield* env.approvals.ask((replyTo) => ({
          _tag: "Resolve",
          id: approvalEntries(env.registry)[0].id,
          response: { decision: "approve" },
          replyTo,
        }));
        yield* env.approvals.tell({ _tag: "Deliver" });
        const state = () =>
          Schema.decodeUnknownSync(GoalState)(env.registry.get("/goals/project")!.state);
        const done = () =>
          state().evaluations?.length === 2 && state().evaluations?.[1].status === "completed";
        if (!done()) yield* changes.pipe(Stream.filter(done), Stream.take(1), Stream.runDrain);
        const after = yield* api.goals.timeline("project");
        const feedback = after.groups[1].inputs.find(
          (input) => input.payload._tag === "ExecutionFeedback" && input.payload.terminal,
        )?.payload;
        assert.ok(feedback && feedback._tag === "ExecutionFeedback");
        assert.equal(feedback.runPath, runPath);
        assert.equal(feedback.evaluationId, before.groups[0].evaluationId);
        assert.equal(after.groups[0].outputs.find((output) => output.runPath)?.status, "applied");
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Goal input admission splits oversized batches without losing or reassigning inputs", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = [
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
        ];
        const release = [
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
        ];
        let calls = 0;
        const env = yield* setup(
          {
            durableSessions: true,
            compact: () => Effect.succeed("Earlier evidence"),
            plan: () =>
              Effect.gen(function* () {
                const index = calls++;
                yield* Deferred.succeed(entered[index]!, undefined);
                yield* Deferred.await(release[index]!);
                return plan;
              }),
          },
          { contextTokens: 12000 },
        );
        yield* env.goals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
        const api = makeApplicationApi({
          registry: env.registry,
          goals: env.goals,
          inspect: Effect.succeed(null),
        });
        yield* api.goals.sendMessage("project", "Start", "batch-start");
        yield* Deferred.await(entered[0]!);
        yield* api.goals.sendMessage("project", "A".repeat(1000), "batch-a");
        yield* api.goals.sendMessage("project", "B".repeat(1000), "batch-b");
        yield* Deferred.succeed(release[0]!, undefined);
        yield* Deferred.await(entered[1]!);
        const partial = yield* api.goals.timeline("project");
        assert.equal(partial.groups[1].inputs.length, 1);
        assert.equal(partial.pendingInputs.length, 1);
        const pendingId = partial.pendingInputs[0].inputId;
        yield* Deferred.succeed(release[1]!, undefined);
        yield* Deferred.await(entered[2]!);
        const last = yield* api.goals.timeline("project");
        assert.equal(last.groups[2].inputs[0].inputId, pendingId);
        assert.equal(last.pendingInputs.length, 0);
        assert.equal(
          new Set(last.groups.flatMap((group) => group.inputs.map((input) => input.inputId))).size,
          3,
        );
        yield* Deferred.succeed(release[2]!, undefined);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
