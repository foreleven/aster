import { ContextsActor } from "../src/context/queries/actor.js";
import { testConversations } from "./conversation-fixtures.js";
import { AgentConversations } from "@aster/agent";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import {
  AgentError,
  type AgentInvocation,
  type AgentResult,
  type AssistantMessage,
} from "@aster/agent";
import { Deferred, Effect, Layer, Logger, Schema, Stream } from "effect";
import {
  ContextRegistry,
  ContextQueries,
  ExternalAgents,
  GoalSettings,
  GoalSnapshot,
  GoalsRootActor,
  TasksRootActor,
  SignalRootActor,
  SignalDefinitions,
  SignalSnapshot,
  ApprovalQueueActor,
  approvalEntries,
  type StoredContext,
  type GoalCommandReply,
} from "../src/index.js";
import { makeContextRegistry, type ContextStore } from "../src/testing/context.js";
import type { GoalSubmission } from "../src/goals/protocol.js";
import { fakeAgent } from "./fixtures.js";
import { agentResult, emptyRecall, modelReplyLayer } from "./workflow-fixtures.js";

const tool = (input: AgentInvocation, name: string, args: object, id = name) =>
  Effect.tryPromise({
    try: (signal) => input.tools!.find((tool) => tool.name === name)!.execute(id, args, signal),
    catch: (cause) => new AgentError("Fake model tool failed", [], { cause }),
  });
const run = <A, E>(effect: Effect.Effect<A, E, import("effect").Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(effect).pipe(Effect.timeout("8 seconds")));
const setup = Effect.fnUntraced(function* (
  conversation: (input: AgentInvocation) => Effect.Effect<AgentResult, AgentError>,
  options: {
    store?: ContextStore;
    goals?: GoalSettings["Service"]["definitions"];
    signals?: SignalDefinitions["Service"];
    gate?: (input: AgentInvocation) => Effect.Effect<AgentResult, AgentError>;
    external?: ReturnType<typeof fakeAgent>;
    history?: ReturnType<typeof testConversations>;
  } = {},
) {
  const registry = yield* makeContextRegistry(options.store);
  const system = yield* ActorSystem.make().pipe(
    ActorSystem.provide(
      Layer.succeed(ContextRegistry, registry),
      ContextQueries.layer.pipe(Layer.provide(Layer.succeed(ContextRegistry, registry))),

      emptyRecall,
      Layer.succeed(GoalSettings, {
        definitions: options.goals ?? [
          { slug: "project", description: "Improve project reliability" },
        ],
        reasoning: { model: "test" },
      }),
      Layer.succeed(AgentConversations, options.history ?? testConversations()),
      Layer.succeed(SignalDefinitions, options.signals ?? []),
      Layer.succeed(ExternalAgents, { test: options.external ?? fakeAgent() }),
      modelReplyLayer(
        "submit_relevance",
        options.gate ??
          (() =>
            Effect.succeed(agentResult("submit_relevance", { relevant: true, reason: "Related" }))),
      ),
      modelReplyLayer(undefined, conversation),
    ),
  );
  yield* system.spawn("contexts", ContextsActor);
  const approvals = yield* system.spawn("approvals", ApprovalQueueActor);
  const signals = yield* system.spawn("signals", SignalRootActor);
  yield* system.spawn("tasks", TasksRootActor);
  const goalActivation = yield* Deferred.make<void>();
  const root = yield* system.spawn("goals", GoalsRootActor, { metadata: { goalActivation } });
  yield* root.awaitStarted;
  yield* (yield* system.select("/user/goals/project").resolve()).awaitStarted;
  const state = () => Schema.decodeUnknownSync(GoalSnapshot)(registry.get("/goals/project")!.state);
  const wait = (predicate: () => boolean) =>
    Effect.scoped(
      Effect.gen(function* () {
        const changes = yield* registry.subscribe;
        if (!predicate())
          yield* changes.pipe(Stream.filter(predicate), Stream.take(1), Stream.runDrain);
      }),
    );
  const submit = (requestId: string, input: GoalSubmission) =>
    root.ask<GoalCommandReply>((replyTo) => ({
      _tag: "Route",
      slug: "project",
      command: { _tag: "SubmitInput", requestId, input, replyTo },
    }));
  const activate = Deferred.succeed(goalActivation, undefined).pipe(Effect.asVoid);
  const end = root.ask<GoalCommandReply>((replyTo) => ({
    _tag: "Route",
    slug: "project",
    command: { _tag: "End", requestId: "end", replyTo },
  }));
  const retry = (requestId: string, turnId: string) =>
    root.ask<GoalCommandReply>((replyTo) => ({
      _tag: "Route",
      slug: "project",
      command: { _tag: "RetryTurn", requestId, turnId, replyTo },
    }));
  return { registry, system, root, signals, approvals, state, wait, submit, activate, end, retry };
});

const contextInput = (requestId: string, revision: number): GoalSubmission => ({
  _tag: "GoalIntent",
  delivery: {
    requestId,
    causationId: requestId,
    source: "/system-one",
    target: "/goals/project",
    expectedRevision: revision,
    intent: {
      intentId: requestId,
      goalSlug: "project",
      source: {
        contextPath: "/chats/project",
        actorPath: "/chats/project",
        name: "Project",
        kind: "context",
      },
      content: {
        summary: "Project evidence",
        summaryRevision: requestId,
        summaryFingerprint: requestId,
      },
      relevance: {
        score: 1,
        rationale: "Candidate",
        screeningRecordId: requestId,
        threshold: 0.7,
        policyVersion: "test",
      },
      createdAt: "2026-10-01T00:00:00Z",
    },
  },
});
const submitContext = (env: Effect.Success<ReturnType<typeof setup>>, id: string) =>
  env.submit(id, contextInput(id, env.registry.get("/goals/project")!.revision!));
const relevant = () =>
  agentResult("submit_relevance", { relevant: true, reason: "Project evidence" });

test("Goal conversation and gate log live responses with their identities without exposing tools in replies", async () => {
  const logs: Array<{ message: unknown; annotations: Record<string, unknown> }> = [];
  const logger = Logger.formatStructured.pipe(
    Logger.map(({ message, annotations }) => {
      logs.push({ message, annotations });
    }),
  );
  const response: AssistantMessage = {
    role: "assistant",
    api: "openai-completions",
    provider: "test",
    model: "test-model",
    content: [
      {
        type: "thinking",
        thinking: "Inspect project evidence",
        thinkingSignature: "private-signature",
      },
      { type: "text", text: "Reading the current state" },
      {
        type: "toolCall",
        id: "read-project",
        name: "read_context",
        arguments: { path: "/goals/project" },
      },
    ],
    stopReason: "toolUse",
    timestamp: 0,
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
  };
  const observe = (input: AgentInvocation) =>
    Effect.promise(async (signal) => {
      assert.ok(input.onResponse);
      await input.onResponse(response, signal);
    });
  await run(
    Effect.gen(function* () {
      const history = testConversations();
      const env = yield* setup(
        (input) =>
          observe(input).pipe(
            Effect.as({
              messages: [
                response,
                {
                  ...response,
                  content: [{ type: "text", text: "Project update" }],
                  stopReason: "stop",
                },
              ],
            }),
          ),
        { history, gate: (input) => observe(input).pipe(Effect.as(relevant())) },
      );
      yield* env.activate;
      yield* env.wait(() => env.state().inputs[0]?.status === "completed");
      const inputId = env.state().inputs[0]!.inputId;
      yield* submitContext(env, "logged-context");
      yield* env.wait(() => env.state().inputs[1]?.status === "completed");
      const conversationLogs = logs.filter((log) => log.annotations.inputId === inputId);
      assert.deepEqual(
        conversationLogs.map((log) => log.message),
        [
          ["Goal agent thinking", { thinking: "Inspect project evidence" }],
          ["Goal agent text", { text: "Reading the current state" }],
          [
            "Goal agent tool call",
            {
              toolCallId: "read-project",
              tool: "read_context",
              arguments: { path: "/goals/project" },
            },
          ],
        ],
      );
      assert.ok(
        conversationLogs.every(
          (log) =>
            log.annotations.goalPath === "/goals/project" &&
            log.annotations.phase === "conversation" &&
            log.annotations.model === "test-model",
        ),
      );
      const gateLogs = logs.filter((log) => log.annotations.phase === "gate");
      assert.equal(gateLogs.length, 3);
      assert.ok(gateLogs.every((log) => log.annotations.intentId === "logged-context"));
      assert.ok(!JSON.stringify(logs).includes("private-signature"));
      const replies = (yield* history.read("/goals/project")).filter(
        (entry) => entry.kind === "goal.reply",
      );
      assert.equal(replies.length, 2);
      assert.ok(
        replies.every((entry) => (entry.data as { text: string }).text === "Project update"),
      );
    }).pipe(Effect.provide(Logger.layer([logger]))),
  );
});

test("a slow Context gate does not delay a user reply or start another gate", async () => {
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let gates = 0;
      const calls: string[] = [];
      const history = testConversations();
      const env = yield* setup(
        (input) =>
          Effect.sync(() => {
            calls.push(input.durable!.requestId);
            return { messages: [] };
          }),
        {
          history,
          gate: () =>
            Effect.gen(function* () {
              gates++;
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
              return relevant();
            }),
        },
      );
      yield* env.activate;
      yield* env.wait(() => env.state().inputs[0]?.status === "completed");
      assert.equal((yield* submitContext(env, "slow-context"))._tag, "Accepted");
      yield* Deferred.await(entered);
      assert.equal((yield* submitContext(env, "next-context"))._tag, "Accepted");
      yield* env.submit("date", { _tag: "UserInput", text: "What day is it today?" });
      const userId = env.state().inputs.at(-1)!.inputId;
      yield* env.wait(
        () => env.state().inputs.find((input) => input.inputId === userId)?.status === "completed",
      );
      assert.equal(gates, 1);
      assert.equal(env.state().inputs[1]!.status, "pending");
      assert.deepEqual(calls, [env.state().inputs[0]!.inputId, userId]);
      assert.ok(
        (yield* history.read("/goals/project")).some(
          (entry) =>
            entry.kind === "goal.reply" && (entry.data as { inputId?: string }).inputId === userId,
        ),
      );
      yield* Deferred.succeed(release, undefined);
      yield* env.wait(() => env.state().inputs.every((input) => input.status === "completed"));
      assert.equal(gates, 2);
    }),
  );
});

test("queued users precede ready Context and Task inputs without overlapping main conversations", async () => {
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const calls: string[] = [];
      let active = 0;
      let maximum = 0;
      const env = yield* setup((input) =>
        Effect.gen(function* () {
          active++;
          maximum = Math.max(maximum, active);
          calls.push(input.durable!.requestId);
          if (calls.length === 1) {
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
          }
          return { messages: [] };
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              active--;
            }),
          ),
        ),
      );
      yield* env.activate;
      yield* Deferred.await(entered);
      yield* submitContext(env, "context");
      yield* env.submit("task", {
        _tag: "TaskMessage",
        delivery: {
          requestId: "task",
          source: "/goals/project",
          task: { _tag: "Goal", target: "/goals/project", text: "Task update" },
          createdAt: "2026-10-01T00:00:00Z",
          causal: { rootRequestId: "task", remainingAgentTurns: 3 },
        },
      });
      yield* env.submit("user-one", { _tag: "UserInput", text: "First question" });
      yield* env.submit("user-two", { _tag: "UserInput", text: "Second question" });
      yield* env.wait(() => env.state().inputs[1]?.relevant === true);
      const ids = env.state().inputs.map((input) => input.inputId);
      assert.equal(calls.length, 1);
      yield* Deferred.succeed(release, undefined);
      yield* env.wait(() => env.state().inputs.every((input) => input.status === "completed"));
      assert.deepEqual(calls, [ids[0], ids[3], ids[4], ids[1], ids[2]]);
      assert.equal(maximum, 1);
    }),
  );
});

test("a failed gate releases screening without releasing the active main conversation", async () => {
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const calls: string[] = [];
      let gates = 0;
      const env = yield* setup(
        (input) =>
          Effect.gen(function* () {
            calls.push(input.durable!.requestId);
            if (calls.length === 1) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }
            return { messages: [] };
          }),
        {
          gate: () =>
            Effect.suspend(() =>
              ++gates === 1
                ? Effect.fail(new AgentError("Screening failed", [], { outcome: "failed" }))
                : Effect.succeed(relevant()),
            ),
        },
      );
      yield* env.activate;
      yield* Deferred.await(entered);
      yield* submitContext(env, "failed-context");
      yield* env.wait(() => env.state().inputs[1]?.status === "failed");
      yield* submitContext(env, "next-context");
      yield* env.wait(() => env.state().inputs[2]?.relevant === true);
      yield* env.submit("user", { _tag: "UserInput", text: "My question" });
      assert.equal(env.state().inputs[0]!.status, "running");
      assert.equal(calls.length, 1);
      const ids = env.state().inputs.map((input) => input.inputId);
      yield* Deferred.succeed(release, undefined);
      yield* env.wait(() => env.state().inputs[2]?.status === "completed");
      assert.deepEqual(calls, [ids[0], ids[3], ids[2]]);
      assert.equal((yield* env.retry("retry-gate", ids[1]!))._tag, "Accepted");
      yield* env.wait(() => env.state().inputs.at(-1)?.status === "completed");
      assert.equal(gates, 3);
    }),
  );
});

test("ending a Goal cancels its read-only gate without marking delivery uncertain", async () => {
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const cancelled = yield* Deferred.make<void>();
      let calls = 0;
      const env = yield* setup(
        () =>
          Effect.sync(() => {
            calls++;
            return { messages: [] };
          }),
        {
          gate: () =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(Deferred.succeed(cancelled, undefined)),
            ),
        },
      );
      yield* env.activate;
      yield* env.wait(() => env.state().inputs[0]?.status === "completed");
      yield* submitContext(env, "context");
      yield* Deferred.await(entered);
      assert.equal((yield* env.end)._tag, "Accepted");
      yield* Deferred.await(cancelled);
      assert.equal(env.state().inputs[1]!.status, "ignored");
      assert.equal(env.state().inputs[1]!.relevant, undefined);
      assert.equal(calls, 1);
    }),
  );
});

test("an interrupted read-only gate reruns on restart without reconciling a Pi delivery", async () => {
  const records = new Map<string, StoredContext>();
  const store: ContextStore = {
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.snapshot.path, structuredClone(record));
    },
  };
  const history = testConversations();
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const env = yield* setup(() => Effect.succeed({ messages: [] }), {
        store,
        history,
        gate: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      });
      yield* env.activate;
      yield* env.wait(() => env.state().inputs[0]?.status === "completed");
      yield* submitContext(env, "context");
      yield* Deferred.await(entered);
      assert.equal(env.state().inputs[1]!.status, "pending");
    }),
  );
  await run(
    Effect.gen(function* () {
      let gates = 0;
      const calls: AgentInvocation[] = [];
      const env = yield* setup(
        (input) =>
          Effect.sync(() => {
            calls.push(input);
            return { messages: [] };
          }),
        {
          store,
          history,
          gate: () =>
            Effect.sync(() => {
              gates++;
              return relevant();
            }),
        },
      );
      yield* env.activate;
      yield* env.wait(() => env.state().inputs[1]?.status === "completed");
      assert.equal(gates, 1);
      assert.equal(calls.length, 1);
      assert.equal(calls[0]!.durable!.reconcile, false);
    }),
  );
});

test("Goal persists input before acknowledgement, serializes delivery, and starts its conversation once", async () => {
  const records = new Map<string, StoredContext>();
  const store: ContextStore = {
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.snapshot.path, structuredClone(record));
    },
  };
  const history = testConversations();
  let calls = 0;
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const env = yield* setup(
        () =>
          Effect.gen(function* () {
            calls++;
            if (calls === 1) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }
            return { messages: [] };
          }),
        { store, history },
      );
      assert.equal(calls, 0);
      assert.equal(env.state().inputs[0]!.kind, "GoalStarted");
      yield* env.activate;
      yield* Deferred.await(entered);
      const input = { _tag: "UserInput" as const, text: "Investigate the build" };
      const accepted = yield* env.submit("user-1", input);
      assert.equal(accepted._tag, "Accepted");
      assert.equal(
        (records.get("/goals/project")!.snapshot.state as GoalSnapshot).inputs.at(-1)!.status,
        "pending",
      );
      assert.deepEqual(yield* env.submit("user-1", input), accepted);
      assert.equal((yield* env.submit("user-1", { ...input, text: "Different" }))._tag, "Rejected");
      yield* env.activate;
      assert.equal(calls, 1);
      yield* Deferred.succeed(release, undefined);
      yield* env.wait(() => env.state().inputs.every((input) => input.status === "completed"));
      assert.equal(calls, 2);
      for (const field of [
        "progress",
        "lastError",
        "historyCount",
        "completionOrigin",
        "causal",
        "requests",
        "evaluations",
        "pendingEvaluation",
        "activeTurnId",
        "pendingInputIds",
        "initialInputId",
        "signalOutbox",
        "nextStep",
      ])
        assert.equal(field in env.state(), false);
    }),
  );
  await run(
    Effect.gen(function* () {
      const env = yield* setup(
        () =>
          Effect.sync(() => {
            calls++;
            return { messages: [] };
          }),
        { store, history },
      );
      yield* env.activate;
      assert.equal(calls, 2);
    }),
  );
});

test("Context changes pass the Agent Gate before Pi; user and Task inputs bypass it", async () => {
  await run(
    Effect.gen(function* () {
      let calls = 0,
        gates = 0;
      const env = yield* setup(
        () =>
          Effect.sync(() => {
            calls++;
            return { messages: [] };
          }),
        {
          gate: () =>
            Effect.sync(() => {
              gates++;
              return agentResult("submit_relevance", {
                relevant: gates > 1,
                reason: gates > 1 ? "Project evidence" : "Different project",
              });
            }),
        },
      );
      yield* env.activate;
      yield* env.wait(() => env.state().inputs[0]?.status === "completed");
      for (const index of [1, 2]) {
        const requestId = `change-${index}`;
        yield* submitContext(env, requestId);
        yield* env.wait(() => ["completed", "ignored"].includes(env.state().inputs.at(-1)!.status));
      }
      assert.equal(calls, 2);
      assert.equal(gates, 2);
      assert.equal(env.state().inputs[1]!.status, "ignored");
      yield* env.submit("direct", { _tag: "UserInput", text: "Direct instruction" });
      yield* env.wait(() => env.state().inputs.at(-1)!.status === "completed");
      assert.equal(calls, 3);
      assert.equal(gates, 2);
      yield* env.submit("task-direct", {
        _tag: "TaskMessage",
        delivery: {
          requestId: "task-direct",
          source: "/goals/project",
          task: { _tag: "Goal", target: "/goals/project", text: "Follow up" },
          createdAt: "2026-10-01T00:00:00Z",
          causal: { rootRequestId: "task-direct", remainingAgentTurns: 3 },
        },
      });
      yield* env.wait(() => env.state().inputs.at(-1)!.status === "completed");
      assert.equal(calls, 4);
      assert.equal(gates, 2);
    }),
  );
});

test("End interrupts Pi without waiting, rejects late writes and leaves accepted input durable", async () => {
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<AgentInvocation>();
      const interrupted = yield* Deferred.make<void>();
      const env = yield* setup((input) =>
        Deferred.succeed(entered, input).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(Deferred.succeed(interrupted, undefined)),
        ),
      );
      yield* env.activate;
      const input = yield* Deferred.await(entered);
      assert.equal((yield* env.end)._tag, "Accepted");
      yield* Deferred.await(interrupted);
      assert.equal(env.state().status, "completed");
      assert.equal(
        (yield* env.submit("late", { _tag: "UserInput", text: "Late" }))._tag,
        "Rejected",
      );
      const result = yield* tool(input, "update_summary", {
        summary: "Late result",
      }).pipe(Effect.result);
      assert.ok(result._tag === "Failure" || result.success.isError);
      assert.notEqual(env.state().summary, "Late result");
    }),
  );
});

test("known failures can retry once; uncertain delivery blocks new work and resumes the same Pi identity on restart", async () => {
  const records = new Map<string, StoredContext>();
  const store: ContextStore = {
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.snapshot.path, structuredClone(record));
    },
  };
  const history = testConversations();
  let original = "";
  await run(
    Effect.gen(function* () {
      let calls = 0;
      const env = yield* setup(
        (input) =>
          Effect.suspend(() => {
            calls++;
            original = input.durable!.requestId;
            return calls === 1
              ? Effect.fail(new AgentError("Known failure", [], { outcome: "failed" }))
              : Effect.fail(new AgentError("Connection lost", [], { outcome: "unknown" }));
          }),
        { store, history },
      );
      yield* env.activate;
      yield* env.wait(() => env.state().inputs[0]?.status === "failed");
      const first = original;
      assert.equal((yield* env.retry("retry", first))._tag, "Accepted");
      yield* env.wait(() => env.state().inputs.at(-1)?.status === "unknown");
      assert.equal((yield* env.retry("again", first))._tag, "Rejected");
      yield* env.submit("fresh", { _tag: "UserInput", text: "New work" });
      assert.equal(calls, 2);
    }),
  );
  await run(
    Effect.gen(function* () {
      const seen: AgentInvocation[] = [];
      const env = yield* setup(
        (input) =>
          Effect.sync(() => {
            seen.push(input);
            return { messages: [] };
          }),
        { store, history },
      );
      yield* env.activate;
      yield* env.wait(() => env.state().inputs.at(-1)?.status === "completed");
      assert.equal(seen[0]!.durable!.requestId, original);
      assert.equal(seen[0]!.durable!.reconcile, true);
      assert.equal(seen.length, 2);
    }),
  );
});

test("Goal tracks multiple Tasks before tool acknowledgement and reuses references on follow-up", async () => {
  await run(
    Effect.gen(function* () {
      const paths: string[] = [];
      let turns = 0;
      const env = yield* setup((input) =>
        Effect.gen(function* () {
          if (input.durable?.owner === "tasks") return yield* Effect.never;
          if (++turns === 1) {
            const external = {
              task: {
                _tag: "Delegate",
                agent: "test",
                replyTo: "/goals/project",
                task: { instructions: "Investigate project changes", input: [] },
              },
            };
            const first = yield* tool(input, "start_task", external, "external-task");
            assert.equal(first.isError, undefined);
            const receipt = first.details as { taskPath: string };
            paths.push(receipt.taskPath);
            const replay = yield* tool(input, "start_task", external, "external-task");
            assert.deepEqual(replay.details, first.details);
            const second = yield* tool(
              input,
              "start_task",
              {
                task: {
                  _tag: "Agent",
                  replyTo: "/goals/project",
                  task: { instructions: "Analyze competitors", input: [] },
                },
              },
              "internal-task",
            );
            assert.equal(second.isError, undefined);
            paths.push((second.details as { taskPath: string }).taskPath);
          } else {
            const reply = yield* tool(input, "task_send", {
              target: paths[0],
              text: "Only the past week",
            });
            assert.equal(reply.isError, undefined);
          }
          const listed = yield* tool(input, "task_list", {});
          assert.deepEqual(
            (listed.details as { path: string }[]).map((task) => task.path),
            paths,
          );
          const current = yield* tool(input, "goal_current", {});
          assert.deepEqual((current.details as { state: { tasks: string[] } }).state.tasks, paths);
          return { messages: [] };
        }),
      );
      yield* env.activate;
      yield* env.wait(() => env.state().inputs[0]?.status === "completed");
      assert.equal(paths.length, 2);
      assert.deepEqual(env.state().tasks, paths);
      yield* env.submit("followup", { _tag: "UserInput", text: "Only the past week" });
      yield* env.wait(() => env.state().inputs.at(-1)?.status === "completed");
      assert.equal(turns, 2);
      assert.deepEqual(env.state().tasks, paths);
    }),
  );
});

test("Tasks execute independently through shared Run approval and return feedback to the conversation", async () => {
  await run(
    Effect.gen(function* () {
      const finished = yield* Deferred.make<void>();
      const submitted = yield* Deferred.make<void>();
      let submissions = 0,
        calls = 0,
        gates = 0;
      const env = yield* setup(
        (input) =>
          Effect.gen(function* () {
            calls++;
            if (calls === 1) {
              const result = yield* tool(input, "start_task", {
                task: {
                  _tag: "Delegate",
                  agent: "test",
                  task: { instructions: "Investigate the project", input: [] },
                  replyTo: "/goals/project",
                },
              });
              assert.equal(result.isError, undefined);
            }
            return { messages: [] };
          }),
        {
          gate: () =>
            Effect.sync(() => {
              gates++;
              return agentResult("submit_relevance", {
                relevant: false,
                reason: "Context gate only",
              });
            }),
          external: fakeAgent({
            submit: () =>
              Effect.sync(() => {
                submissions++;
                return { sessionId: "external" };
              }).pipe(Effect.tap(() => Deferred.succeed(submitted, undefined))),
            status: () => Effect.succeed({ state: "running" }),
            wait: () =>
              Deferred.await(finished).pipe(
                Effect.as({ state: "completed", result: { text: "Root cause identified" } }),
              ),
          }),
        },
      );
      yield* env.activate;
      yield* env.wait(() =>
        approvalEntries(env.registry).some((entry) => entry.status === "pending"),
      );
      assert.equal(submissions, 0);
      yield* env.wait(() => env.state().inputs[0]!.status === "completed");
      const approval = approvalEntries(env.registry)[0]!;
      yield* env.approvals.ask((replyTo) => ({
        _tag: "Resolve",
        id: approval.id,
        response: { decision: "approve" },
        replyTo,
      }));
      yield* env.approvals.tell({ _tag: "Deliver" });
      yield* env.wait(() =>
        Object.values(env.registry.snapshot()).some(
          (record) =>
            record.path.startsWith("/tasks/") &&
            (record.state as { status?: string }).status === "running",
        ),
      );
      yield* Deferred.await(submitted);
      assert.equal(submissions, 1);
      yield* env.submit("unrelated-user-turn", { _tag: "UserInput", text: "A different request" });
      yield* env.wait(() => env.state().inputs.at(-1)!.status === "completed");
      yield* Deferred.succeed(finished, undefined);
      yield* env.wait(() =>
        env
          .state()
          .inputs.some(
            (input) => input.kind === "ExecutionFeedback" && input.status === "completed",
          ),
      );
      assert.equal(submissions, 1);
      assert.deepEqual(env.state().tasks, [approval.contextPath]);
      assert.deepEqual(
        Schema.decodeUnknownSync(Schema.Struct({ tasks: Schema.Array(Schema.String) }))(
          env.registry.reader.get("/goals/project")!.state,
        ).tasks,
        [approval.contextPath],
      );
      assert.equal(gates, 0);
      const feedback = env.state().inputs.findLast((input) => input.kind === "ExecutionFeedback")!;
      assert.equal(feedback.causal?.rootRequestId, "goal:project:initial");
      assert.notEqual(feedback.causal?.rootRequestId, "unrelated-user-turn");
    }),
  );
});

test("a lost delivery-commit acknowledgement reopens the same Pi exchange without duplicating accepted input", async () => {
  const records = new Map<string, StoredContext>();
  let loseAck = true;
  const calls: AgentInvocation[] = [];
  await run(
    Effect.gen(function* () {
      const env = yield* setup(
        (input) =>
          Effect.sync(() => {
            calls.push(input);
            return { messages: [] };
          }),
        {
          store: {
            loadAll: () => [...records.values()],
            save: (record) => {
              records.set(record.snapshot.path, structuredClone(record));
              if (
                loseAck &&
                record.snapshot.path === "/goals/project" &&
                (record.snapshot.state as GoalSnapshot).inputs[0]?.status === "running"
              ) {
                loseAck = false;
                throw new Error("Persisted before acknowledgement loss");
              }
            },
          },
        },
      );
      yield* env.activate;
      yield* env.wait(() => env.state().inputs[0]?.status === "completed");
      assert.equal(env.state().inputs.length, 1);
      assert.equal(calls.length, 1);
      assert.equal(calls[0]!.durable!.reconcile, true);
    }),
  );
});

test("Task recovery reconnects its Goal and delivers the result without resubmitting external work", async () => {
  const records = new Map<string, StoredContext>();
  const store: ContextStore = {
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.snapshot.path, structuredClone(record));
    },
  };
  const history = testConversations();
  let submissions = 0;
  await run(
    Effect.gen(function* () {
      let calls = 0;
      const waiting = yield* Deferred.make<void>();
      const env = yield* setup(
        (input) =>
          Effect.gen(function* () {
            if (++calls === 1)
              yield* tool(input, "start_task", {
                task: {
                  _tag: "Delegate",
                  agent: "test",
                  task: { instructions: "Recover this external task", input: [] },
                  replyTo: "/goals/project",
                },
              });
            return { messages: [] };
          }),
        {
          store,
          history,
          external: fakeAgent({
            submit: () =>
              Effect.sync(() => {
                submissions++;
                return { sessionId: "retained-session" };
              }),
            wait: () => Deferred.succeed(waiting, undefined).pipe(Effect.andThen(Effect.never)),
          }),
        },
      );
      yield* env.activate;
      yield* env.wait(() =>
        approvalEntries(env.registry).some((entry) => entry.status === "pending"),
      );
      const approval = approvalEntries(env.registry)[0]!;
      yield* env.approvals.ask((replyTo) => ({
        _tag: "Resolve",
        id: approval.id,
        response: { decision: "approve" },
        replyTo,
      }));
      yield* env.approvals.tell({ _tag: "Deliver" });
      yield* Deferred.await(waiting);
      yield* env.wait(() =>
        Object.values(env.registry.snapshot()).some(
          (record) =>
            record.path.startsWith("/tasks/") &&
            (record.state as { status?: string }).status === "running",
        ),
      );
      assert.equal(submissions, 1);
    }),
  );
  await run(
    Effect.gen(function* () {
      const result = { state: "completed" as const, result: { text: "Recovered task result" } };
      const env = yield* setup(() => Effect.succeed({ messages: [] }), {
        store,
        history,
        external: fakeAgent({
          submit: () =>
            Effect.sync(() => {
              submissions++;
              return { sessionId: "duplicate" };
            }),
          status: () => Effect.succeed(result),
          wait: () => Effect.succeed(result),
        }),
      });
      yield* env.activate;
      yield* env.wait(() =>
        env
          .state()
          .inputs.some(
            (input) => input.kind === "ExecutionFeedback" && input.status === "completed",
          ),
      );
      assert.equal(submissions, 1);
    }),
  );
});

test("an ended Goal retains uncertain delivery on restart without restarting Pi", async () => {
  const records = new Map<string, StoredContext>();
  const store: ContextStore = {
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.snapshot.path, structuredClone(record));
    },
  };
  const history = testConversations();
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const env = yield* setup(
        () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        { store, history },
      );
      yield* env.activate;
      yield* Deferred.await(entered);
      yield* env.end;
      assert.equal(env.state().inputs[0]!.status, "unknown");
    }),
  );
  await run(
    Effect.gen(function* () {
      let calls = 0;
      const env = yield* setup(
        () =>
          Effect.sync(() => {
            calls++;
            return { messages: [] };
          }),
        { store, history },
      );
      yield* env.activate;
      assert.equal(env.state().status, "completed");
      assert.equal(env.state().inputs[0]!.status, "unknown");
      assert.equal(calls, 0);
    }),
  );
});

test("Goal receipts normalize nested object keys, retain array order and omit duplicate payloads", async () => {
  await run(
    Effect.gen(function* () {
      const env = yield* setup(() => Effect.succeed({ messages: [] }));
      const delivery = {
        requestId: "canonical-task",
        source: "/goals/project",
        createdAt: "2026-10-05T00:00:00Z",
        task: { _tag: "Goal" as const, target: "/goals/project", text: "Review" },
        causal: { rootRequestId: "canonical-task", remainingAgentTurns: 2 },
        evidence: {
          path: "/source",
          description: "Evidence",
          state: { first: 1, nested: { a: 2, b: 3 }, items: [1, 2] },
          messages: [],
        },
      };
      const accepted = yield* env.submit(delivery.requestId, { _tag: "TaskMessage", delivery });
      assert.equal(accepted._tag, "Accepted");
      const reordered = {
        ...delivery,
        evidence: {
          ...delivery.evidence,
          state: { items: [1, 2], nested: { b: 3, a: 2 }, first: 1 },
        },
      };
      assert.deepEqual(
        yield* env.submit(delivery.requestId, { _tag: "TaskMessage", delivery: reordered }),
        accepted,
      );
      const changed = {
        ...reordered,
        evidence: { ...reordered.evidence, state: { ...reordered.evidence.state, items: [2, 1] } },
      };
      assert.equal(
        (yield* env.submit(delivery.requestId, { _tag: "TaskMessage", delivery: changed }))._tag,
        "Rejected",
      );
      const receipt = env.state().receipts.find((item) => item.requestId === delivery.requestId)!;
      assert.deepEqual(Object.keys(receipt).sort(), ["payloadFingerprint", "receipt", "requestId"]);
      assert.equal(env.state().inputs.filter((item) => item.kind === "TaskMessage").length, 1);
    }),
  );
});

test("summary tools persist before acknowledgement and keep the Goal available for another input", async () => {
  await run(
    Effect.gen(function* () {
      let calls = 0;
      let previous: AgentInvocation | undefined;
      const env = yield* setup((input) =>
        Effect.gen(function* () {
          calls++;
          if (previous) {
            const stale = yield* tool(previous, "update_summary", {
              summary: "Retired finding",
            }).pipe(Effect.result);
            assert.ok(stale._tag === "Failure" || stale.success.isError);
            assert.notEqual(env.state().summary, "Retired finding");
          }
          previous = input;
          assert.equal(
            input.tools!.some((tool) => tool.name === "update_goal"),
            false,
          );
          const result = yield* tool(input, "update_summary", { summary: `Finding ${calls}` });
          assert.equal(result.isError, undefined);
          const current = yield* tool(input, "goal_current", {});
          assert.equal(
            (current.details as { state: { summary: string } }).state.summary,
            `Finding ${calls}`,
          );
          return { messages: [] };
        }),
      );
      yield* env.activate;
      yield* env.wait(() => env.state().inputs[0]?.status === "completed");
      assert.equal(env.state().status, "active");
      yield* env.submit("continue", { _tag: "UserInput", text: "Continue" });
      yield* env.wait(() => env.state().inputs.at(-1)?.status === "completed");
      assert.equal(env.state().summary, "Finding 2");
      assert.equal(env.state().status, "active");
    }),
  );
});

test("Goal lifecycle pauses only owned Signals through its ActorContext", async () => {
  await run(
    Effect.gen(function* () {
      const definition = {
        trigger: { _tag: "Context" as const, when: "Evidence changes" },
        task: { _tag: "Goal" as const, target: "/goals/project", text: "Review evidence" },
      };
      const env = yield* setup(() => Effect.die("No conversation expected"), {
        goals: [
          { slug: "project", description: "Project" },
          { slug: "other", description: "Other" },
        ],
        signals: [{ ...definition, slug: "configured" }],
      });
      yield* (yield* env.system.select("/user/goals/other").resolve()).awaitStarted;
      yield* (yield* env.system.select("/user/signals/configured").resolve()).awaitStarted;
      for (const slug of ["project", "other"]) {
        const reply = yield* env.signals.ask<
          import("../src/signals/protocol.js").SignalCommandReply
        >((replyTo) => ({
          _tag: "Change",
          replyTo,
          input: {
            requestId: `create:${slug}`,
            source: `/goals/${slug}`,
            target: `/signals/${slug}--watch`,
            change: { operation: "create", definition },
            causal: { rootRequestId: "test", remainingAgentTurns: 3 },
          },
        }));
        assert.equal(reply._tag, "Accepted");
      }
      const listed = yield* env.signals.ask<import("../src/commands/query.js").QueryReply>(
        (replyTo) => ({
          _tag: "ListByOwner",
          owner: "/goals/project",
          replyTo,
        }),
      );
      assert.equal(listed._tag, "Success");
      if (listed._tag === "Success")
        assert.deepEqual(
          Schema.decodeUnknownSync(Schema.Array(Schema.Struct({ path: Schema.String })))(
            listed.value,
          ).map((record) => record.path),
          ["/signals/project--watch"],
        );
      assert.equal((yield* env.end)._tag, "Accepted");
      const signalState = (path: string) =>
        Schema.decodeUnknownSync(SignalSnapshot)(env.registry.get(path)!.state);
      yield* env.wait(() => signalState("/signals/project--watch").status === "paused");
      const paused = env.registry.get("/signals/project--watch");
      yield* env.signals.ask<void>((replyTo) => ({
        _tag: "PauseByOwner",
        owner: "/goals/project",
        replyTo,
      }));
      assert.deepEqual(env.registry.get("/signals/project--watch"), paused);
      assert.equal(signalState("/signals/other--watch").status, "active");
      assert.equal(signalState("/signals/configured").status, "active");
    }),
  );
});
