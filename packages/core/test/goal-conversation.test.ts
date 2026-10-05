import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, type ActorRef } from "@aster/actor";
import { AgentError, type AgentInvocation, type AgentResult } from "@aster/agent";
import { Deferred, Effect, Layer, Schema, Stream } from "effect";
import {
  ContextRegistry,
  ExternalAgents,
  GoalSettings,
  GoalHistoryStore,
  GoalSignals,
  GoalState,
  GoalsRootActor,
  RunRootActor,
  SignalRootActor,
  SignalDefinitions,
  ApprovalQueueActor,
  approvalEntries,
  makeGoalSignalCommands,
  makeMemoryGoalHistory,
  type ContextRecord,
  type GoalCommandReply,
  type GoalReadyReply,
  type SignalRootCommand,
} from "../src/index.js";
import { makeContextRegistry, type ContextStore } from "../src/testing/context.js";
import type { GoalSubmission } from "../src/goals/protocol.js";
import { fakeAgent, preparationLayer } from "./fixtures.js";
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
    gate?: (input: AgentInvocation) => Effect.Effect<AgentResult, AgentError>;
    external?: ReturnType<typeof fakeAgent>;
    history?: ReturnType<typeof makeMemoryGoalHistory>;
  } = {},
) {
  const registry = yield* makeContextRegistry(options.store);
  const system = yield* ActorSystem.make().pipe(
    ActorSystem.provide(
      Layer.succeed(ContextRegistry, registry),
      preparationLayer,
      emptyRecall,
      Layer.succeed(GoalSettings, {
        definitions: [{ slug: "project", description: "Improve project reliability" }],
        reasoning: { model: "test" },
      }),
      Layer.succeed(GoalHistoryStore, options.history ?? makeMemoryGoalHistory()),
      Layer.effect(
        GoalSignals,
        Effect.sync(() =>
          makeGoalSignalCommands(registry, {
            ask: (command, timeout) => signals.ask(command, timeout),
          }),
        ),
      ),
      Layer.succeed(SignalDefinitions, []),
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
  const approvals = yield* system.spawn("approvals", ApprovalQueueActor);
  const signals: ActorRef<SignalRootCommand> = yield* system.spawn("signals", SignalRootActor);
  yield* system.spawn("runs", RunRootActor);
  const root = yield* system.spawn("goals", GoalsRootActor);
  yield* root.ask<GoalReadyReply>((replyTo) => ({
    _tag: "AwaitReady",
    stage: "restored",
    replyTo,
  }));
  const state = () => Schema.decodeUnknownSync(GoalState)(registry.get("/goals/project")!.state);
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
  const activate = root.tell({ _tag: "Initialize" });
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
  return { registry, system, root, approvals, state, wait, submit, activate, end, retry };
});

test("Goal persists input before acknowledgement, serializes delivery, and starts its conversation once", async () => {
  const records = new Map<string, ContextRecord>();
  const store: ContextStore = {
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.path, structuredClone(record));
    },
  };
  const history = makeMemoryGoalHistory();
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
      assert.equal(env.state().inputs[0]!.payload._tag, "GoalStarted");
      yield* env.activate;
      yield* Deferred.await(entered);
      const input = { _tag: "UserInput" as const, text: "Investigate the build" };
      const accepted = yield* env.submit("user-1", input);
      assert.equal(accepted._tag, "Accepted");
      assert.equal(
        (records.get("/goals/project")!.state as GoalState).inputs.at(-1)!.status,
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
        "evaluations",
        "pendingEvaluation",
        "activeTurnId",
        "pendingInputIds",
        "initialInputId",
        "tasks",
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
      yield* env.root.ask<GoalReadyReply>((replyTo) => ({ _tag: "AwaitReady", replyTo }));
      assert.equal(calls, 2);
    }),
  );
});

test("Context changes pass the Agent Gate before Pi; user input bypasses it", async () => {
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
        yield* env.submit(requestId, {
          _tag: "GoalIntent",
          delivery: {
            requestId,
            causationId: requestId,
            source: "/system-one",
            target: "/goals/project",
            expectedRevision: env.registry.get("/goals/project")!.revision!,
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
                summary: "Evidence",
                summaryRevision: String(index),
                summaryFingerprint: String(index),
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
        yield* env.wait(() => ["completed", "ignored"].includes(env.state().inputs.at(-1)!.status));
      }
      assert.equal(calls, 2);
      assert.equal(gates, 2);
      assert.equal(env.state().inputs[1]!.status, "ignored");
      yield* env.submit("direct", { _tag: "UserInput", text: "Direct instruction" });
      yield* env.wait(() => env.state().inputs.at(-1)!.status === "completed");
      assert.equal(calls, 3);
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
      const result = yield* tool(input, "update_goal", {
        progress: "Late result",
        completed: false,
        evidence: [],
      }).pipe(Effect.result);
      assert.ok(result._tag === "Failure" || result.success.isError);
      assert.notEqual(env.state().progress, "Late result");
    }),
  );
});

test("known failures can retry once; uncertain delivery blocks new work and resumes the same Pi identity on restart", async () => {
  const records = new Map<string, ContextRecord>();
  const store: ContextStore = {
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.path, structuredClone(record));
    },
  };
  const history = makeMemoryGoalHistory();
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

test("Tasks execute independently through shared Run approval and return feedback to the conversation", async () => {
  await run(
    Effect.gen(function* () {
      const finished = yield* Deferred.make<void>();
      let submissions = 0,
        calls = 0;
      const env = yield* setup(
        (input) =>
          Effect.gen(function* () {
            calls++;
            if (calls === 1) {
              const result = yield* tool(input, "start_task", {
                agent: "test",
                task: { instructions: "Investigate the project", input: [] },
              });
              assert.equal(result.isError, undefined);
            }
            return { messages: [] };
          }),
        {
          external: fakeAgent({
            submit: () =>
              Effect.sync(() => {
                submissions++;
                return { sessionId: "external" };
              }),
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
            record.path.startsWith("/runs/") &&
            (record.state as { status?: string }).status === "running",
        ),
      );
      assert.equal(submissions, 1);
      yield* Deferred.succeed(finished, undefined);
      yield* env.wait(() =>
        env
          .state()
          .inputs.some(
            (input) =>
              input.payload._tag === "ExecutionFeedback" &&
              input.payload.text.includes("Root cause identified") &&
              input.status === "completed",
          ),
      );
      assert.equal(submissions, 1);
      assert.equal("tasks" in env.state(), false);
    }),
  );
});

test("a lost delivery-commit acknowledgement reopens the same Pi exchange without duplicating accepted input", async () => {
  const records = new Map<string, ContextRecord>();
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
              records.set(record.path, structuredClone(record));
              if (
                loseAck &&
                record.path === "/goals/project" &&
                (record.state as GoalState).inputs[0]?.status === "running"
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
  const records = new Map<string, ContextRecord>();
  const store: ContextStore = {
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.path, structuredClone(record));
    },
  };
  const history = makeMemoryGoalHistory();
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
                agent: "test",
                task: { instructions: "Recover this external task", input: [] },
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
            record.path.startsWith("/runs/") &&
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
            (input) =>
              input.payload._tag === "ExecutionFeedback" &&
              input.payload.text.includes("Recovered task result") &&
              input.status === "completed",
          ),
      );
      assert.equal(submissions, 1);
    }),
  );
});

test("an ended Goal retains uncertain delivery on restart without restarting Pi", async () => {
  const records = new Map<string, ContextRecord>();
  const store: ContextStore = {
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.path, structuredClone(record));
    },
  };
  const history = makeMemoryGoalHistory();
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
      yield* env.root.ask<GoalReadyReply>((replyTo) => ({ _tag: "AwaitReady", replyTo }));
      assert.equal(env.state().status, "completed");
      assert.equal(env.state().inputs[0]!.status, "unknown");
      assert.equal(calls, 0);
    }),
  );
});
