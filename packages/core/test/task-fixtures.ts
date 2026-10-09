import { Actor, ActorSystem } from "@aster/actor";
import { AgentConversations, DurableHarness } from "@aster/agent/harness";
import { Clock, Effect, Layer, Schema, Stream } from "effect";
import {
  ApprovalQueueActor,
  ContextQueries,
  ContextRegistry,
  ExternalAgents,
  GoalSettings,
  SignalDefinitions,
  TaskSnapshot,
  TasksRootActor,
  defineContext,
  type ExternalAgent,
  type StoredContext,
} from "../src/index.js";
import {
  readExecutionCheckpoint,
  type ExecutionCheckpoint,
} from "../src/tasks/execution/checkpoint.js";
import { testConversations } from "./conversation-fixtures.js";
import { makeHarness } from "./harness-fixtures.js";
import { emptyRecall } from "./workflow-fixtures.js";

import { GoalCommands, type GoalCommand } from "../src/goals/protocol.js";

import type { TaskDeliveryInput } from "../src/tasks/contracts.js";
import { taskPathFor } from "../src/tasks/state/admission.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { fakeAgent } from "./fixtures.js";

export const taskInput = (requestId = "task", source = "/goals/personal"): TaskDeliveryInput => ({
  requestId,
  source,
  target: taskPathFor(source, requestId),
  createdAt: "2026-10-01T00:00:00Z",
  agent: "test",
  task: { instructions: "Read evidence", input: [] },
  replyTo: "/goals/personal",
  remainingAgentTurns: 3,
});
export const taskFixture = Effect.fnUntraced(function* (
  options: {
    records?: Map<string, StoredContext>;
    agent?: ExternalAgent;
    agents?: ExternalAgents["Service"];
    harness?: DurableHarness["Service"];
    clock?: Clock.Clock;
    conversations?: AgentConversations["Service"];
    saved?: (record: StoredContext) => void;
  } = {},
) {
  const records = options.records ?? new Map<string, StoredContext>();
  const registry = yield* makeContextRegistry({
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.snapshot.path, structuredClone(record));
      options.saved?.(record);
    },
  });
  const feedback: GoalCommand[] = [];
  const GoalSink = Actor.define("test/TaskGoal", {
    commands: GoalCommands,
  })(
    Effect.succeed({
      receive: (command) =>
        Effect.gen(function* () {
          if (command._tag === "AttachTask")
            return yield* command.replyTo.tell({ _tag: "Attached" });
          feedback.push(command);
          yield* command.replyTo.tell({
            _tag: "Accepted",
            receipt: { requestId: command.requestId, revision: 1 },
          });
        }),
    }),
  );
  const GoalRoot = Actor.define("test/TaskGoals", {
    commands: [],
  })(
    Effect.succeed({
      started: (actor) => actor.spawn("personal", GoalSink).pipe(Effect.asVoid),
      receive: () => Effect.void,
    }),
  );
  yield* registry.register(
    "/goals/personal",
    defineContext({ state: Schema.Struct({ status: Schema.String }), message: Schema.Never }),
  );
  if (!registry.get("/goals/personal"))
    yield* registry.commit(
      {
        path: "/goals/personal",
        description: "Assistant",
        state: { status: "active" },
        messages: [],
      },
      { expectedRevision: 0 },
    );
  const conversations = options.conversations ?? testConversations();
  const system = yield* ActorSystem.make().pipe(
    ActorSystem.provide(
      Layer.succeed(ContextRegistry, registry),
      Layer.succeed(AgentConversations, conversations),
      Layer.succeed(GoalSettings, { definitions: [], reasoning: { model: "test" } }),
      Layer.succeed(
        DurableHarness,
        options.harness ?? makeHarness(() => Effect.die("Unexpected internal execution")),
      ),
      ContextQueries.layer.pipe(Layer.provide(Layer.succeed(ContextRegistry, registry))),
      emptyRecall,
      Layer.succeed(ExternalAgents, options.agents ?? { test: options.agent ?? fakeAgent() }),
      Layer.succeed(SignalDefinitions, []),
      Layer.succeed(Clock.Clock, options.clock ?? (yield* Clock.Clock)),
    ),
  );
  yield* system.spawn("goals", GoalRoot);
  const approvals = yield* system.spawn("approvals", ApprovalQueueActor);
  const tasks = yield* system.spawn("tasks", TasksRootActor);
  yield* tasks.awaitStarted;
  // Tests inspecting recovery wait only for their pre-existing subjects.
  for (const record of records.values())
    if (record.snapshot.path.startsWith("/tasks/"))
      yield* (yield* system.select(`/user${record.snapshot.path}`).resolve()).awaitStarted;
  const wait = (predicate: () => boolean) =>
    Effect.gen(function* () {
      const changes = yield* registry.subscribe;
      if (!predicate())
        yield* changes.pipe(Stream.filter(predicate), Stream.take(1), Stream.runDrain);
    });
  return { conversations, system, registry, records, feedback, approvals, tasks, wait };
});

export const retainedTask = Effect.fnUntraced(function* (
  messages: ReturnType<typeof testConversations>,
  status: TaskSnapshot["status"] = "uncertain",
  input = taskInput(),
) {
  const entry = yield* messages.append(input.target, input.requestId, "task.admission", input);
  const { source, replyTo, agent, remainingAgentTurns } = input;
  const terminal = ["completed", "failed", "cancelled", "uncertain"].includes(status);
  const inputs: TaskSnapshot["inputs"] = [
    {
      requestId: input.requestId,
      entryId: entry.id,
      receipt: { requestId: input.requestId, revision: 1 },
      status: terminal && status !== "uncertain" ? "completed" : "pending",
    },
  ];
  const outcome = terminal
    ? yield* messages.append(input.target, "original-result", "task.result", {
        text: "Original result",
        status,
        covered: status === "uncertain" ? [] : inputs.map((input) => input.requestId),
        roundId: input.requestId,
      })
    : undefined;
  const state: TaskSnapshot = {
    admission: { source, replyTo, agent, remainingAgentTurns },
    roundId: input.requestId,
    status,
    inputs,
    ...(outcome ? { outcomeEntryId: outcome.id } : {}),
  };
  yield* messages.append(input.target, "execution:1", "task.execution", {
    revision: 1,
    prompt: "Test policy",
    approved: true,
    ...(status === "running" ? { session: { sessionId: "original" } } : {}),
    deliveries: [
      {
        requestId: input.requestId,
        roundId: input.requestId,
        kind: "instruction",
        status: status === "uncertain" ? "unknown" : "accepted",
      },
    ],
  });
  return {
    snapshot: {
      path: input.target,
      description: "Retained Task",
      revision: 2,
      messages: [],
      state,
    },
    events: [],
  } satisfies StoredContext;
});

export const checkpoint = (messages: AgentConversations["Service"], path: string) =>
  readExecutionCheckpoint(path).pipe(Effect.provideService(AgentConversations, messages));
export const seedCheckpoint = Effect.fnUntraced(function* (
  messages: AgentConversations["Service"],
  path: string,
  patch: Partial<ExecutionCheckpoint>,
) {
  const previous = (yield* checkpoint(messages, path))!;
  const revision = previous.revision + 1;
  yield* messages.append(path, `execution:${revision}`, "task.execution", {
    ...previous,
    ...patch,
    revision,
  });
});
