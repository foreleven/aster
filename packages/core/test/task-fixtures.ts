import { AgentConversations, AgentRunner } from "@aster/agent";
import { testConversations } from "./conversation-fixtures.js";
import { emptyRecall } from "./workflow-fixtures.js";
import { GoalSettings, ContextQueries } from "../src/index.js";
import { Actor, ActorSystem } from "@aster/actor";
import { Clock, Effect, Layer, Schema, Stream } from "effect";
import {
  ApprovalQueueActor,
  ChannelWrites,
  SignalDefinitions,
  ContextRegistry,
  ExternalAgents,
  TasksRootActor,
  TaskState,
  defineContext,
  type ContextRecord,
  type ExternalAgent,
} from "../src/index.js";
import { GoalCommand } from "../src/goals/protocol.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { fakeAgent } from "./fixtures.js";
import { taskPathFor } from "../src/tasks/admission.js";
import type { TaskDeliveryInput } from "@aster/api-contracts";

export const taskInput = (requestId = "task", source = "/goals/personal"): TaskDeliveryInput => ({
  requestId,
  source,
  target: taskPathFor(source, requestId),
  createdAt: "2026-10-01T00:00:00Z",
  agent: "test",
  task: { instructions: "Read evidence", input: [] },
  replyTo: "/goals/personal",
  causal: { rootRequestId: requestId, remainingAgentTurns: 3 },
});
export const taskFixture = Effect.fnUntraced(function* (
  options: {
    records?: Map<string, ContextRecord>;
    agent?: ExternalAgent;
    agents?: ExternalAgents["Service"];
    runner?: AgentRunner["Service"];
    clock?: Clock.Clock;
    publish?: ChannelWrites["Service"]["publish"];
    conversations?: AgentConversations["Service"];
    saved?: (record: ContextRecord) => void;
  } = {},
) {
  const records = options.records ?? new Map<string, ContextRecord>();
  const registry = yield* makeContextRegistry({
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.path, structuredClone(record));
      options.saved?.(record);
    },
  });
  const feedback: GoalCommand[] = [];
  class GoalSink extends Actor.Service<GoalSink>()("test/TaskGoal", { command: GoalCommand }) {
    static readonly layer = Layer.succeed(
      GoalSink,
      GoalSink.of({
        receive: (command) =>
          Effect.gen(function* () {
            feedback.push(command);
            yield* command.replyTo.tell({
              _tag: "Accepted",
              receipt: { requestId: command.requestId, revision: 1 },
            });
          }),
      }),
    );
  }
  class GoalRoot extends Actor.Service<GoalRoot>()("test/TaskGoals", { command: Schema.Never }) {
    static readonly layer = Layer.succeed(
      GoalRoot,
      GoalRoot.of({
        started: (actor) => actor.spawn("personal", GoalSink).pipe(Effect.asVoid),
        receive: () => Effect.void,
      }),
    );
  }
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
        AgentRunner,
        options.runner ?? AgentRunner.make(() => Effect.die("Unexpected internal execution")),
      ),
      ContextQueries.layer.pipe(Layer.provide(Layer.succeed(ContextRegistry, registry))),
      emptyRecall,
      Layer.succeed(ExternalAgents, options.agents ?? { test: options.agent ?? fakeAgent() }),
      Layer.succeed(SignalDefinitions, []),
      Layer.succeed(Clock.Clock, options.clock ?? (yield* Clock.Clock)),
      Layer.succeed(ChannelWrites, {
        publish: options.publish ?? (() => Effect.die("Unexpected publication")),
      }),
    ),
  );
  yield* system.spawn("goals", GoalRoot);
  const approvals = yield* system.spawn("approvals", ApprovalQueueActor);
  const tasks = yield* system.spawn("tasks", TasksRootActor);
  yield* tasks.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
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
  status: TaskState["status"] = "uncertain",
) {
  const input = taskInput();
  const entry = yield* messages.append(input.target, input.requestId, "task.admission", input);
  const { task: _task, ...identity } = input;
  const terminal = ["completed", "failed", "cancelled", "uncertain"].includes(status);
  const inputs: TaskState["inputs"] = [
    {
      requestId: input.requestId,
      entryId: entry.id,
      receipt: { requestId: input.requestId, revision: 1 },
      status: terminal ? "completed" : "accepted",
    },
  ];
  const outcome = terminal
    ? yield* messages.append(input.target, "original-result", "task.result", {
        text: "Original result",
        status,
        inputs,
      })
    : undefined;
  const state: TaskState = {
    admission: {
      input: identity,
      entryId: entry.id,
      receipt: { requestId: input.requestId, revision: 1 },
    },
    executorPrompt: "Test policy",
    status,
    inputs,
    approvals: [],
    ...(outcome ? { outcomeEntryId: outcome.id } : {}),
    ...(status === "running" ? { session: { sessionId: "original" } } : {}),
  };
  return {
    path: input.target,
    description: "Retained Task",
    revision: 2,
    messages: [],
    state,
  } satisfies ContextRecord;
});
