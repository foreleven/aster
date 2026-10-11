import { testSettings } from "./settings-fixtures.js";
import { DurableContext } from "@aster/core";
import { makeHarness } from "./harness-fixtures.js";
import { DurableHarness, AgentConversations } from "@aster/agent/harness";
import { ActorSystem } from "@aster/actor";
import { AgentRunner } from "@aster/agent/agent";
import { Context, Effect, Layer } from "effect";
import { ContextQueries } from "../src/queries/registry.js";
import { QueryExecution } from "../src/queries/execution.js";
import { MemoryRecall } from "../src/memory/recall.js";
import { MemoryActor } from "../src/memory/actor.js";
import { MemoryBackend } from "../src/memory/contracts.js";
import { ContextCaptures } from "../src/memory/capture.js";
import { GoalSettings } from "../src/goals/settings.js";
import { ExternalAgents } from "../src/tasks/execution/contracts.js";
import { GoalsRootActor } from "../src/goals/root.js";
import { makeTestContext } from "../src/testing/context.js";

/** Real domain query owners, with fake backend work and model execution held pending. */
export const toolSystem = Effect.fnUntraced(function* (
  options: {
    registry?: import("../src/testing/context.js").TestContext;
    messages?: AgentConversations["Service"];
    queries?: ContextQueries["Service"];
    memory?: MemoryRecall["Service"];
    goals?: GoalSettings["Service"]["definitions"];
  } = {},
) {
  const registry = options.registry ?? (yield* makeTestContext());
  const messages = options.messages ?? (yield* AgentConversations.makeMemory());
  const queries =
    options.queries ?? (yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer)));
  const memoryBackend = MemoryBackend.of({
    description: "Test memory",
    retrieval: "bm25",
    drain: Effect.void,
    capture: () => Effect.void,
    recall: options.memory ?? {
      search: () => Effect.succeed([]),
      expand: () => Effect.succeed([]),
    },
  });
  const execution = yield* Layer.build(QueryExecution.layer).pipe(
    Effect.map(Context.get(QueryExecution)),
    Effect.provideService(ContextQueries, queries),
    Effect.provideService(AgentConversations, messages),
  );
  const recall = yield* Layer.build(MemoryRecall.layer).pipe(
    Effect.map(Context.get(MemoryRecall)),
    Effect.provideService(MemoryBackend, memoryBackend),
  );
  const system = yield* ActorSystem.make().pipe(
    ActorSystem.provide(
      testSettings,
      Layer.mergeAll(Layer.succeed(DurableContext, registry.backend)),
      Layer.succeed(AgentConversations, messages),
      Layer.succeed(ContextQueries, queries),
      ContextCaptures.layer,
      Layer.succeed(MemoryBackend, memoryBackend),
      Layer.succeed(QueryExecution, execution),
      Layer.succeed(MemoryRecall, recall),
      Layer.succeed(GoalSettings, {
        definitions: options.goals ?? [],
        reasoning: { model: "test", contextTokens: 200000, reserveTokens: 8192 },
      }),
      Layer.succeed(ExternalAgents, {}),

      Layer.succeed(
        DurableHarness,
        makeHarness(() => Effect.never),
      ),
      Layer.succeed(
        AgentRunner,
        AgentRunner.make(() => Effect.die("Query fixture cannot execute a model")),
      ),
    ),
  );
  const memory = yield* system.spawn("memory", MemoryActor);
  yield* memory.awaitStarted;
  if (options.goals?.length) {
    const goals = yield* system.spawn("goals", GoalsRootActor);
    yield* goals.awaitStarted;
    // Query fixtures inspect restored snapshots; production routing does not wait for children.
    for (const goal of options.goals)
      yield* (yield* system.select(`/user/goals/${goal.slug}`).resolve()).awaitStarted;
  }
  return { system, registry, messages, queries, execution, recall, memory };
});
