import { ActorSystem } from "@aster/actor";
import { AgentConversations, AgentRunner } from "@aster/agent";
import { Deferred, Effect, Layer } from "effect";
import { ContextRegistry } from "../src/context/registry.js";
import { ContextQueries } from "../src/context/queries.js";
import { ContextsActor } from "../src/context/queries-actor.js";
import { MemoryActor } from "../src/memory/actor.js";
import { MemoryBackend, type MemoryRecall } from "../src/memory/contracts.js";
import { ContextCaptures } from "../src/memory/capture.js";
import { GoalSettings } from "../src/config/settings.js";
import { ExternalAgents } from "../src/tasks/execution/contracts.js";
import { GoalSignals } from "../src/signals/goal-owner.js";
import { GoalsRootActor } from "../src/goals/root.js";
import { makeContextRegistry } from "../src/testing/context.js";

/** Real domain query owners, with fake backend work and no activated model conversations. */
export const toolSystem = Effect.fnUntraced(function* (
  options: {
    registry?: ContextRegistry["Service"];
    messages?: AgentConversations["Service"];
    queries?: ContextQueries["Service"];
    memory?: MemoryRecall["Service"];
    goals?: GoalSettings["Service"]["definitions"];
  } = {},
) {
  const registry = options.registry ?? (yield* makeContextRegistry());
  const messages = options.messages ?? (yield* AgentConversations.makeMemory());
  const queries =
    options.queries ?? (yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer)));
  const system = yield* ActorSystem.make().pipe(
    ActorSystem.provide(
      Layer.succeed(ContextRegistry, registry),
      Layer.succeed(AgentConversations, messages),
      Layer.succeed(ContextQueries, queries),
      ContextCaptures.layer,
      Layer.succeed(MemoryBackend, {
        description: "Test memory",
        retrieval: "bm25",
        drain: Effect.void,
        capture: () => Effect.void,
        recall: options.memory ?? {
          search: () => Effect.succeed([]),
          expand: () => Effect.succeed([]),
        },
      }),
      Layer.succeed(GoalSettings, {
        definitions: options.goals ?? [],
        reasoning: { model: "test" },
      }),
      Layer.succeed(ExternalAgents, {}),
      Layer.succeed(GoalSignals, {
        applySignal: () => Effect.die("Unexpected signal"),
        deactivate: () => Effect.void,
      }),
      Layer.succeed(
        AgentRunner,
        AgentRunner.make(() => Effect.die("Query fixture cannot execute a model")),
      ),
    ),
  );
  const contexts = yield* system.spawn("contexts", ContextsActor);
  yield* contexts.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
  const memory = yield* system.spawn("memory", MemoryActor);
  yield* memory.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
  if (options.goals?.length) {
    const goalActivation = yield* Deferred.make<void>();
    const goals = yield* system.spawn("goals", GoalsRootActor, { metadata: { goalActivation } });
    yield* goals.awaitStarted;
    // Query fixtures inspect restored snapshots; production routing does not wait for children.
    for (const goal of options.goals)
      yield* (yield* system.select(`/user/goals/${goal.slug}`).resolve()).awaitStarted;
  }
  return { system, registry, messages, queries, contexts, memory };
});
