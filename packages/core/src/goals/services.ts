import { Effect, Layer } from "effect";
import { Models } from "@aster/agent";
import { GoalSettings } from "../config/settings.js";
import { ContextRegistry } from "../context/registry.js";
import { MemoryRecall } from "../context/memory.js";
import { SignalCommands } from "../signals/commands.js";
import { GoalHistoryStore } from "./history.js";
import { makeGoalReasoner } from "./agent-reasoner.js";
import { GoalRuntime, makeGoalRuntime } from "./runtime.js";

export const goalRuntimeLayer = Layer.effect(
  GoalRuntime,
  Effect.gen(function* () {
    const settings = yield* GoalSettings;
    const registry = yield* ContextRegistry;
    const commands = yield* SignalCommands;
    const history = yield* GoalHistoryStore;
    if (!settings.definitions.length)
      return makeGoalRuntime(
        settings,
        registry,
        commands,
        {
          plan: () => Effect.die(new Error("No Goals configured")),
        },
        history,
      );
    const models = yield* Models;
    yield* models.resolve(settings.reasoning!.model);
    const reasoner = yield* makeGoalReasoner(
      settings.reasoning!.model,
      yield* MemoryRecall,
      settings.reasoning,
    );
    return makeGoalRuntime(settings, registry, commands, reasoner, history);
  }),
);
