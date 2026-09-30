import { TaskPreparationError } from "./errors.js";
import { Context, Effect, Layer } from "effect";
import { Models } from "@aster/agent";
import { MemoryRecall } from "../context/memory.js";
import { internalAgentSettings } from "../config/settings.js";
import { makeInternalAgent } from "./preparation.js";
import { TaskPreparation, ExternalAgents } from "./model.js";
import { SystemOneClient } from "../decisions/system-one.js";
import { makeExecutionGate } from "../signals/execution-gate.js";

export class InternalAgent extends Context.Service<
  InternalAgent,
  Effect.Success<ReturnType<typeof makeInternalAgent>>
>()("tasks/InternalAgent") {
  static readonly layer = Layer.effect(
    InternalAgent,
    Effect.gen(function* () {
      const settings = yield* internalAgentSettings;
      const models = yield* Models;
      yield* models.resolve(settings.model);
      return yield* makeInternalAgent(settings.model, yield* MemoryRecall, settings.executorPrompt);
    }),
  );
}

export const taskPreparationLayer = Layer.effect(
  TaskPreparation,
  Effect.gen(function* () {
    const internal = yield* InternalAgent;
    const client = yield* SystemOneClient;
    const agents = yield* ExternalAgents;
    const ready = makeExecutionGate(client, (name) => ({
      supportedAgent: !!agents[name],
      workspace: "Isolated local workspace",
      capabilities: agents[name]?.capabilities ?? "Unsupported executor",
    }));
    return {
      prepare: internal.prepare,
      ready: (...args: Parameters<typeof ready>) =>
        ready(...args).pipe(
          Effect.mapError(
            (cause) =>
              new TaskPreparationError({ operation: "readiness", cause, message: cause.message }),
          ),
        ),
    };
  }),
);
