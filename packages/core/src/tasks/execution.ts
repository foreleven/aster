import { TaskPreparationError } from "./errors.js";
import { Effect } from "effect";
import { AgentRunner } from "@aster/agent";
import { MemoryRecall } from "../memory/contracts.js";
import { internalAgentSettings } from "../config/settings.js";
import { makeStructuredReasoning } from "../reasoning/structured.js";
import { makeExecutionInputBuilder } from "./build-execution-input.js";
import { type TaskExecution, ExternalAgents } from "./model.js";
import { SystemOneClient } from "../decisions/system-one.js";
import { makeExecutionGate } from "../signals/execution-gate.js";

export type TaskExecutionServices = AgentRunner | MemoryRecall | ExternalAgents | SystemOneClient;

export const makeTaskExecution = Effect.fn("Task.execution")(function* () {
  const settings = yield* internalAgentSettings;
  const memory = yield* MemoryRecall;
  const run = yield* makeStructuredReasoning(settings.model, memory);
  const client = yield* SystemOneClient;
  const agents = yield* ExternalAgents;
  const ready = makeExecutionGate(client, (name) => ({
    supportedAgent: !!agents[name],
    workspace: "Isolated local workspace",
    capabilities: agents[name]?.capabilities ?? "Unsupported executor",
  }));
  const execution: TaskExecution = {
    buildExecutionInput: makeExecutionInputBuilder({
      memory,
      run,
      executorPrompt: (name) => agents[name]?.executorPrompt,
    }),
    checkReadiness: (...args) =>
      ready(...args).pipe(
        Effect.mapError(
          (cause) =>
            new TaskPreparationError({ operation: "readiness", cause, message: cause.message }),
        ),
      ),
  };
  return execution;
});
