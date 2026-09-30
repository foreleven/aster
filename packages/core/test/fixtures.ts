import { Effect, Layer } from "effect";
import { ExternalAgents, TaskPreparation, type ExternalAgent } from "../src/index.js";
export const preparationLayer = Layer.succeed(TaskPreparation, {
  prepare: (definition, source) =>
    Effect.sync(() => ({
      instructions: definition.task,
      input: [{ content: JSON.stringify(source.state), sources: [source.path] }],
    })),
  ready: () => Effect.sync(() => true),
});
export const fakeAgent = (overrides: Partial<ExternalAgent> = {}): ExternalAgent => ({
  capabilities: "Test executor",
  submit: () => Effect.sync(() => ({ sessionId: "test-session", runId: "test-run" })),
  status: () => Effect.sync(() => ({ state: "running" })),
  wait: () => Effect.sync(() => ({ state: "completed", result: { text: "done" } })),
  resume: (session) => Effect.sync(() => session),
  respond: () => Effect.sync(() => {}),
  ...overrides,
});
export const agentsLayer = Layer.succeed(ExternalAgents, { "doubao-delegate": fakeAgent() });
