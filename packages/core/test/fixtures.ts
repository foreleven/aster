import { Effect, Layer } from "effect";
import { ExternalAgents, type ExternalAgent } from "../src/index.js";
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
