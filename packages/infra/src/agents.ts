import type { ManagedExternalAgent } from "./external-agent.js";
import { Config, Effect, Schema } from "effect";
import { ProcessEnvironment } from "@aster/core";
import { makeCodexAgent } from "./codex/agent.js";
import { makeDoubaoAgent } from "./doubao/delegation.js";
import { agentEnvironment } from "./process/environment.js";

export const makeExternalAgents = Effect.fn("ExternalAgents.make")(function* () {
  const childEnvironment = agentEnvironment(yield* ProcessEnvironment);
  const prompt = yield* Config.schema(Schema.optional(Schema.String), [
    "agents",
    "doubao",
    "prompt",
  ]);
  const acquire = (make: () => ManagedExternalAgent) =>
    Effect.acquireRelease(Effect.sync(make), (agent) => agent.close());
  const codex = yield* acquire(() => makeCodexAgent(childEnvironment));
  const doubao = yield* acquire(() =>
    makeDoubaoAgent(childEnvironment, undefined, undefined, prompt),
  );
  return { codex, "doubao-delegate": doubao };
});
