import type { ManagedExternalAgent } from "./external-agent.js";
import { Config, Effect, Layer, Schema } from "effect";
import { ConfigLocation, ExternalAgents, ProcessEnvironment } from "@aster/core";
import { makeCodexAgent } from "./codex/agent.js";
import { makeDoubaoAgent } from "./doubao/delegation.js";

export const ExternalAgentsLive = {
  layer: Layer.effect(
    ExternalAgents,
    Effect.gen(function* () {
      const { envPath } = yield* ConfigLocation;
      const environment = yield* ProcessEnvironment;
      const privateKeys = new Set(environment.privateKeys);
      const childEnvironment = Object.fromEntries(
        Object.entries(environment.values).filter(([key]) => !privateKeys.has(key)),
      );
      const prompt = yield* Config.schema(Schema.optional(Schema.String), [
        "agents",
        "doubao",
        "prompt",
      ]);
      const acquire = (make: () => ManagedExternalAgent) =>
        Effect.acquireRelease(Effect.sync(make), (agent) => agent.close());
      const codex = yield* acquire(() =>
        makeCodexAgent(envPath, undefined, undefined, childEnvironment),
      );
      const doubao = yield* acquire(() =>
        makeDoubaoAgent(envPath, undefined, undefined, prompt, childEnvironment),
      );
      return { codex, "doubao-delegate": doubao };
    }),
  ),
};
