import type { ManagedExternalAgent } from "./external-agent.js";
import { Config, Effect, Layer, Schema } from "effect";
import {
  ConfigLocation,
  ExternalAgents,
  ProcessEnvironment,
  type ExternalAgent,
} from "@aster/core";
import { makeCodexAgent } from "./codex/agent.js";
import { makeDoubaoAgent } from "./doubao/delegation.js";
import { Models } from "@aster/agent";
import { homedir } from "node:os";
import { join } from "node:path";
import { makePiAgent } from "./pi/agent.js";

export const piAgentSettings = Config.schema(
  Schema.optional(
    Schema.Struct({
      model: Schema.NonEmptyString,
      storageDirectory: Schema.optional(Schema.NonEmptyString),
    }),
  ),
  ["agents", "pi"],
);

export const makeExternalAgents = Effect.fn("ExternalAgents.make")(function* (
  sharedPi?: ExternalAgent,
) {
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
  const pi = yield* piAgentSettings;
  const executor =
    sharedPi ??
    (pi
      ? yield* makePiAgent({
          model: pi.model,
          directory: pi.storageDirectory ?? join(homedir(), ".aster", "executions", "pi"),
          shardId: "aster-executions",
        })
      : undefined);
  return { codex, "doubao-delegate": doubao, ...(executor ? { pi: executor } : {}) };
});

export const ExternalAgentsLive = {
  layer: Layer.effect(ExternalAgents, makeExternalAgents()).pipe(Layer.provide(Models.configured)),
};
