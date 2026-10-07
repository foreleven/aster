import type { ManagedExternalAgent } from "./external-agent.js";
import { Config, Effect, Schema } from "effect";
import { ConfigLocation, ProcessEnvironment, type ExternalAgent } from "@aster/core";
import { makeCodexAgent } from "./codex/agent.js";
import { makeDoubaoAgent } from "./doubao/delegation.js";
import { agentEnvironment } from "./process/environment.js";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
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
  const { baseDir } = yield* ConfigLocation;
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
  const pi = yield* piAgentSettings;
  const executor =
    sharedPi ??
    (pi
      ? yield* makePiAgent({
          model: pi.model,
          directory: pi.storageDirectory
            ? resolve(baseDir, pi.storageDirectory)
            : join(homedir(), ".aster", "executions", "pi"),
          shardId: "aster-executions",
        })
      : undefined);
  return { codex, "doubao-delegate": doubao, ...(executor ? { pi: executor } : {}) };
});
