import { Models, type ResolvedModel } from "@aster/agent";
import { AgentRunner } from "@aster/agent/agent";
import { AgentConversations, DurableHarness } from "@aster/agent/harness";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { GoalState } from "../src/goals/state/model.js";
import { CurrentActors } from "../src/services/actors.js";
import { testConversations } from "./conversation-fixtures.js";
import { toolSystem } from "./tool-fixtures.js";

import { Effect, Layer } from "effect";
import {
  ContextRegistry,
  ExternalAgents,
  GoalActor,
  GoalAgent,
  GoalSettings,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

test("durable Goal compacts its native transcript and finishes the same request", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-goal-compaction-"));
  const conversations = testConversations(directory);
  t.after(() => rm(directory, { recursive: true, force: true }));
  let generations = 0;
  let summaries = 0;
  let sawSummary = false;
  const model: ResolvedModel["model"] = {
    id: "test",
    name: "test",
    provider: "test",
    api: "openai-completions",
    baseUrl: "http://unused",
    reasoning: false,
    input: ["text"],
    contextWindow: 120000,
    maxTokens: 4096,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  };
  const models = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "unused",
        stream: (_model, context) => {
          const isSummary = context.messages.some(
            (message) =>
              message.role === "system" &&
              typeof message.content === "string" &&
              message.content.includes("You are a context summarization assistant"),
          );
          if (isSummary) summaries++;
          else generations++;
          if (!isSummary && JSON.stringify(context.messages).includes("Retained work checkpoint"))
            sawSummary = true;
          const inputTokens = Math.ceil(Buffer.byteLength(JSON.stringify(context.messages)) / 4);
          const message: AssistantMessage = {
            role: "assistant",
            provider: "test",
            model: "test",
            api: "openai-completions",
            timestamp: 0,
            stopReason: isSummary || generations > 24 ? "stop" : "toolUse",
            content: isSummary
              ? [
                  {
                    type: "text",
                    text: "Retained work checkpoint: continue coordinating the project; no actions were executed.",
                  },
                ]
              : generations <= 24
                ? Array.from({ length: 6 }, (_, page) => ({
                    type: "toolCall" as const,
                    id: `call-${generations}-${page}`,
                    name: "goal_current",
                    arguments: {},
                  }))
                : [{ type: "text", text: "Review completed" }],
            usage: {
              input: inputTokens,
              output: 100,
              cacheRead: 0,
              cacheWrite: 0,
              totalTokens: inputTokens + 100,
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
            },
          };
          const stream = createAssistantMessageEventStream();
          stream.push({
            type: "done",
            reason: isSummary || generations > 24 ? "stop" : "toolUse",
            message,
          });
          return stream;
        },
      }),
  });
  const run = Effect.gen(function* () {
    const registry = yield* makeContextRegistry();
    yield* registry.register("/goals/test", GoalActor.contextDefinition);
    const { system } = yield* toolSystem({ registry, messages: conversations });
    return yield* GoalAgent.use((agent) =>
      agent.converse({
        goal: { slug: "test", description: "Review evidence" },
        input: {
          kind: "UserInput",
          entryId: 0,
          inputId: "review",
          receivedAt: "2026-10-01T00:00:00Z",
          remainingAgentTurns: 4,
          status: "pending",
          payload: { _tag: "UserInput", text: "Review the evidence" },
        },
      }),
    ).pipe(
      Effect.provide([
        GoalAgent.layer,
        GoalState.layer("/goals/test", {
          slug: "test",
          description: "Coordinate the project and its ongoing tasks. ".repeat(50),
        }),
      ]),
      Effect.provideService(AgentConversations, conversations),
      Effect.provideService(CurrentActors, system),
      Effect.provideService(ContextRegistry, registry),
      Effect.provideService(GoalSettings, {
        definitions: [],
        reasoning: { model: "test", contextTokens: 48000 },
      }),
      Effect.provideService(ExternalAgents, {}),
    );
  }).pipe(
    Effect.provide(
      Layer.mergeAll(AgentRunner.layer, DurableHarness.layer).pipe(
        Layer.provide(models),
        Layer.provide(Layer.succeed(AgentConversations, conversations)),
      ),
    ),
    Effect.timeout("15 seconds"),
    Effect.scoped,
  );
  const result = await Effect.runPromise(run);
  assert.equal(result, "Review completed");
  assert.ok(summaries > 0, "Native compaction must run before the Goal gives up");
  assert.equal(sawSummary, true);
  assert.match(
    await readFile(
      join(directory, createHash("sha256").update("/goals/test").digest("hex"), "main.jsonl"),
      "utf8",
    ),
    /"kind":"pi.compaction"/,
  );
  const calls = generations + summaries;
  assert.deepEqual(await Effect.runPromise(run), result);
  assert.equal(generations + summaries, calls, "Completed request replay does not call the model");
});
