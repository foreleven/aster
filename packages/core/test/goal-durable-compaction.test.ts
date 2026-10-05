import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import { AgentRunner, Models, type ResolvedModel } from "@aster/agent";
import { Effect, Layer, Schema } from "effect";
import { runGoalConversation, conversationText, defineContext, contextView } from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

test("durable Goal compacts its native transcript and finishes the same request", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-goal-compaction-"));
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
                    text: "Retained work checkpoint: continue reviewing the source; no actions were executed.",
                  },
                ]
              : generations <= 24
                ? Array.from({ length: 6 }, (_, page) => ({
                    type: "toolCall" as const,
                    id: `call-${generations}-${page}`,
                    name: "read_context",
                    arguments: { path: "/source", offset: page * 2000 },
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
    const registry = yield* makeContextRegistry({
      loadAll: () => [
        {
          path: "/source",
          description: "Evidence",
          state: { evidence: "detail ".repeat(3000) },
          messages: [],
        },
      ],
      save: () => {},
    });
    // The fixture is explicitly public evidence, using a registered Context projection.
    yield* registry.register(
      "/source",
      defineContext({
        state: Schema.Struct({ evidence: Schema.String }),
        message: Schema.Never,
        view: contextView({
          state: Schema.Struct({ evidence: Schema.String }),
          message: Schema.Never,
        }),
      }),
    );
    return yield* runGoalConversation({
      goal: { slug: "test", description: "Review evidence" },
      model: "test",
      registry,
      memory: { search: () => Effect.succeed([]), expand: () => Effect.succeed([]) },
      executors: [],
      contextTokens: 48000,
      reconcile: false,
      storageDirectory: directory,
      input: {
        inputId: "review",
        goalSlug: "test",
        ordinal: 1,
        receivedAt: "2026-10-01T00:00:00Z",
        status: "pending",
        payload: { _tag: "UserInput", text: "Review the evidence" },
      },
      update: () => Effect.die("No update expected"),
      startTask: () => Effect.die("No Task expected"),
      signal: () => Effect.die("No Signal expected"),
    });
  }).pipe(
    Effect.provide(AgentRunner.layer.pipe(Layer.provide(models))),
    Effect.timeout("15 seconds"),
  );
  const result = await Effect.runPromise(run);
  assert.equal(conversationText(result.messages), "Review completed");
  assert.ok(summaries > 0, "Native compaction must run before the Goal gives up");
  assert.equal(sawSummary, true);
  assert.match(await readFile(join(directory, "main.jsonl"), "utf8"), /"kind":"pi.compaction"/);
  const calls = generations + summaries;
  assert.deepEqual(await Effect.runPromise(run), result);
  assert.equal(generations + summaries, calls, "Completed request replay does not call the model");
});
