import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import { Models, type ResolvedModel } from "@aster/agent";
import { Effect, Layer } from "effect";
import { makeGoalReasoner } from "../src/index.js";

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
            stopReason: isSummary ? "stop" : "toolUse",
            content: isSummary
              ? [
                  {
                    type: "text",
                    text: "Retained work checkpoint: continue reviewing the source; no actions were executed.",
                  },
                ]
              : [
                  {
                    type: "toolCall",
                    id: `call-${generations}`,
                    name: generations <= 24 ? "read_context" : "submit_plan",
                    arguments:
                      generations <= 24
                        ? { path: "/source" }
                        : {
                            disposition: "no_change",
                            progress: "Review completed",
                            completed: false,
                            evidence: ["/source"],
                            taskChanges: [],
                            signalChanges: [],
                          },
                  },
                ],
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
          stream.push({ type: "done", reason: isSummary ? "stop" : "toolUse", message });
          return stream;
        },
      }),
  });
  const run = Effect.gen(function* () {
    const reasoner = yield* makeGoalReasoner(
      "test",
      {
        search: () => Effect.succeed([]),
        expand: () => Effect.succeed([]),
      },
      { contextTokens: 48000 },
    );
    return yield* reasoner.plan({
      goal: { slug: "test", description: "Review evidence" },
      current: { path: "/goals/test", description: "Review evidence", state: {}, messages: [] },
      contexts: {
        "/source": {
          path: "/source",
          description: "Evidence",
          state: { evidence: "detail ".repeat(3000) },
          messages: [],
        },
      },
      signals: [],
      reason: "Review",
      durable: { sessionId: "test", requestId: "review", storageDirectory: directory },
    });
  }).pipe(Effect.provide(models), Effect.timeout("15 seconds"));
  const result = await Effect.runPromise(run);
  assert.equal(result.progress, "Review completed");
  assert.ok(summaries > 0, "Native compaction must run before the Goal gives up");
  assert.equal(sawSummary, true);
  assert.match(await readFile(join(directory, "main.jsonl"), "utf8"), /"kind":"pi.compaction"/);
  const calls = generations + summaries;
  assert.deepEqual(await Effect.runPromise(run), result);
  assert.equal(generations + summaries, calls, "Completed request replay does not call the model");
});
