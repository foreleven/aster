import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import { AgentRunner, Models, type ResolvedModel } from "@aster/agent";
import { Effect, Layer } from "effect";
import { runGoalConversation, makeContextRegistry, conversationText } from "../src/index.js";

test("reopened Goal sessions keep their policy and history while tools read the new turn", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-goal-current-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  let calls = 0;
  let initialPolicy: string | undefined;
  const model: ResolvedModel["model"] = {
    id: "test",
    name: "test",
    provider: "test",
    api: "openai-completions",
    baseUrl: "http://unused",
    reasoning: false,
    input: ["text"],
    contextWindow: 200000,
    maxTokens: 4096,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  };
  const models = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "unused",
        stream: (_model, context) => {
          calls++;
          const turn = Math.floor((calls - 1) / 2);
          const reading = calls % 2 === 1;
          const policy = JSON.stringify(
            context.messages.filter((message) => message.role === "system"),
          );
          assert.match(policy, /You are the user's personal assistant/);
          assert.doesNotMatch(policy, /PRIVATE_/);
          initialPolicy ??= policy;
          assert.equal(policy, initialPolicy);
          if (calls === 3) {
            assert.match(JSON.stringify(context.messages), /PRIVATE_summary_0/);
            assert.doesNotMatch(JSON.stringify(context.messages), /PRIVATE_summary_1/);
          }
          if (!reading) {
            const response = context.messages.findLast(
              (message) => message.role === "toolResult" && message.toolName === "goal_current",
            );
            assert.ok(response?.role === "toolResult");
            const content = response.content[0]!;
            assert.ok(content.type === "text");
            const page = JSON.parse(content.text);
            const current = page;
            assert.equal(current.state.summary, `PRIVATE_summary_${turn}`);
            assert.equal(current.goal.description, `PRIVATE_goal_${turn}`);
          }
          const message: AssistantMessage = {
            role: "assistant",
            provider: "test",
            model: "test",
            api: "openai-completions",
            timestamp: 0,
            stopReason: reading ? "toolUse" : "stop",
            content: reading
              ? [{ type: "toolCall", id: `call-${calls}`, name: "goal_current", arguments: {} }]
              : [{ type: "text", text: `Recorded turn ${turn}` }],
            usage: {
              input: 100,
              output: 100,
              cacheRead: 0,
              cacheWrite: 0,
              totalTokens: 200,
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
            },
          };
          const stream = createAssistantMessageEventStream();
          stream.push({ type: "done", reason: reading ? "toolUse" : "stop", message });
          return stream;
        },
      }),
  });
  const run = (turn: number) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({
          loadAll: () => [
            {
              path: "/goals/test",
              description: "Goal",
              state: { summary: `PRIVATE_summary_${turn}` },
              messages: [],
            },
          ],
          save: () => {},
        });
        return yield* runGoalConversation({
          goal: { slug: "test", description: `PRIVATE_goal_${turn}` },
          model: "test",
          registry,
          memory: { search: () => Effect.succeed([]), expand: () => Effect.succeed([]) },
          executors: [],
          reconcile: false,
          storageDirectory: directory,
          input: {
            inputId: `turn-${turn}`,
            goalSlug: "test",
            ordinal: turn + 1,
            receivedAt: "2026-10-01T00:00:00Z",
            status: "pending",
            payload: { _tag: "UserInput", text: "Continue the Goal" },
          },
          update: () => Effect.die("No update expected"),
          startTask: () => Effect.die("No Task expected"),
          signal: () => Effect.die("No Signal expected"),
        });
      }).pipe(
        Effect.provide(AgentRunner.layer.pipe(Layer.provide(models))),
        Effect.timeout("10 seconds"),
      ),
    );
  const first = await run(0);
  const next = await run(1);
  assert.equal(conversationText(first.messages), "Recorded turn 0");
  assert.equal(conversationText(next.messages), "Recorded turn 1");
  assert.equal(calls, 4);
  assert.deepEqual(await run(0), first);
  assert.equal(calls, 4, "Replaying a completed turn must not call the model again");
});
