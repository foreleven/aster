import { DurableContext } from "@aster/core";
import { Schema } from "effect";
import { GoalSnapshot } from "@aster/core";
import { Models, type ResolvedModel } from "@aster/agent";
import { AgentRunner } from "@aster/agent/agent";
import { AgentConversations, DurableHarness } from "@aster/agent/harness";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
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
  GoalAgent,
  GoalSettings,
  MemoryRecall,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

test("reopened Goal sessions keep their policy and history while tools read the new turn", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-goal-current-"));
  const conversations = testConversations(directory);
  t.after(() => rm(directory, { recursive: true, force: true }));
  let calls = 0;
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
          // Pi retains prior system updates in history; the latest update is authoritative.
          const currentPolicy = JSON.stringify(
            context.messages.findLast((message) => message.role === "system"),
          );
          assert.match(currentPolicy, new RegExp(`PRIVATE_goal_${turn}`));
          assert.doesNotMatch(currentPolicy, new RegExp(`PRIVATE_goal_${1 - turn}`));
          assert.doesNotMatch(policy, /PRIVATE_summary_/);
          if (reading) {
            const latest = context.messages.findLast((message) => message.role === "user");
            assert.equal(latest?.content, "Continue the Goal");
          }
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
              snapshot: {
                revision: 0,
                path: "/goals/test",
                description: "Goal",
                state: {
                  definition: { slug: "test", description: `PRIVATE_goal_${turn}` },
                  status: "active",
                  inputs: [],
                  receipts: [],
                  tasks: [],
                  summary: `PRIVATE_summary_${turn}`,
                },
                messages: [],
              },
              events: [],
            },
          ],
          save: () => {},
        });
        yield* registry.register("/goals/test", { state: GoalSnapshot, message: Schema.Never });
        const { system } = yield* toolSystem({
          registry,
          messages: conversations,
        });
        return yield* GoalAgent.use((agent) =>
          agent.converse({
            goal: { slug: "test", description: `PRIVATE_goal_${turn}` },
            input: {
              kind: "UserInput",
              entryId: 0,
              inputId: `turn-${turn}`,
              receivedAt: "2026-10-01T00:00:00Z",
              remainingAgentTurns: 4,
              status: "pending",
              payload: { _tag: "UserInput", text: "Continue the Goal" },
            },
          }),
        ).pipe(
          Effect.provide([
            GoalAgent.layer,
            GoalState.layer("/goals/test", { slug: "test", description: `PRIVATE_goal_${turn}` }),
          ]),
          Effect.provideService(AgentConversations, conversations),
          Effect.provideService(CurrentActors, system),
          Effect.provideService(ContextRegistry, registry),
          Effect.provideService(DurableContext, registry.backend),
          Effect.provideService(GoalSettings, { definitions: [], reasoning: { model: "test" } }),
          Effect.provideService(MemoryRecall, {
            search: () => Effect.succeed([]),
            expand: () => Effect.succeed([]),
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
        Effect.timeout("10 seconds"),
        Effect.scoped,
      ),
    );
  const first = await run(0);
  const next = await run(1);
  assert.equal(first, "Recorded turn 0");
  assert.equal(next, "Recorded turn 1");
  assert.equal(calls, 4);
  assert.deepEqual(await run(0), first);
  assert.equal(calls, 4, "Replaying a completed turn must not call the model again");
});
