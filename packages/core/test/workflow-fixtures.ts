import type { HarnessCall } from "./harness-fixtures.js";
import { makeHarness } from "./harness-fixtures.js";
import { DurableHarness, AgentConversations } from "@aster/agent/harness";
import { testConversations } from "./conversation-fixtures.js";
import { AgentError, type AgentResult } from "@aster/agent";
import { AgentRunner, type AgentInvocation } from "@aster/agent/agent";
import { ConfigProvider, Effect, Layer, Schema } from "effect";

import { MemoryRecall, GoalSettings, type ContextInput } from "../src/index.js";

export const emptyRecall = Layer.succeed(MemoryRecall, {
  search: () => Effect.succeed({ results: [] }),
  expand: () => Effect.succeed({ results: [] }),
});
export const reasoningConfig = ConfigProvider.layerAdd(
  ConfigProvider.fromUnknown({
    config: { agent: { model: "test" } },
  }),
);
export const agentResult = (toolName: string, details: unknown): AgentResult => ({
  messages: [
    {
      role: "toolResult",
      toolCallId: "test",
      toolName,
      details: Schema.decodeUnknownSync(Schema.Json)(details),
      content: [],
      isError: false,
      timestamp: 0,
    },
  ],
});
const agentFailure = (cause: Error) => new AgentError(cause.message, [], { cause });

export const modelReplyLayer = (
  resultTool: string,
  execute: (invocation: AgentInvocation) => Effect.Effect<AgentResult, AgentError>,
) =>
  Layer.succeed(
    AgentRunner,
    AgentRunner.make((request) =>
      request.resultTool === resultTool
        ? execute(request)
        : Effect.die(new Error(`Unexpected result tool: ${request.resultTool}`)),
    ),
  );

export const harnessReplyLayer = (
  execute: (invocation: HarnessCall) => Effect.Effect<AgentResult, AgentError>,
) => Layer.succeed(DurableHarness, makeHarness(execute));

// The SDK tool boundary is exercised by the fake model, just like a real model tool call.
const callTool = (input: HarnessCall, name: string, args: object) =>
  Effect.tryPromise({
    try: (signal) => input.tools!.find((tool) => tool.name === name)!.execute("test", args, signal),
    catch: (cause) => new AgentError(String(cause), [], { cause }),
  }).pipe(
    Effect.map((result) => {
      const content = result.content.find((part) => part.type === "text");
      return JSON.parse(content?.type === "text" ? content.text : "null");
    }),
  );
export interface GoalScenario {
  readonly definitions: GoalSettings["Service"]["definitions"];
  readonly reasoner: {
    readonly plan: (input: {
      current: ContextInput;
      messages: readonly import("@aster/agent").AgentMessage[];
    }) => Effect.Effect<
      { progress: string; completed: boolean; evidence: readonly string[] },
      Error
    >;
  };

  readonly history?: AgentConversations["Service"];
  readonly contextTokens?: number;
  readonly reserveTokens?: number;
}
export const goalWorkflowLayer = (scenario: GoalScenario) =>
  Layer.mergeAll(
    emptyRecall,
    Layer.succeed(GoalSettings, {
      definitions: scenario.definitions,
      reasoning: {
        model: "test",
        contextTokens: scenario.contextTokens,
        reserveTokens: scenario.reserveTokens,
      },
    }),
    Layer.succeed(AgentConversations, scenario.history ?? testConversations()),
    modelReplyLayer("submit_context_relevance", () =>
      Effect.succeed(
        agentResult("submit_context_relevance", {
          relevant: true,
          reason: "Relevant test evidence",
        }),
      ),
    ),
    harnessReplyLayer((input) =>
      Effect.gen(function* () {
        const current = yield* callTool(input, "goal_current", {});
        const response = yield* scenario.reasoner
          .plan({
            current: {
              path: `/goals/${current.goal.slug}`,
              description: current.goal.description,
              state: current.state,
              messages: [],
            },
            messages: [{ role: "user", content: input.content, timestamp: 0 }],
          })
          .pipe(Effect.mapError(agentFailure));
        yield* callTool(input, "update_summary", { summary: response.progress });
        return { messages: [] };
      }),
    ),
  );
