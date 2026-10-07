import { testConversations } from "./conversation-fixtures.js";
import { AgentConversations } from "@aster/agent";
import { AgentRunner, AgentError, type AgentInvocation, type AgentResult } from "@aster/agent";
import { Context, ConfigProvider, Effect, Layer, Option, Schema } from "effect";

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

class ModelResponder extends Context.Service<
  ModelResponder,
  (invocation: AgentInvocation) => Effect.Effect<AgentResult, AgentError>
>()("test/ModelResponder") {}

/** Compose fake native responders beneath the runner's single SDK adaptation boundary. */
export const modelReplyLayer = (
  resultTool: string | undefined,
  execute: (invocation: AgentInvocation) => Effect.Effect<AgentResult, AgentError>,
) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const previous = yield* Effect.serviceOption(ModelResponder);
      const respond = (options: AgentInvocation) =>
        options.resultTool === resultTool
          ? execute(options)
          : Option.isSome(previous)
            ? previous.value(options)
            : Effect.die(new Error(`Unexpected model invocation: ${options.resultTool}`));
      return Context.make(ModelResponder, respond).pipe(
        Context.add(AgentRunner, AgentRunner.make(respond)),
      );
    }),
  );

// The SDK tool boundary is exercised by the fake model, just like a real model tool call.
const callTool = (input: AgentInvocation, name: string, args: object) =>
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
    modelReplyLayer("submit_relevance", () =>
      Effect.succeed(
        agentResult("submit_relevance", { relevant: true, reason: "Relevant test evidence" }),
      ),
    ),
    modelReplyLayer(undefined, (input) =>
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
            messages: input.messages.filter((message) => message.role !== "system"),
          })
          .pipe(Effect.mapError(agentFailure));
        yield* callTool(input, "update_summary", { summary: response.progress });
        return { messages: [] };
      }),
    ),
  );
