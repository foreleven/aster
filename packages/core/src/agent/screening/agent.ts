import { schemaTool, AgentError } from "../index.js";
import { AgentRuntime } from "../runtime/ordinary-runtime.js";
import { Clock, Effect, Schema } from "effect";
import type { GoalDefinition } from "../../goals/contracts.js";
import type { GoalIntent } from "../../goals/screening/intent.js";
import { outputLanguageInstruction } from "../../config/language.js";
import { logGoalResponse } from "../goal/extensions.js";
import { instructions } from "../runtime/instructions.js";
const ContextRelevanceDecision = Schema.Struct({
  relevant: Schema.Boolean,
  reason: Schema.NonEmptyString,
});
const submitContextRelevance = schemaTool({
  name: "submit_context_relevance",
  replay: "safe" as const,
  label: "Context relevance",
  description:
    "Submit whether this Context change concretely affects the current Goal, with a reason.",
  parameters: ContextRelevanceDecision,
  execute: (_id, args) =>
    Effect.succeed({
      content: [{ type: "text", text: "Relevance decision accepted." }],
      details: args,
      terminate: true,
    }),
});
/** Read-only second gate. Rejected context changes never enter the Goal conversation. */
export const goalAgentGate = Effect.fn("Goal.agentGate")(function* (
  model: string,
  goal: GoalDefinition,
  intent: GoalIntent,
  language: string,
) {
  const runner = yield* AgentRuntime;
  const timestamp = yield* Clock.currentTimeMillis;
  const result = yield* runner.run({
    name: model,
    resultTool: submitContextRelevance.name,
    tools: [submitContextRelevance],
    onResponse: (message) =>
      logGoalResponse(message, {
        goalPath: `/goals/${goal.slug}`,
        phase: "gate",
        intentId: intent.intentId,
      }),
    messages: [
      {
        role: "system" as const,
        timestamp,
        content: [instructions.screening, outputLanguageInstruction(language)].join("\n\n"),
      },
      { role: "user" as const, timestamp, content: JSON.stringify({ goal, change: intent }) },
    ],
  });
  const answer = result.messages.findLast(
    (message) =>
      message.role === "toolResult" &&
      message.toolName === submitContextRelevance.name &&
      !message.isError,
  );
  return yield* Schema.decodeUnknownEffect(ContextRelevanceDecision)(
    answer?.role === "toolResult" ? answer.details : undefined,
  ).pipe(
    Effect.mapError(
      (cause) =>
        new AgentError("Goal relevance gate returned an invalid decision", [], {
          cause,
          outcome: "failed",
        }),
    ),
  );
});

export const ScreeningAgent = {
  make: Effect.fn("ScreeningAgent.make")(function* (model: string, language: string) {
    const runner = yield* AgentRuntime;
    return {
      screen: (goal: GoalDefinition, intent: GoalIntent) =>
        goalAgentGate(model, goal, intent, language).pipe(
          Effect.provideService(AgentRuntime, runner),
        ),
    };
  }),
};
