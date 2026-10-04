import { Agent, AgentError, Models } from "@aster/agent";
import { Clock, Effect, type Schema } from "effect";
import type { MemoryRecall } from "../context/memory.js";
import type { ContextQueries } from "../context/queries.js";
import { withAgentCallbacks } from "../reasoning/agent-callbacks.js";
import { goalAgentPrompt } from "./agent-prompt.js";
import { decodeGoalAgentResult, finishTurnTool } from "./agent-result.js";
import { boundGoalTool, makeGoalReadTools } from "./agent-tools.js";
import { GoalReasoningError } from "./errors.js";
import type { GoalReasoner, GoalReasoningInput } from "./reasoner.js";

interface GoalReasonerOptions {
  readonly contextTokens?: number;
  readonly reserveTokens?: number;
  readonly queries?: ContextQueries["Service"];
}

const reasoningError = (cause: AgentError | GoalReasoningError | Schema.SchemaError) => {
  if (cause instanceof GoalReasoningError) return cause;
  return new GoalReasoningError({
    operation: "plan",
    cause,
    outcome: cause instanceof AgentError ? cause.outcome : undefined,
    message: cause.message,
  });
};

export const makeGoalReasoner = Effect.fnUntraced(function* (
  name: string,
  memory: MemoryRecall["Service"],
  options: GoalReasonerOptions = {},
): Effect.fn.Return<GoalReasoner, never, Models> {
  const models = yield* Models;
  const contextTokens = options.contextTokens ?? 200000;
  const reserveTokens = options.reserveTokens ?? 8192;

  // Planning can span many model/tool rounds. Its owner cancels on Goal End
  // or shutdown; a whole-run deadline would interrupt recoverable progress.
  const plan = (input: GoalReasoningInput) =>
    withAgentCallbacks((invoke) =>
      Effect.gen(function* () {
        const readTools = yield* makeGoalReadTools(input, memory, options.queries, invoke);
        const tools = [...readTools, finishTurnTool(input, invoke)];
        const agent = yield* Agent.make({
          name,
          tools: tools.map(boundGoalTool),
          resultTool: "finish_turn",
          durable: {
            ...input.durable,
            catalogueId: JSON.stringify(["aster.goal.v10", contextTokens, reserveTokens]),
            contextBudget: { contextTokens, reserveTokens },
          },
        });
        const timestamp = yield* Clock.currentTimeMillis;
        const { messages } = yield* agent.run({
          messages: [
            { role: "system", content: goalAgentPrompt, timestamp },
            ...(input.messages ?? []),
          ],
        });
        return yield* decodeGoalAgentResult(messages, input.durable);
      }),
    ).pipe(Effect.provideService(Models, models), Effect.mapError(reasoningError));
  return { plan };
});
