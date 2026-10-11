import { Effect, Schema } from "effect";
import { GoalState } from "../../goals/state/model.js";
import { ExternalAgents } from "../../tasks/execution/contracts.js";
import { defineTool } from "../define.js";

export const goalCurrent = () =>
  defineTool(
    {
      name: "goal_current",
      label: "Read Goal",
      replay: "safe",
      description:
        "Read current Goal understanding and execution capabilities. Internal Tasks can discover and query connected Contexts and memory; external executors have their own tools and require confirmation.",
      parameters: Schema.Struct({}),
    },
    () =>
      Effect.gen(function* () {
        const goal = yield* GoalState;
        return {
          ...(yield* goal.inspect),
          execution: {
            internal: {
              tool: "start_task",
              capabilities: [
                "list_contexts",
                "describe_context",
                "query_context",
                "read_query_result",
                "memory_search",
                "memory_expand",
              ],
            },
            external: {
              tool: "delegate_task",
              executors: Object.keys(yield* ExternalAgents),
              confirmationRequired: true,
            },
          },
        };
      }),
  );

export const updateSummary = () =>
  defineTool(
    {
      name: "update_summary",
      label: "Update summary",
      replay: "never",
      description:
        "Persist a concise business summary of useful findings and unfinished work. This does not complete or archive the Goal.",
      parameters: Schema.Struct({
        summary: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(6000)),
      }),
    },
    ({ summary }) => Effect.flatMap(GoalState, (goal) => goal.updateSummary(summary)),
  );
