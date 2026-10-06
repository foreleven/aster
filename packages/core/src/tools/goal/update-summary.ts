import { Type } from "@aster/agent";
import { Effect } from "effect";
import { GoalState } from "../../goals/state/model.js";
import { commandTool } from "../define.js";

export const updateSummary = () =>
  commandTool(
    {
      name: "update_summary",
      replay: "safe",
      label: "Update summary",
      description:
        "Record a concise business summary, preserving useful findings and unfinished work.",
      parameters: Type.Object({ summary: Type.String({ minLength: 1, maxLength: 6000 }) }),
    },
    ({ summary }) => Effect.flatMap(GoalState, (goal) => goal.updateSummary(summary)),
  );
