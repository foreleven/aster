import { Type } from "@aster/agent";
import { Effect } from "effect";
import { GoalState } from "../../goals/state/model.js";
import { queryTool } from "../define.js";

export const goalCurrent = (executors: readonly string[]) =>
  queryTool(
    {
      name: "goal_current",
      replay: "safe",
      label: "Read Goal",
      description: "Read current public Goal state and available Task executors.",
      parameters: Type.Object({}),
    },
    () =>
      Effect.gen(function* () {
        const goal = yield* GoalState;
        return { ...(yield* goal.inspect), executors };
      }),
  );
