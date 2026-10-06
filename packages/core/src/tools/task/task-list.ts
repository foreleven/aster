import { Type } from "@aster/agent";
import { Effect } from "effect";
import { GoalState } from "../../goals/state/model.js";
import { queryTool } from "../define.js";

export const taskList = () =>
  queryTool(
    {
      name: "task_list",
      replay: "safe",
      label: "Read Tasks",
      description:
        "Read this Goal's asynchronous Tasks, including completed work that can continue.",
      parameters: Type.Object({}),
    },
    () => Effect.flatMap(GoalState, (goal) => goal.listTasks),
  );
