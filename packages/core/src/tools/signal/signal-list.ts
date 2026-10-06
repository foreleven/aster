import { askQuery } from "../actors.js";
import { Type } from "@aster/agent";
import type { SignalRootCommand } from "../../signals/actors.js";
import { queryTool } from "../define.js";

export const signalList = (goal: string) =>
  queryTool(
    {
      name: "signal_list",
      replay: "safe",
      label: "Read Signals",
      description: "Read Goal signals and timers, including their current revisions.",
      parameters: Type.Object({}),
    },
    () =>
      askQuery<SignalRootCommand>("/user/signals", (replyTo) => ({
        _tag: "ListByGoal",
        goal,
        replyTo,
      })),
  );
