import { goalCurrent, updateSummary } from "./state.js";
import { taskList, startTask, delegate, taskSend } from "./tasks.js";
import { sendGoalMessage } from "./messages.js";
import { signalList, createSignal, updateSignal, controlSignal } from "./signals.js";
import { memorySave } from "../../task/tools/memory.js";
import type { CoreTool } from "../../shared/define-tool.js";
import type { CurrentActors } from "../../../services/actors.js";
import type { GoalState } from "../../../goals/state/model.js";
import type { ExternalAgents } from "../../../tasks/execution/contracts.js";
import type { DurableContext } from "../../../context/store.js";
import type { ToolInvocation } from "../../shared/invocation.js";

export const goalTools = (): readonly CoreTool<
  CurrentActors | GoalState | ExternalAgents | DurableContext | ToolInvocation
>[] => [
  goalCurrent(),
  updateSummary(),
  taskList(),
  startTask(),
  delegate(),
  taskSend(),
  sendGoalMessage(),
  signalList(),
  createSignal(),
  updateSignal(),
  controlSignal(),
  memorySave(),
];
