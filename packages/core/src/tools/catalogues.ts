import type { GoalState } from "../goals/state/model.js";
import type { CurrentActors } from "../services/actors.js";
import type { CoreTool } from "./define.js";
import { listContexts } from "./context/list-contexts.js";
import { describeContext } from "./context/describe-context.js";
import { queryContext } from "./context/query-context.js";
import { readQueryResult } from "./context/read-query-result.js";
import { memorySearch } from "./memory/memory-search.js";
import { memoryExpand } from "./memory/memory-expand.js";
import { goalCurrent } from "./goal/goal-current.js";
import { updateSummary } from "./goal/update-summary.js";
import { startTask, type TaskOrigin } from "./task/start-task.js";
import { taskList } from "./task/task-list.js";
import { taskSend } from "./task/task-send.js";
import { signalList } from "./signal/signal-list.js";
import { setSignal } from "./signal/set-signal.js";

export const contextTools = (): readonly CoreTool[] => [listContexts(), describeContext()] as const;
export const contextQueryTools = (
  owner: string,
  requestId: (callId: string) => string,
): readonly CoreTool[] => [queryContext(owner, requestId), readQueryResult(owner)] as const;
export const memoryTools = (): readonly CoreTool[] => [memorySearch(), memoryExpand()];
export const taskTools = (
  owner: string,
  requestId: (callId: string) => string,
): readonly CoreTool[] => [
  ...contextTools(),
  ...contextQueryTools(owner, requestId),
  ...memoryTools(),
];
export const goalTools = (options: {
  goal: string;
  origin: (callId: string) => TaskOrigin;
  executors: readonly string[];
}): readonly CoreTool<CurrentActors | GoalState>[] => {
  const source = `/goals/${options.goal}`;
  const requestId = (id: string) => options.origin(id).requestId;
  // The primary conversation coordinates work. Evidence retrieval belongs to Task execution.
  return [
    goalCurrent(options.executors),
    updateSummary(),
    taskList(),
    startTask(options.origin),
    taskSend(source, requestId),
    signalList(options.goal),
    setSignal(options.goal, options.origin),
  ];
};
