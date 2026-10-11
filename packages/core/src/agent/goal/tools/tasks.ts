import { Effect, Schema } from "effect";
import { GoalState } from "../../../goals/state/model.js";
import { InternalTaskInput, ExternalTaskInput, TaskPath } from "../../../tasks/contracts.js";
import { startInternalTask, delegateTask, sendTaskInput } from "../../../tasks/operations.js";
import { defineTool } from "../../shared/define-tool.js";
import { ToolInvocation, toolOrigin, toolRequestId } from "../../shared/invocation.js";

export const taskList = () =>
  defineTool(
    {
      name: "task_list",
      label: "Read Tasks",
      replay: "safe",
      description:
        "Read this Goal's Tasks with their titles and statuses, including completed work that can continue. Reuse relevant work with task_send.",
      parameters: Schema.Struct({}),
    },
    () => Effect.flatMap(GoalState, (goal) => goal.listTasks),
  );

export const startTask = () =>
  defineTool(
    {
      name: "start_task",
      label: "Start internal Task",
      replay: "never",
      description:
        "Start internal read-only research. The Task has list_contexts, describe_context, query_context, read_query_result, memory_search, memory_expand and memory_save. Supply a short descriptive title, instructions, known Context path hints and user constraints; hints may be empty. Feedback automatically returns to this Goal. Only successful acceptance means work was admitted; do not announce starting before calling this tool or poll for completion.",
      parameters: InternalTaskInput,
    },
    (input, callId) =>
      Effect.gen(function* () {
        return yield* startInternalTask(yield* toolOrigin(callId), input);
      }),
  );

export const delegate = () =>
  defineTool(
    {
      name: "delegate_task",
      label: "Delegate external Task",
      replay: "never",
      description:
        "Admit work with a short descriptive title for a configured external executor through confirmation. External executors do not inherit internal Context tools: retrieve needed connected evidence with an internal Task first. Feedback automatically returns to this Goal.",
      parameters: ExternalTaskInput,
    },
    (input, callId) =>
      Effect.gen(function* () {
        return yield* delegateTask(yield* toolOrigin(callId), input);
      }),
  );

export const taskSend = () =>
  defineTool(
    {
      name: "task_send",
      label: "Continue Task",
      replay: "never",
      description:
        "Send a correction or follow-up to this Goal's existing Task. Completed Tasks can continue the same work.",
      parameters: Schema.Struct({ taskPath: TaskPath, text: Schema.NonEmptyString }),
    },
    ({ taskPath, text }, callId) =>
      Effect.gen(function* () {
        const invocation = yield* ToolInvocation;
        return yield* sendTaskInput({
          target: taskPath,
          text,
          source: invocation.ownerPath,
          requestId: yield* toolRequestId(callId),
        });
      }),
  );
