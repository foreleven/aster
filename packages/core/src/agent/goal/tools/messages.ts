import { Effect } from "effect";
import { SendGoalMessageInput } from "../../../goals/message-contracts.js";
import { messageGoal } from "../../../goals/messages.js";
import { defineTool } from "../../shared/define-tool.js";
import { toolOrigin } from "../../shared/invocation.js";

export const sendGoalMessage = () =>
  defineTool(
    {
      name: "send_goal_message",
      label: "Send Goal message",
      replay: "never",
      description:
        "Send source-qualified internal evidence to another Goal. This is communication, not a Task, user instruction or external-action authorization. Returns durable acceptance.",
      parameters: SendGoalMessageInput,
    },
    (input, callId) =>
      Effect.gen(function* () {
        return yield* messageGoal(yield* toolOrigin(callId), input);
      }),
  );
