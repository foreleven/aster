import { Effect } from "effect";
import { CurrentActors } from "../../services/actors.js";
import { Type } from "@aster/agent";
import { followupTask } from "../../tasks/delivery.js";
import { commandTool } from "../define.js";

export const taskSend = (source: string, requestId: (callId: string) => string) =>
  commandTool(
    {
      name: "task_send",
      replay: "never",
      label: "Continue Task",
      description:
        "Send a correction or follow-up to an existing Task. Completed Tasks can continue the same work.",
      parameters: Type.Object({
        target: Type.String({ pattern: "^/tasks/[a-f0-9]{64}$" }),
        text: Type.String({ minLength: 1 }),
      }),
    },
    (args, id) =>
      CurrentActors.pipe(
        Effect.flatMap((actors) =>
          followupTask(actors, { ...args, source, requestId: requestId(id) }),
        ),
      ),
  );
