import { Effect } from "effect";
import { CurrentActors } from "../../services/actors.js";
import { Type } from "@aster/agent";
import type { TaskMessage } from "@aster/api-contracts";
import { deliverTask } from "../../tasks/delivery.js";
import { commandTool } from "../define.js";
import { actorTask } from "./schema.js";

export type TaskOrigin = Pick<TaskMessage, "requestId" | "source" | "createdAt" | "causal">;
export const startTask = (origin: (callId: string) => TaskOrigin) =>
  commandTool(
    {
      name: "start_task",
      replay: "never",
      label: "Start asynchronous Task",
      description:
        "Start sustained work with the internal Agent, send a message to a Goal, or delegate externally. Delegate tasks require user confirmation and reply to the specified Goal. Keep the same request identity on unknown outcomes.",
      parameters: Type.Object({ task: actorTask }),
    },
    (args, id) =>
      CurrentActors.pipe(
        Effect.flatMap((actors) => deliverTask(actors, { ...args, ...origin(id) })),
      ),
  );
