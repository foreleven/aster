import { ask } from "../actors.js";
import { Type } from "@aster/agent";
import { Effect } from "effect";
import type { SignalRootCommand, SignalCommandReply } from "../../signals/protocol.js";
import type { TaskOrigin } from "../task/start-task.js";
import { commandTool } from "../define.js";
import { signalDefinition } from "./schema.js";

export const setSignal = (goal: string, origin: (callId: string) => TaskOrigin) =>
  commandTool(
    {
      name: "set_signal",
      replay: "never",
      label: "Manage Signal",
      description:
        "Create, change, pause, resume or delete a Goal signal or timer. It executes its configured Task when triggered. Read the current version before changing an existing Signal.",
      parameters: Type.Object({
        id: Type.String({ pattern: "^[a-z0-9][a-z0-9-]*$" }),
        change: Type.Union([
          Type.Object({ operation: Type.Literal("create"), definition: signalDefinition }),
          Type.Object({
            operation: Type.Literal("update"),
            version: Type.Integer(),
            definition: signalDefinition,
          }),
          Type.Object({
            operation: Type.Union([
              Type.Literal("pause"),
              Type.Literal("resume"),
              Type.Literal("delete"),
            ]),
            version: Type.Integer(),
          }),
        ]),
      }),
    },
    ({ id, change }, callId) => {
      const { source, requestId, remainingAgentTurns } = origin(callId);
      return ask<SignalRootCommand, SignalCommandReply>("/user/signals", (replyTo) => ({
        _tag: "Change",
        input: {
          source,
          requestId,
          remainingAgentTurns,
          target: `/signals/${id.startsWith(`${goal}--`) ? id : `${goal}--${id}`}`,
          change,
        },
        replyTo,
      })).pipe(
        Effect.flatMap((reply) =>
          reply._tag === "Accepted" ? Effect.succeed(reply.receipt) : Effect.fail(reply.error),
        ),
      );
    },
  );
