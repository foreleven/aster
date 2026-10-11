import type { AssistantMessage } from "@earendil-works/pi-ai";
import { Effect, Match } from "effect";

/** Log provider blocks as each round arrives, without adding them to the public Timeline. */
export const logGoalResponse = (
  message: AssistantMessage,
  execution: {
    readonly goalPath: string;
    readonly phase: "conversation" | "gate";
    readonly inputId?: string;
    readonly intentId?: string;
  },
) =>
  Effect.forEach(
    message.content,
    (block) =>
      Match.value(block).pipe(
        Match.when({ type: "thinking" }, (block) =>
          Effect.logInfo("Goal agent thinking", { thinking: block.thinking }),
        ),
        Match.when({ type: "text" }, (block) =>
          Effect.logInfo("Goal agent text", { text: block.text }),
        ),
        Match.when({ type: "toolCall" }, (block) =>
          Effect.logInfo("Goal agent tool call", {
            toolCallId: block.id,
            tool: block.name,
            arguments: block.arguments,
          }),
        ),
        Match.orElse(() => Effect.void),
      ),
    { discard: true },
  ).pipe(
    Effect.annotateLogs({ ...execution, model: message.model, stopReason: message.stopReason }),
  );
