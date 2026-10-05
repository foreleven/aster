import { Effect, Option, Schema } from "effect";
import { TaskDeliveryInput } from "@aster/api-contracts";
import type { AgentConversations } from "@aster/agent";
import type { CapturePolicy } from "../memory/capture.js";
import { TaskState } from "./state.js";
import { taskView } from "./view.js";

/** Resolve frozen evidence from Pi; later source edits cannot change a capture. */
export const taskCapture = (messages: AgentConversations["Service"]): CapturePolicy => ({
  matches: (path) => taskView.matches!(path),
  capture: Effect.fn("Task.capture")(function* (record) {
    const decoded = Schema.decodeUnknownOption(TaskState)(record.state);
    if (Option.isNone(decoded)) return undefined;
    const state = decoded.value;
    const entry = yield* messages.get(record.path, state.admission.entryId).pipe(Effect.orDie);
    const admission = Schema.decodeUnknownSync(TaskDeliveryInput)(entry.data);
    if (!admission.evidence) return undefined;
    return {
      sessionId: `${record.path}:${state.outcomeEntryId === undefined ? "trigger" : `outcome:${state.outcomeEntryId}`}`,
      records: [record, admission.evidence],
    };
  }),
});
