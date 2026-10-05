import { Option, Schema } from "effect";
import { PublicContext } from "@aster/api-contracts";
import type { CapturePolicy } from "../memory/capture.js";
import { RunState } from "./run-state.js";
import { runView } from "./view.js";
const Triggered = Schema.Struct({
  type: Schema.Literal("Triggered"),
  sourceContext: PublicContext,
});
export const runCapture: CapturePolicy = {
  matches: (path) => runView.matches!(path),
  capture: (input) => {
    const state = Schema.decodeUnknownOption(RunState)(input.state);
    if (Option.isNone(state)) return undefined;
    const record = { ...input, state: state.value };
    const trigger = record.messages
      .map((message) => Schema.decodeUnknownOption(Triggered)(message))
      .find(Option.isSome)?.value;
    const terminal = [
      "completed",
      "uncertain",
      "failed",
      "cancelled",
      "rejected",
      "preparation-failed",
      "blocked",
    ].includes(String(record.state.status));
    return trigger
      ? {
          sessionId: `${record.path}:${terminal ? `outcome:${record.state.status}` : "trigger"}`,
          records: [record, trigger.sourceContext],
        }
      : undefined;
  },
};
