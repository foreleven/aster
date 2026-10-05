import { Option, Schema } from "effect";
import type { CapturePolicy } from "../memory/capture.js";
import { RunState } from "./run-state.js";
import { runView } from "./view.js";
export const runCapture: CapturePolicy = {
  matches: (path) => runView.matches!(path),
  capture: (input) => {
    const state = Schema.decodeUnknownOption(RunState)(input.state);
    if (Option.isNone(state)) return undefined;
    const record = { ...input, state: state.value };
    const evidence = record.state.admission.input.evidence;
    const terminal = ["completed", "uncertain", "failed", "cancelled", "rejected"].includes(
      String(record.state.status),
    );
    return evidence
      ? {
          sessionId: `${record.path}:${terminal ? `outcome:${record.state.status}` : "trigger"}`,
          records: [record, evidence],
        }
      : undefined;
  },
};
