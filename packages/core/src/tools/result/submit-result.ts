import { Effect } from "effect";
import { Type, type EffectTool } from "@aster/agent";
import { output } from "../define.js";

/** This tool returns to the current invocation; there is no domain command to send. */
export const submitResult = (schema: object): EffectTool => ({
  name: "submit_result",
  replay: "safe",
  label: "Submit result",
  description: "Return the requested structured result",
  parameters: Type.Unsafe(schema),
  execute: (_id, args) => Effect.succeed({ ...output(args), terminate: true }),
});
