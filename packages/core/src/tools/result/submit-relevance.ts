import { Effect } from "effect";
import { Type, type EffectTool } from "@aster/agent";
import { output } from "../define.js";

const parameters = Type.Object({ relevant: Type.Boolean(), reason: Type.String({ minLength: 1 }) });
export const submitRelevance: EffectTool<typeof parameters> = {
  name: "submit_relevance",
  replay: "safe" as const,
  label: "Goal relevance",
  description: "Decide whether the evidence concretely affects this Goal.",
  parameters,
  execute: (_id, args) =>
    Effect.succeed({
      ...output(args),
      terminate: true,
    }),
};
