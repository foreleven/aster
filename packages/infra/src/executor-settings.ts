import { Config, Schema } from "effect";

export const executorSettings = Config.schema(Schema.optional(Schema.String), [
  "agents",
  "doubao",
  "prompt",
]);
