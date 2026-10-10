import { Schema } from "effect";
import { AppName } from "./commands.js";

export const AppsState = Schema.Struct({ apps: Schema.Array(Schema.String) });
export const AppState = Schema.Struct({
  app: AppName,
  mode: Schema.Literal("query-only"),
});
