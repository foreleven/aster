import { contextView, ContextQueryResult } from "@aster/core";
import { Schema } from "effect";
import { AppName } from "./commands.js";

export const AppsState = Schema.Struct({ apps: Schema.Array(Schema.String) });
export const AppState = Schema.Struct({
  app: AppName,
  mode: Schema.Literal("query-only"),
  lastResult: Schema.optional(ContextQueryResult),
});
export const appsView = contextView({ matches: (path) => path === "/apps", state: AppsState });
export const appView = contextView({
  matches: (path) => path === "/apps/xiaohongshu" || path === "/apps/ctrip",
  state: AppState,
});
