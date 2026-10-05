import { Schema } from "effect";
import { contextView } from "../context/view.js";
import { publicBusinessMessage } from "../reasoning/public-messages.js";
import { GoalState } from "./state.js";
const optionalString = Schema.optional(Schema.String);
const GoalView = Schema.Struct({
  completionOrigin: GoalState.fields.completionOrigin,
  title: GoalState.fields.title,
  status: optionalString,
  description: optionalString,
  completionCriteria: GoalState.fields.completionCriteria,
  summary: optionalString,
  progress: optionalString,
  lastError: optionalString,
  historyCount: Schema.optional(Schema.Number),
});
export const goalView = contextView({
  matches: (path) => /^\/goals\/[^/]+$/.test(path),
  state: GoalView,
  projectMessage: publicBusinessMessage,
});
export const goalsRootView = contextView({
  matches: (path) => path === "/goals",
  state: Schema.Struct({}),
});
