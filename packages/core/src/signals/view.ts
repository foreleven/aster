import { SignalOccurrence } from "./reaction.js";
import { Schema } from "effect";
import { contextView } from "../context/view.js";
import { publicBusinessMessage } from "../reasoning/public-messages.js";
import { SignalDefinition } from "../config/schema.js";
export const signalView = contextView({
  matches: (path) => /^\/signals\/[^/]+$/.test(path),
  state: Schema.Struct({
    ...SignalDefinition.fields,
    goal: Schema.optional(Schema.String),
    active: Schema.Boolean,
    deleted: Schema.optional(Schema.Boolean),
    revision: Schema.Int,
    occurrences: Schema.Array(SignalOccurrence),
    nextDue: Schema.optional(Schema.Number),
  }),
  projectMessage: publicBusinessMessage,
});
export const signalsRootView = contextView({
  matches: (path) => path === "/signals",
  state: Schema.Struct({}),
});
