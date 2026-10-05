import { Schema } from "effect";
import { contextView } from "../context/view.js";
import { publicBusinessMessage } from "../reasoning/public-messages.js";
import { SignalDefinition } from "../config/schema.js";
const optionalString = Schema.optional(Schema.String);
const SignalView = Schema.Struct({
  action: SignalDefinition.fields.action,
  slug: optionalString,
  when: optionalString,
  task: optionalString,
  agent: optionalString,
  mode: optionalString,
  goal: optionalString,
  taskId: optionalString,
  owner: optionalString,
  active: Schema.optional(Schema.Boolean),
  deleted: Schema.optional(Schema.Boolean),
  revision: Schema.optional(Schema.Number),
  nextDue: Schema.optional(Schema.Number),
  schedule: SignalDefinition.fields.schedule,
  notBefore: SignalDefinition.fields.notBefore,
  occurrences: Schema.optional(
    Schema.Array(
      Schema.Struct({
        id: Schema.String,
        text: Schema.String,
        delivered: Schema.Boolean,
        source: Schema.Struct({ path: Schema.String }),
      }),
    ),
  ),
});
export const signalView = contextView({
  matches: (path) => /^\/signals\/[^/]+$/.test(path),
  state: SignalView,
  projectMessage: publicBusinessMessage,
});
export const signalsRootView = contextView({
  matches: (path) => path === "/signals",
  state: Schema.Struct({}),
});
