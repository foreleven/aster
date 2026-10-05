import { Schema } from "effect";
import { contextView } from "../context/view.js";
import { publicBusinessMessage } from "../reasoning/public-messages.js";
import { WritebackOperation, PreparedTask, RunResumption } from "@aster/api-contracts";
import { SignalDefinition } from "../config/schema.js";
const optionalString = Schema.optional(Schema.String);
const RunView = Schema.Struct({
  writeback: Schema.optional(WritebackOperation),
  status: optionalString,
  signalSlug: optionalString,
  sourcePath: optionalString,
  task: Schema.optional(PreparedTask),
  definition: Schema.optional(SignalDefinition),
  outcomeText: optionalString,
  approvals: Schema.optional(Schema.Array(Schema.String)),
  resumptions: Schema.optional(Schema.Array(RunResumption)),
});
export const runView = contextView({
  matches: (path) => /^\/(?:runs\/[^/]+|(?:goals|signals)\/[^/]+\/runs\/[^/]+)$/.test(path),
  state: RunView,
  projectMessage: publicBusinessMessage,
});
export const tasksRootView = contextView({
  matches: (path) => path === "/runs",
  state: Schema.Struct({}),
});
