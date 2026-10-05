import { Schema } from "effect";
import { ContextRevision } from "./command.js";

const InspectionTaskPath = Schema.String.check(Schema.isPattern(/^\/tasks\/[^/]+$/));
/** Business inspection deliberately excludes provider metadata, native frames and credential handles. */
export const TaskInspection = Schema.Struct({
  path: InspectionTaskPath,
  revision: ContextRevision,
  taskPath: Schema.String,
  agent: Schema.String,
  status: Schema.Literals([
    "ready",
    "rejected",
    "awaiting-confirmation",
    "submitting",
    "uncertain",
    "running",
    "waiting_input",
    "failed",
    "cancelled",
    "unknown",
    "completed",
  ]),
  instructions: Schema.String,
  sources: Schema.Array(Schema.String),
  hasExecution: Schema.Boolean,
  result: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
  resumptions: Schema.optional(
    Schema.Array(
      Schema.Struct({
        requestId: Schema.String,
        status: Schema.Literals(["pending", "resuming", "done", "unknown"]),
      }),
    ),
  ),
  messages: Schema.Array(
    Schema.Struct({ id: Schema.Int, kind: Schema.String, text: Schema.String, at: Schema.String }),
  ),
  requests: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      kind: Schema.Literals(["approval", "input"]),
      prompt: Schema.String,
      responseStatus: Schema.Literals(["pending", "received", "sending", "sent", "uncertain"]),
    }),
  ),
});
export type TaskInspection = typeof TaskInspection.Type;
