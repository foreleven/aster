import { Schema } from "effect";
import { ContextRevision } from "./command.js";

export const DelegationPath = Schema.String.check(Schema.isPattern(/^\/delegations\/[^/]+$/));
/** Business inspection deliberately excludes provider metadata, native frames and credential handles. */
export const DelegationInspection = Schema.Struct({
  path: DelegationPath,
  revision: ContextRevision,
  runPath: Schema.String,
  agent: Schema.String,
  status: Schema.Literals([
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
  requests: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      kind: Schema.Literals(["approval", "input"]),
      prompt: Schema.String,
      responseStatus: Schema.Literals(["pending", "received", "sending", "sent", "uncertain"]),
    }),
  ),
});
export type DelegationInspection = typeof DelegationInspection.Type;
