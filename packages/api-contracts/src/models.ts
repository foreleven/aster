import { Schema } from "effect";

export const InputRequest = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literals(["approval", "input"]),
  prompt: Schema.String,
  options: Schema.optional(Schema.Array(Schema.String)),
  questions: Schema.optional(
    Schema.Array(
      Schema.Struct({
        id: Schema.String,
        prompt: Schema.String,
        options: Schema.optional(Schema.Array(Schema.String)),
        // Providers explicitly opt into custom text or restrict a question to one selection.
        allowOther: Schema.optional(Schema.Boolean),
        multiple: Schema.optional(Schema.Boolean),
      }),
    ),
  ),
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
});
export type InputRequest = typeof InputRequest.Type;
export const ApprovalResponse = Schema.Struct({
  decision: Schema.optional(Schema.Literals(["approve", "reject"])),
  text: Schema.optional(Schema.String),
  answers: Schema.optional(Schema.Record(Schema.String, Schema.Array(Schema.String))),
});
export type ApprovalResponse = typeof ApprovalResponse.Type;

export const ApprovalEntry = Schema.Struct({
  id: Schema.String,
  target: Schema.String,
  contextPath: Schema.String,
  kind: Schema.Literals(["confirmation", "approval", "input"]),
  request: InputRequest,
  status: Schema.Literals(["pending", "resolved", "acknowledged", "revoked"]),
  response: Schema.optional(ApprovalResponse),
});
export type ApprovalEntry = typeof ApprovalEntry.Type;

export const PublicContext = Schema.Struct({
  projection: Schema.optional(
    Schema.Struct({
      version: Schema.Literal(1),
      visibility: Schema.Literals(["public", "restricted"]),
      reason: Schema.optional(Schema.Literals(["missing-policy", "invalid-data"])),
    }),
  ),
  path: Schema.String,
  /** Absent only on legacy snapshots; the first versioned commit starts at one. */
  revision: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  description: Schema.String,
  state: Schema.ObjectKeyword,
  messages: Schema.Array(Schema.Unknown),
});
export type PublicContext = typeof PublicContext.Type;
export const HistoryPage = Schema.Struct({
  entries: Schema.Array(
    Schema.Struct({ seq: Schema.Int, at: Schema.String, message: Schema.Unknown }),
  ),
  total: Schema.Int,
  nextBefore: Schema.NullOr(Schema.Int),
});
export type HistoryPage = typeof HistoryPage.Type;
const FailureSummary = Schema.Struct({
  message: Schema.String,
  stack: Schema.optional(Schema.String),
});
export const RuntimeEvent = Schema.Union([
  Schema.TaggedStruct("CommandProcessed", {
    path: Schema.String,
    incarnation: Schema.String,
    commandTag: Schema.optional(Schema.String),
    success: Schema.Boolean,
    timestamp: Schema.String,
  }),
  Schema.TaggedStruct("DeadLetter", {
    target: Schema.String,
    incarnation: Schema.String,
    commandTag: Schema.optional(Schema.String),
    reason: Schema.String,
    timestamp: Schema.String,
  }),
  Schema.TaggedStruct("ActorRestarting", {
    path: Schema.String,
    incarnation: Schema.String,
    cause: FailureSummary,
    timestamp: Schema.String,
  }),
  Schema.TaggedStruct("ActorStopped", {
    path: Schema.String,
    incarnation: Schema.String,
    cause: Schema.optional(FailureSummary),
    timestamp: Schema.String,
  }),
]);
export type RuntimeEvent = typeof RuntimeEvent.Type;
export const RuntimePhase = Schema.Literals(["starting", "ready", "failed", "stopping"]);
export type RuntimePhase = typeof RuntimePhase.Type;
export const RuntimeSnapshot = Schema.Struct({
  storageOwners: Schema.optional(
    Schema.Array(
      Schema.Struct({
        ownerId: Schema.String,
        leaseId: Schema.String,
        storageId: Schema.String,
        pid: Schema.Int,
        status: Schema.Literals(["held", "quarantined"]),
      }),
    ),
  ),
  phase: RuntimePhase,
  actors: Schema.Array(
    Schema.Struct({
      path: Schema.String,
      parent: Schema.String,
      incarnation: Schema.String,
      contextPath: Schema.optional(Schema.String),
      status: Schema.Literals(["running", "stopping", "stopped"]),
      phase: Schema.Literals(["starting", "running", "restarting", "stopping", "stopped"]),
      processing: Schema.Boolean,
      currentCommand: Schema.optional(Schema.String),
      pendingEffects: Schema.Int,
      failures: Schema.Int,
      lastError: Schema.optional(Schema.String),
      mailboxSize: Schema.Int,
      processed: Schema.Int,
      restarts: Schema.Int,
      lastActivity: Schema.String,
    }),
  ),
  events: Schema.Array(RuntimeEvent),
});
export type RuntimeSnapshot = typeof RuntimeSnapshot.Type;
export class ApplicationError extends Schema.TaggedError<ApplicationError>()("ApplicationError", {
  kind: Schema.Literals(["not-found", "invalid-input", "unavailable", "conflict"]),
  message: Schema.String,
}) {}
