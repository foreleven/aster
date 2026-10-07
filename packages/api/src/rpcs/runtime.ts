import { ApplicationError } from "@aster/core/contracts";
import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/rpc";

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

export const RuntimeRpcs = RpcGroup.make(
  Rpc.make("InspectRuntime", { success: RuntimeSnapshot, error: ApplicationError }),
);
