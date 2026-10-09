import {
  CommandIdentifier,
  ContextRevision,
  ApplicationError,
  RecoveryInput,
  CommandReceipt,
} from "@aster/core/contracts";
import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/rpc";

/** Per-target routing decisions; independent of work and delivery lifecycle. */
export const ReactionMatch = Schema.TaggedUnion({
  Matched: { target: Schema.String, reason: Schema.String },
  NotMatched: { target: Schema.String, reason: Schema.String },
  Failed: { target: Schema.String, error: Schema.String },
});
export type ReactionMatch = typeof ReactionMatch.Type;

export const ProcessingOwner = Schema.Literal("system-one");
export type ProcessingOwner = typeof ProcessingOwner.Type;
export const ProcessingSnapshot = Schema.Struct({
  owner: ProcessingOwner,
  revision: ContextRevision,
  entries: Schema.Array(
    Schema.Struct({
      id: CommandIdentifier,
      kind: Schema.Literals(["screening", "reaction-delivery"]),
      workId: Schema.optional(CommandIdentifier),
      source: Schema.String,
      target: Schema.String,
      status: Schema.String,
      attempts: Schema.optional(Schema.Int),
      error: Schema.optional(Schema.String),
      matches: Schema.optional(Schema.Array(ReactionMatch)),
    }),
  ),
});
export type ProcessingSnapshot = typeof ProcessingSnapshot.Type;

export const ProcessingRpcs = RpcGroup.make(
  Rpc.make("InspectProcessing", {
    payload: { owner: ProcessingOwner },
    success: ProcessingSnapshot,
    error: ApplicationError,
  }),
  Rpc.make("RecoverProcessing", {
    payload: RecoveryInput,
    success: CommandReceipt,
    error: ApplicationError,
  }),
);
