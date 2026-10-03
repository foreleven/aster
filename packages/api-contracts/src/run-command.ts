import { CausalChain } from "./notification.js";
import { Schema } from "effect";
import { CommandIdentifier, CommandReceipt, ContextRevision } from "./command.js";

export const RunPath = Schema.String.check(
  Schema.isPattern(
    /^\/(?:runs\/personal--[a-f0-9]{64}|(?:signals|goals)\/[a-z0-9][a-z0-9-]*\/runs\/[a-zA-Z0-9_-]+)$/,
  ),
);
export const PersonalResumeRunInput = Schema.Struct({
  requestId: CommandIdentifier,
  causationId: CommandIdentifier,
  expectedRevision: ContextRevision,
  runPath: RunPath,
  runRevision: ContextRevision,
});
export type PersonalResumeRunInput = typeof PersonalResumeRunInput.Type;
export const ResumeRunDeliveryInput = Schema.Struct({
  operation: Schema.Literal("resumeRun"),
  requestId: CommandIdentifier,
  causationId: CommandIdentifier,
  source: Schema.Literal("/personal"),
  causal: Schema.optional(CausalChain),
  target: RunPath,
  expectedRevision: ContextRevision,
  createdAt: Schema.NonEmptyString,
});
export type ResumeRunDeliveryInput = typeof ResumeRunDeliveryInput.Type;
export const RunResumption = Schema.Struct({
  input: ResumeRunDeliveryInput,
  receipt: CommandReceipt,
  status: Schema.Literals(["pending", "delivered"]),
  error: Schema.optional(Schema.String),
});
export type RunResumption = typeof RunResumption.Type;

export const ExecutionResumption = Schema.Struct({
  input: ResumeRunDeliveryInput,
  receipt: CommandReceipt,
  status: Schema.Literals(["pending", "resuming", "done", "unknown"]),
});
