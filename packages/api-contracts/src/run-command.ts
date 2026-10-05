import { Schema } from "effect";
import { CommandIdentifier, CommandReceipt, ContextRevision } from "./command.js";
export const RunPath = Schema.String.check(Schema.isPattern(/^\/runs\/[a-f0-9]{64}$/));
/** Explicit operator authorization, addressed directly to the execution owner. */
export const ResumeRunDeliveryInput = Schema.Struct({
  requestId: CommandIdentifier,
  target: RunPath,
  expectedRevision: ContextRevision,
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
