import { Schema } from "effect";

/** Native invocation provenance shares the existing owner-bound conversation store. */
export const failureRequestId = (id: string) => JSON.stringify(["aster.agent.failure", id]);
export const resolutionRequestId = (id: string) =>
  JSON.stringify(["aster.agent.failure.resolved", id]);
export const FailureEvidence = Schema.Struct({
  requestId: Schema.String,
  message: Schema.String,
  outcome: Schema.Literals(["failed", "unknown"]),
  operationId: Schema.optional(Schema.String),
});
export const FailureResolution = Schema.Struct({
  requestId: Schema.String,
  operationId: Schema.String,
});
