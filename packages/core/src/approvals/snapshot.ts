import { Schema } from "effect";
import { ApprovalEntry, ApprovalResponse } from "./contracts.js";

export const ApprovalSnapshot = Schema.Struct({
  entries: Schema.Array(ApprovalEntry),
  revokedIds: Schema.Array(Schema.String),
});
export const ApprovalEvent = Schema.Struct({
  type: Schema.Literals(["Requested", "Revoked", "Resolved", "Acknowledged"]),
  requestId: Schema.String,
  at: Schema.String,
  response: Schema.optional(ApprovalResponse),
});
