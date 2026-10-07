import { ApplicationError, ApprovalResponse, PublicApprovalEntry } from "@aster/core/contracts";
import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/rpc";

export const ApprovalRpcs = RpcGroup.make(
  Rpc.make("ListApprovals", {
    success: Schema.Array(PublicApprovalEntry),
    error: ApplicationError,
  }),
  Rpc.make("RespondToApproval", {
    payload: { id: Schema.String, response: ApprovalResponse },
    success: Schema.Void,
    error: ApplicationError,
  }),
);
