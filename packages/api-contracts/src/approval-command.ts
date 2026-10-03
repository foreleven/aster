import { CausalChain } from "./notification.js";
import { RunPath } from "./run-command.js";
import { DelegationPath } from "./delegation.js";
import { Schema } from "effect";
import { ApprovalResponse } from "./models.js";
import { CommandIdentifier, CommandReceipt, ContextRevision } from "./command.js";

/** Only explicit user operations can enter this path; model results cannot propose decisions. */
export const PersonalApprovalResponseInput = Schema.Struct({
  requestId: CommandIdentifier,
  causationId: CommandIdentifier,
  expectedRevision: ContextRevision,
  approvalsRevision: ContextRevision,
  approvalId: CommandIdentifier,
  response: ApprovalResponse,
});
export type PersonalApprovalResponseInput = typeof PersonalApprovalResponseInput.Type;
export const ApprovalDeliveryInput = Schema.Struct({
  operation: Schema.Literal("respondApproval"),
  requestId: CommandIdentifier,
  causationId: CommandIdentifier,
  source: Schema.Literal("/personal"),
  causal: Schema.optional(CausalChain),
  target: Schema.Literal("/approvals"),
  expectedRevision: ContextRevision,
  createdAt: Schema.NonEmptyString,
  approvalId: CommandIdentifier,
  response: ApprovalResponse,
});
export type ApprovalDeliveryInput = typeof ApprovalDeliveryInput.Type;
export const ApprovalDelivery = Schema.Struct({
  input: ApprovalDeliveryInput,
  receipt: CommandReceipt,
});

/** Requests use an existing domain demand; prompt and reply target are not caller-supplied. */
export const PersonalApprovalRequestProposal = Schema.Struct({
  contextPath: Schema.Union([RunPath, DelegationPath]),
  contextRevision: ContextRevision,
  approvalsRevision: ContextRevision,
  approvalId: CommandIdentifier,
});
export type PersonalApprovalRequestProposal = typeof PersonalApprovalRequestProposal.Type;
export const PersonalApprovalRequestInput = Schema.Struct({
  ...PersonalApprovalRequestProposal.fields,
  requestId: CommandIdentifier,
  causationId: CommandIdentifier,
  expectedRevision: ContextRevision,
});
export type PersonalApprovalRequestInput = typeof PersonalApprovalRequestInput.Type;
export const ApprovalRequestDeliveryInput = Schema.Struct({
  operation: Schema.Literal("requestApproval"),
  requestId: CommandIdentifier,
  causationId: CommandIdentifier,
  source: Schema.Literal("/personal"),
  causal: Schema.optional(CausalChain),
  target: Schema.Literal("/approvals"),
  expectedRevision: ContextRevision,
  createdAt: Schema.NonEmptyString,
  contextPath: Schema.Union([RunPath, DelegationPath]),
  contextRevision: ContextRevision,
  approvalId: CommandIdentifier,
});
export type ApprovalRequestDeliveryInput = typeof ApprovalRequestDeliveryInput.Type;
export const ApprovalRequestDelivery = Schema.Struct({
  input: ApprovalRequestDeliveryInput,
  receipt: CommandReceipt,
});
