import { CausalChain } from "./notification.js";
import { Schema } from "effect";
import { SignalAction } from "./writeback.js";
import { CommandReceipt } from "./command.js";

const Identifier = Schema.NonEmptyString;
const Revision = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
export const SignalSchedule = Schema.Union([
  Schema.Struct({ type: Schema.Literal("once"), at: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("cron"),
    expression: Schema.String,
    timeZone: Schema.String,
  }),
]);
export type SignalSchedule = typeof SignalSchedule.Type;

/** Personal may define work, but each resulting Run still requires confirmation. */
export const PersonalSignalDefinition = Schema.Struct({
  action: Schema.optional(SignalAction),
  when: Schema.String.check(Schema.isPattern(/\S/)),
  task: Schema.String.check(Schema.isPattern(/\S/)),
  agent: Schema.NonEmptyString,
  schedule: Schema.optional(SignalSchedule),
  notBefore: Schema.optional(Schema.String),
});
export const PersonalSignalProposal = Schema.Struct({
  operation: Schema.Literals(["createSignal", "updateSignal"]),
  signalSlug: Schema.String.check(Schema.isPattern(/^personal--[a-z0-9][a-z0-9-]*$/)),
  signalRevision: Revision,
  definition: PersonalSignalDefinition,
  active: Schema.Boolean,
});
export type PersonalSignalProposal = typeof PersonalSignalProposal.Type;
export const PersonalSignalCommandInput = Schema.Struct({
  ...PersonalSignalProposal.fields,
  requestId: Identifier,
  causationId: Identifier,
  expectedRevision: Revision,
});
export type PersonalSignalCommandInput = typeof PersonalSignalCommandInput.Type;

export const SignalDeliveryInput = Schema.Struct({
  operation: PersonalSignalProposal.fields.operation,
  requestId: Identifier,
  causationId: Identifier,
  source: Schema.Literal("/personal"),
  causal: Schema.optional(CausalChain),
  target: Schema.String.check(Schema.isPattern(/^\/signals\/personal--[a-z0-9][a-z0-9-]*$/)),
  expectedRevision: Revision,
  createdAt: Schema.NonEmptyString,
  definition: PersonalSignalDefinition,
  active: Schema.Boolean,
});
export type SignalDeliveryInput = typeof SignalDeliveryInput.Type;
export const SignalDeliveryReceipt = CommandReceipt;
export type SignalDeliveryReceipt = typeof SignalDeliveryReceipt.Type;
export const SignalDelivery = Schema.Struct({
  input: SignalDeliveryInput,
  receipt: SignalDeliveryReceipt,
});
