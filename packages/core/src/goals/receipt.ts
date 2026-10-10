import { Schema } from "effect";
import { CommandReceipt } from "../operations.js";

export const GoalReceipt = Schema.Struct({
  requestId: Schema.String,
  payloadFingerprint: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  receipt: CommandReceipt,
});
export type GoalReceipt = typeof GoalReceipt.Type;
