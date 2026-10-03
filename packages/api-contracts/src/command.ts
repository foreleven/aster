import { Schema } from "effect";

export const CommandIdentifier = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(256),
);
export const ContextRevision = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
/** Receipt revision belongs to the owner acknowledging this command. */
export const CommandReceipt = Schema.Struct({
  requestId: CommandIdentifier,
  revision: ContextRevision,
});
export type CommandReceipt = typeof CommandReceipt.Type;
