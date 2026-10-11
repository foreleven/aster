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

export class ApplicationError extends Schema.TaggedError<ApplicationError>()("ApplicationError", {
  kind: Schema.Literals(["not-found", "invalid-input", "unavailable", "conflict", "uncertain"]),
  message: Schema.String,
  operationId: Schema.optional(CommandIdentifier),
}) {}

/** Dispatch occurred, but no authoritative acknowledgement established its outcome. */
export const operationUncertain = (options: {
  readonly requestId: string;
  readonly message: string;
}) =>
  new ApplicationError({
    kind: "uncertain",
    message: options.message,
    operationId: options.requestId,
  });
