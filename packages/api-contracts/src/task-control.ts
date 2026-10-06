import { Schema } from "effect";
import { CommandIdentifier, ContextRevision } from "./command.js";
export const TaskPath = Schema.String.check(Schema.isPattern(/^\/tasks\/[a-f0-9]{64}$/));
/** Explicit operator authorization, addressed directly to the execution owner. */
export const TaskRecoveryInput = Schema.Struct({
  requestId: CommandIdentifier,
  target: TaskPath,
  expectedRevision: ContextRevision,
});
export type TaskRecoveryInput = typeof TaskRecoveryInput.Type;
export const FollowupTaskInput = Schema.Struct({
  requestId: CommandIdentifier,
  target: TaskPath,
  source: Schema.String.check(Schema.isPattern(/^\/goals\/[a-z0-9][a-z0-9-]*$/)),
  text: Schema.NonEmptyString,
});
export type FollowupTaskInput = typeof FollowupTaskInput.Type;
