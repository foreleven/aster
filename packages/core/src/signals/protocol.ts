import { GetSignal } from "./queries.js";
import type { MailboxOf } from "@aster/actor";
import { Command as ActorCommand, ReplyTo } from "@aster/actor";
import { Context, Schema } from "effect";
import { SignalDefinition } from "../config/schema.js";
import { PublicContext } from "../context/contracts.js";
import { GoalPath } from "../goals/contracts.js";
import { ApplicationError, CommandReceipt } from "../operations.js";
import { QueryReply } from "../services/actors.js";
import { RemainingAgentTurns } from "../tasks/contracts.js";
import { SignalTime } from "./state/snapshot.js";

export const SignalReactionInput = Schema.Struct({
  requestId: Schema.NonEmptyString,
  causationId: Schema.NonEmptyString,
  source: Schema.Literal("/system-one"),
  target: Schema.String.check(Schema.isPattern(/^\/signals\/[a-z0-9][a-z0-9-]*$/)),
  version: Schema.Int.check(Schema.isGreaterThan(0)),
  sourceContext: PublicContext,
});
export type SignalReactionInput = typeof SignalReactionInput.Type;

const SignalPatch = Schema.Struct({
  trigger: SignalDefinition.fields.trigger,
  task: SignalDefinition.fields.task,
});
export const SignalChange = Schema.Union([
  Schema.Struct({ operation: Schema.Literal("create"), definition: SignalPatch }),
  Schema.Struct({
    operation: Schema.Literal("update"),
    version: Schema.Int,
    definition: SignalPatch,
  }),
  Schema.Struct({ operation: Schema.Literals(["pause", "resume", "delete"]), version: Schema.Int }),
]);
/** Tool-call identity and arguments are replayed by Pi; the Signal mailbox owns the receipt. */
export const SignalChangeInput = Schema.Struct({
  requestId: Schema.NonEmptyString,
  source: GoalPath,
  target: Schema.String.check(Schema.isPattern(/^\/signals\/[a-z0-9][a-z0-9-]*$/)),
  change: SignalChange,
  remainingAgentTurns: RemainingAgentTurns,
}).check(
  Schema.makeFilter(
    (input) =>
      input.target
        .slice("/signals/".length)
        .startsWith(`${input.source.slice("/goals/".length)}--`),
    { expected: "Signal belongs to the requesting Goal" },
  ),
);
export type SignalChangeInput = typeof SignalChangeInput.Type;

export class SignalDefinitions extends Context.Service<
  SignalDefinitions,
  readonly SignalDefinition[]
>()("signals/Definitions") {}
export const SignalCommandReply = Schema.TaggedUnion({
  Accepted: { receipt: CommandReceipt },
  Rejected: { error: Schema.instanceOf(ApplicationError) },
});
export type SignalCommandReply = typeof SignalCommandReply.Type;
export class React extends ActorCommand.Class<React>()("React", {
  payload: { input: SignalReactionInput },
  reply: SignalCommandReply,
}) {}
export class Change extends ActorCommand.Class<Change>()("Change", {
  payload: { input: SignalChangeInput },
  reply: SignalCommandReply,
}) {}
export class PauseByOwner extends ActorCommand.Class<PauseByOwner>()("PauseByOwner", {
  payload: { owner: GoalPath },
  reply: Schema.Void,
}) {}
export const SignalCommands = [React, Change, PauseByOwner, GetSignal] as const;
export const SignalInternal = Schema.TaggedUnion({
  Tick: { version: Schema.Int, due: SignalTime },
  Dispatch: {},
  Delivered: {
    id: Schema.String,
    result: Schema.TaggedUnion({
      Success: { value: Schema.optional(CommandReceipt) },
      Failure: { error: Schema.instanceOf(ApplicationError) },
    }),
  },
});
export type SignalCommand = MailboxOf<typeof SignalCommands, typeof SignalInternal>;
export class ListByOwner extends ActorCommand.Class<ListByOwner>()("ListByOwner", {
  payload: { owner: GoalPath },
  reply: QueryReply,
}) {}
export const SignalRootCommands = [ListByOwner, React, Change, PauseByOwner] as const;
export const SignalRootInternal = Schema.TaggedUnion({
  OwnerPaused: { replyTo: ReplyTo<void>() },
});
export type SignalRootCommand = MailboxOf<typeof SignalRootCommands, typeof SignalRootInternal>;
