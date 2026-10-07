import { ReplyTo } from "@aster/actor";
import { ApplicationError, CommandReceipt } from "../operations.js";
import { RemainingAgentTurns } from "../tasks/contracts.js";
import { GoalPath } from "../goals/contracts.js";
import { PublicContext } from "../context/contracts.js";
import { Context, Schema } from "effect";
import { queryReplyTo } from "../services/actors.js";
import { SignalDefinition } from "../config/schema.js";
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
export const SignalCommandReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: Schema.instanceOf(ApplicationError) }),
]);
export type SignalCommandReply = typeof SignalCommandReply.Type;
const React = Schema.TaggedStruct("React", {
  input: SignalReactionInput,
  replyTo: ReplyTo<SignalCommandReply>(),
});
const Change = Schema.TaggedStruct("Change", {
  input: SignalChangeInput,
  replyTo: ReplyTo<SignalCommandReply>(),
});
const PauseByOwner = Schema.TaggedStruct("PauseByOwner", {
  owner: GoalPath,
  replyTo: ReplyTo<void>(),
});
export const SignalCommand = Schema.Union([
  React,
  Change,
  PauseByOwner,
  Schema.TaggedStruct("Tick", { version: Schema.Int, due: SignalTime }),
  Schema.TaggedStruct("Dispatch", {}),
  Schema.TaggedStruct("Delivered", {
    id: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.optional(CommandReceipt) }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(ApplicationError) }),
    ]),
  }),
]);
export type SignalCommand = typeof SignalCommand.Type;
export const SignalRootCommand = Schema.Union([
  Schema.TaggedStruct("ListByOwner", { owner: GoalPath, replyTo: queryReplyTo }),
  React,
  Change,
  PauseByOwner,
  Schema.TaggedStruct("OwnerPaused", { replyTo: ReplyTo<void>() }),
]);
export type SignalRootCommand = typeof SignalRootCommand.Type;
