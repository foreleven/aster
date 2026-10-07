import {
  CommandIdentifier,
  ApplicationError,
  PublicContext,
  CommandReceipt,
} from "@aster/core/contracts";
import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/rpc";

export const RetryGoalTurnInput = Schema.Struct({
  slug: Schema.NonEmptyString,
  requestId: CommandIdentifier,
  turnId: CommandIdentifier,
});
export type RetryGoalTurnInput = typeof RetryGoalTurnInput.Type;

/** User-facing dialogue projected from Pi entries, never tool or evidence records. */
export const GoalConversationMessage = Schema.Struct({
  id: Schema.Int,
  role: Schema.Literals(["user", "assistant"]),
  text: Schema.String,
  at: Schema.String,
});
export type GoalConversationMessage = typeof GoalConversationMessage.Type;
export const GoalTimelinePage = Schema.Struct({
  messages: Schema.Array(GoalConversationMessage),
  total: Schema.Int,
  nextBefore: Schema.NullOr(Schema.Int),
});
export type GoalTimelinePage = typeof GoalTimelinePage.Type;

export const GoalRpcs = RpcGroup.make(
  Rpc.make("ListGoals", { success: Schema.Array(PublicContext), error: ApplicationError }),
  Rpc.make("GetGoalTimeline", {
    payload: {
      slug: Schema.String,
      before: Schema.optional(Schema.Int),
      limit: Schema.optional(Schema.Int),
    },
    success: GoalTimelinePage,
    error: ApplicationError,
  }),
  Rpc.make("SendGoalMessage", {
    payload: {
      slug: Schema.String,
      text: Schema.String,
      requestId: Schema.optional(Schema.NonEmptyString),
    },
    success: Schema.Void,
    error: ApplicationError,
  }),
  Rpc.make("RetryGoalTurn", {
    payload: RetryGoalTurnInput,
    success: CommandReceipt,
    error: ApplicationError,
  }),
  Rpc.make("EndGoal", {
    payload: { slug: Schema.String, requestId: Schema.optional(Schema.NonEmptyString) },
    success: Schema.Void,
    error: ApplicationError,
  }),
);
