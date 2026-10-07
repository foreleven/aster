import { Schema } from "effect";
import { CommandIdentifier } from "@aster/core/contracts";
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
