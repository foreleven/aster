import { Schema } from "effect";
import { ContextText } from "../context/contracts.js";
import { CommandIdentifier } from "../operations.js";
import { RemainingAgentTurns, TaskMessage } from "../tasks/contracts.js";
import { GoalPath } from "./contracts.js";

export const GoalMessageDelivery = Schema.TaggedStruct("GoalMessageDelivery", {
  requestId: CommandIdentifier,
  source: TaskMessage.fields.source,
  goalPath: GoalPath,
  text: Schema.NonEmptyString,
  createdAt: Schema.NonEmptyString,
  remainingAgentTurns: RemainingAgentTurns,
  evidence: Schema.optional(ContextText),
});
export type GoalMessageDelivery = typeof GoalMessageDelivery.Type;
export const SendGoalMessageInput = Schema.Struct({
  goalPath: GoalPath,
  text: Schema.NonEmptyString,
});
