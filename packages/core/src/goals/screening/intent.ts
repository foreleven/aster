import { GoalIntent } from "../contracts.js";
import { type PublicContext as ContextRecord } from "../../context/contracts.js";
import { contextSummaryText, type GoalRelevance } from "./decision.js";
import { Schema } from "effect";
import { createHash } from "node:crypto";

export { GoalIntent } from "../contracts.js";

/** Frozen System One delivery; acknowledgement refers to the receiver's durable inbox. */
export const GoalIntentInput = Schema.Struct({
  requestId: Schema.NonEmptyString,
  causationId: Schema.NonEmptyString,
  source: Schema.Literal("/system-one"),
  target: Schema.String.check(Schema.isPattern(/^\/goals\/[a-z0-9][a-z0-9-]*$/)),
  intent: GoalIntent,
});
export type GoalIntentInput = typeof GoalIntentInput.Type;

export const makeGoalIntent = (
  record: ContextRecord,
  relevance: GoalRelevance,
  createdAt: string,
): GoalIntent => {
  const summary = contextSummaryText(record);
  const fingerprint = relevance.screening.summaryFingerprint;
  const intentId = createHash("sha256")
    .update(`${relevance.slug}:${record.path}:${fingerprint}`)
    .digest("hex");
  return {
    intentId,
    source: {
      contextPath: record.path,
      name: record.description,
    },
    content: { summary },
    relevance: {
      score: relevance.score,
      rationale: relevance.rationale,
      threshold: relevance.screening.threshold,
    },
    createdAt,
  };
};
