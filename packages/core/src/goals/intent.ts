import { createHash } from "node:crypto";
import { Schema } from "effect";
import type { AgentMessage } from "@aster/agent";
import type { ContextRecord } from "../context/model.js";
import type { GoalRelevance } from "./relevance.js";
import { contextSummaryText } from "./screening.js";

import { GoalIntent } from "@aster/api-contracts";
export { GoalIntent } from "@aster/api-contracts";

/** Frozen System One delivery; acknowledgement refers to the receiver's durable inbox. */
export const GoalIntentInput = Schema.Struct({
  requestId: Schema.NonEmptyString,
  causationId: Schema.NonEmptyString,
  source: Schema.Literal("/system-one"),
  target: Schema.String.check(Schema.isPattern(/^\/goals\/[a-z0-9][a-z0-9-]*$/)),
  expectedRevision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  intent: GoalIntent,
});
export type GoalIntentInput = typeof GoalIntentInput.Type;

export const makeGoalIntent = (
  record: ContextRecord,
  relevance: GoalRelevance,
  createdAt: string,
): GoalIntent => {
  const summary = contextSummaryText(record);
  const summaryFingerprint = relevance.screening.summaryFingerprint;
  const summaryRevision = relevance.screening.summaryRevision;
  const intentId = createHash("sha256")
    .update(`${relevance.slug}:${record.path}:${summaryRevision}`)
    .digest("hex");
  const rawChat = record.state as { chat?: { name?: string } };
  return {
    intentId,
    goalSlug: relevance.slug,
    source: {
      contextPath: record.path,
      actorPath: record.path,
      name: rawChat.chat?.name?.trim() || record.description,
      kind: "context",
    },
    content: { summary, summaryRevision, summaryFingerprint },
    relevance: {
      score: relevance.score,
      rationale: relevance.rationale,
      screeningRecordId: relevance.screening.screeningRecordId,
      threshold: relevance.screening.threshold,
      policyVersion: relevance.screening.policyVersion,
    },
    createdAt,
  };
};

export const goalIntentMessage = (intent: GoalIntent): AgentMessage => ({
  role: "user",
  content: [
    {
      type: "text",
      text: `[Goal intent]\n${JSON.stringify(intent)}`,
    },
  ],
  timestamp: Date.parse(intent.createdAt),
});
