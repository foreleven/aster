import { GoalTaskOutput } from "@aster/api-contracts";
import { Schema } from "effect";
import { GoalPlan } from "./plan.js";

const Identity = {
  evaluationId: Schema.NonEmptyString,
  inputIds: Schema.optional(Schema.Array(Schema.NonEmptyString)),
  retryOf: Schema.optional(Schema.NonEmptyString),
  reason: Schema.String,
  historyThrough: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  startedAt: Schema.NonEmptyString,
};

/** Business journal owned by the Goal mailbox; never reconstructed from transcript text. */
export const GoalEvaluationRecord = Schema.Union([
  Schema.Struct({ ...Identity, status: Schema.Literals(["pending", "running"]) }),
  Schema.Struct({
    ...Identity,
    status: Schema.Literals(["failed", "reconciliation_required"]),
    error: Schema.String,
    result: Schema.optional(GoalPlan),
    observedAt: Schema.NonEmptyString,
  }),
  Schema.Struct({
    ...Identity,
    status: Schema.Literals(["completed", "partially_applied"]),
    resultId: Schema.NonEmptyString,
    result: GoalPlan,
    appliedAt: Schema.NonEmptyString,
    taskOutputs: Schema.optional(Schema.Array(GoalTaskOutput)),
  }),
]);
export type GoalEvaluationRecord = typeof GoalEvaluationRecord.Type;

export const evaluationIdentity = (record: GoalEvaluationRecord) => ({
  evaluationId: record.evaluationId,
  ...(record.inputIds ? { inputIds: record.inputIds } : {}),
  ...(record.retryOf ? { retryOf: record.retryOf } : {}),
  reason: record.reason,
  historyThrough: record.historyThrough,
  startedAt: record.startedAt,
});

export const validEvaluationJournal = (records: readonly GoalEvaluationRecord[]) => {
  const seen = new Map<string, GoalEvaluationRecord>();
  for (const record of records) {
    if (seen.has(record.evaluationId)) return false;
    if (new Set(record.inputIds).size !== (record.inputIds?.length ?? 0)) return false;
    if (
      record.retryOf &&
      JSON.stringify(record.inputIds) !== JSON.stringify(seen.get(record.retryOf)?.inputIds)
    )
      return false;
    if (record.retryOf && seen.get(record.retryOf)?.status !== "failed") return false;
    if (
      (record.status === "completed" || record.status === "partially_applied") &&
      record.resultId !== record.evaluationId
    )
      return false;
    seen.set(record.evaluationId, record);
  }
  return true;
};
