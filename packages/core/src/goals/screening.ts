import { Context, Effect, Schema } from "effect";
import type { ContextRecord } from "../context/model.js";
import type { GoalDefinition } from "../config/schema.js";
import type { DecisionError, SystemOneClient } from "../decisions/system-one.js";
import { score } from "../decisions/system-one.js";

export const GoalScreeningSnapshot = Schema.Struct({
  chatSummary: Schema.String,
  goalTitle: Schema.String,
  goalDescription: Schema.String,
  goalSummary: Schema.String,
});
export type GoalScreeningSnapshot = typeof GoalScreeningSnapshot.Type;

export const GoalScreeningRecord = Schema.Struct({
  screeningRecordId: Schema.String,
  sourcePath: Schema.String,
  goalSlug: Schema.String,
  summaryRevision: Schema.String,
  summaryFingerprint: Schema.String,
  input: GoalScreeningSnapshot,
  score: Schema.Number,
  admitted: Schema.Boolean,
  threshold: Schema.Number,
  policyVersion: Schema.String,
  model: Schema.String,
  requestId: Schema.String,
  latencyMs: Schema.Number,
  rationale: Schema.String,
  error: Schema.optional(Schema.String),
  createdAt: Schema.String,
});
export type GoalScreeningRecord = typeof GoalScreeningRecord.Type;

export interface GoalScreeningStoreShape {
  readonly append: (record: GoalScreeningRecord) => Effect.Effect<void, GoalScreeningStoreError>;
}

export class GoalScreeningStoreError extends Schema.TaggedError<GoalScreeningStoreError>()(
  "GoalScreeningStoreError",
  { message: Schema.String, cause: Schema.optional(Schema.Unknown) },
) {}

export class GoalScreeningStore extends Context.Service<
  GoalScreeningStore,
  GoalScreeningStoreShape
>()("goals/ScreeningStore") {}

export const makeMemoryGoalScreeningStore = (): GoalScreeningStore["Service"] => {
  const records: GoalScreeningRecord[] = [];
  return {
    append: (record) =>
      Effect.sync(() => {
        records.push(structuredClone(record));
      }),
  };
};

export const chatSummaryText = (record: ContextRecord): string => {
  const state = record.state as { summary?: unknown };
  const summary = state.summary;
  if (typeof summary === "string") return summary;
  if (summary && typeof summary === "object" && "text" in summary) {
    const text = (summary as { text?: unknown }).text;
    if (typeof text === "string") return text;
  }
  return "";
};

export const goalSummaryText = (record: ContextRecord | undefined): string => {
  const summary = record?.state && (record.state as { summary?: unknown }).summary;
  return typeof summary === "string" ? summary : "";
};

export const goalTitleText = (goal: GoalDefinition, record: ContextRecord | undefined): string => {
  const title = record?.state && (record.state as { title?: unknown }).title;
  return typeof title === "string" && title.trim() ? title : goal.title?.trim() || goal.description;
};

export const relevanceQuestion = (snapshot: GoalScreeningSnapshot) =>
  score(
    `Score how strongly this Chat Summary contains evidence relevant to Goal "${snapshot.goalTitle}". Return 0 for unrelated information and 10 for direct, actionable evidence that may change Goal progress, blockers, Tasks, Signals, or conclusions. Use the supplied Goal Summary to judge current relevance, not urgency.\n\n${JSON.stringify(snapshot)}`,
    [
      "0: unrelated to this Goal",
      "1: almost certainly unrelated",
      "2: weak or incidental overlap",
      "3: possible context but no clear Goal impact",
      "4: some relevant detail with limited impact",
      "5: moderately relevant evidence",
      "6: clearly relevant to part of the Goal",
      "7: strong evidence that may change planning",
      "8: very strong evidence affecting progress or blockers",
      "9: direct evidence requiring Goal assessment",
      "10: direct, actionable evidence central to the Goal",
    ],
  );

export const normalizeScore = (value: unknown): number | undefined => {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0 || value > 10)
    return undefined;
  return value / 10;
};

export const screeningDecision = (options: {
  readonly client: SystemOneClient;
  readonly goal: GoalDefinition;
  readonly source: ContextRecord;
  readonly goalRecord?: ContextRecord;
  readonly screeningRecordId: string;
  readonly requestId: string;
  readonly summaryRevision: string;
  readonly summaryFingerprint: string;
  readonly threshold: number;
  readonly policyVersion: string;
  readonly model: string;
  readonly now: () => number;
  readonly store?: GoalScreeningStore["Service"];
}): Effect.Effect<GoalScreeningRecord, DecisionError | GoalScreeningStoreError> =>
  Effect.gen(function* () {
    const input: GoalScreeningSnapshot = {
      chatSummary: chatSummaryText(options.source),
      goalTitle: goalTitleText(options.goal, options.goalRecord),
      goalDescription: options.goal.description,
      goalSummary: goalSummaryText(options.goalRecord),
    };
    const started = options.now();
    const result = yield* options.client.systemOne({
      state: input,
      questions: { relevance: relevanceQuestion(input) },
    });
    const answer = result.answers.relevance;
    const normalized = normalizeScore(answer?.score);
    const scoreValue = normalized ?? 0;
    const legend = answer?.legend?.[String(Math.round(answer.score ?? 0))];
    const rubric =
      normalized === undefined
        ? "System One returned no valid relevance score; admission failed closed."
        : typeof legend === "string"
          ? legend
          : `System One scored ${(normalized * 10).toFixed(1)}/10.`;
    const record: GoalScreeningRecord = {
      screeningRecordId: options.screeningRecordId,
      sourcePath: options.source.path,
      goalSlug: options.goal.slug,
      summaryRevision: options.summaryRevision,
      summaryFingerprint: options.summaryFingerprint,
      input,
      score: scoreValue,
      admitted: normalized !== undefined && scoreValue >= options.threshold,
      threshold: options.threshold,
      policyVersion: options.policyVersion,
      model: options.model,
      requestId: options.requestId,
      latencyMs: Math.max(0, options.now() - started),
      rationale: rubric,
      ...(normalized === undefined ? { error: "invalid-score" } : {}),
      createdAt: new Date(options.now()).toISOString(),
    };
    if (options.store) yield* options.store.append(record);
    return record;
  });
