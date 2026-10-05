import { createHash } from "node:crypto";
import { Effect } from "effect";
import type { ContextRecord } from "../context/model.js";
import type { CoreConfig } from "../config/schema.js";
import type { SystemOneClient } from "../decisions/system-one.js";
import {
  GoalScreeningStore,
  screeningDecision,
  contextSummaryText,
  type GoalScreeningRecord,
} from "./screening.js";

export type GoalRelevance = CoreConfig["goals"][number] & {
  readonly score: number;
  readonly rationale: string;
  readonly screening: GoalScreeningRecord;
};

export const relevantGoals = (
  client: SystemOneClient,
  record: ContextRecord,
  goals: CoreConfig["goals"],
  options: {
    readonly goalRecords?: Readonly<Record<string, ContextRecord>>;
    readonly screening?: GoalScreeningStore["Service"];
    readonly threshold?: number;
    readonly policyVersion?: string;
    readonly model?: string;
    readonly now?: () => number;
  } = {},
) =>
  Effect.gen(function* () {
    const summary = contextSummaryText(record);
    if (!goals.length || !summary.trim()) return [];
    const summaryFingerprint = createHash("sha256")
      .update(JSON.stringify({ path: record.path, summary }))
      .digest("hex");
    const summaryRevision = summaryFingerprint;
    const threshold = options.threshold ?? 0.7;
    const policyVersion = options.policyVersion ?? "goal-relevance-v3";
    const model = options.model ?? "system-one";
    const now = options.now ?? Date.now;
    const relevant: GoalRelevance[] = [];
    for (const goal of goals) {
      const requestId = createHash("sha256")
        .update(`${record.path}:${goal.slug}:${summaryRevision}`)
        .digest("hex");
      const screening = yield* screeningDecision({
        client,
        goal,
        source: record,
        goalRecord: options.goalRecords?.[`/goals/${goal.slug}`],
        screeningRecordId: requestId,
        requestId,
        summaryRevision,
        summaryFingerprint,
        threshold,
        policyVersion,
        model,
        now,
        store: options.screening,
      });
      yield* Effect.logInfo(
        JSON.stringify({
          event: "goal.screening.completed",
          screeningRecordId: screening.screeningRecordId,
          sourcePath: screening.sourcePath,
          goalSlug: screening.goalSlug,
          score: screening.score,
          admitted: screening.admitted,
          latencyMs: screening.latencyMs,
          error: screening.error,
        }),
      );
      if (screening.admitted)
        relevant.push({
          ...goal,
          score: screening.score,
          rationale: screening.rationale,
          screening,
        });
    }
    return relevant;
  });
