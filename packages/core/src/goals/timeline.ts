import { ApplicationError, GoalTimelinePage, type GoalTimelineGroup } from "@aster/api-contracts";
import { Effect, Schema } from "effect";
import type { ContextRegistry } from "../context/registry.js";
import { GoalState } from "./state.js";

/** Read only business records. Never infer grouping by parsing Agent/history text. */
export const goalTimeline = Effect.fn("Goal.timeline")(function* (
  registry: ContextRegistry["Service"],
  slug: string,
  page: { before?: number; limit?: number } = {},
) {
  const record = registry.get(`/goals/${slug}`);
  if (!record) return yield* new ApplicationError({ kind: "not-found", message: "Goal not found" });
  const state = yield* Schema.decodeUnknownEffect(GoalState)(record.state).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "unavailable", message: "Goal timeline unavailable" }),
    ),
  );
  const evaluations = state.evaluations ?? [];
  const before = page.before ?? evaluations.length + 1;
  const limit = page.limit ?? 30;
  if (
    !Number.isInteger(before) ||
    before < 1 ||
    !Number.isInteger(limit) ||
    limit < 1 ||
    limit > 100
  )
    return yield* new ApplicationError({ kind: "invalid-input", message: "Invalid timeline page" });
  const end = Math.min(before - 1, evaluations.length);
  const start = Math.max(0, end - limit);
  const groups: GoalTimelineGroup[] = evaluations.slice(start, end).map((evaluation, index) => {
    const applied = evaluation.status === "completed" || evaluation.status === "partially_applied";
    const result = "result" in evaluation ? evaluation.result : undefined;
    const taskOutputs = applied
      ? (evaluation.taskOutputs ?? [])
      : (result?.taskChanges ?? []).map((change, index) => ({
          id: `${evaluation.evaluationId}:task:${index}`,
          taskId: change.id,
          operation: change.operation,
          title: "title" in change ? (change.title ?? change.id) : change.id,
        }));
    const signals =
      state.signalOutbox?.filter((item) => item.input.evaluationId === evaluation.evaluationId) ??
      [];
    const finishedAt = () => {
      if ("appliedAt" in evaluation) return evaluation.appliedAt;
      if ("observedAt" in evaluation) return evaluation.observedAt;
      return undefined;
    };
    return {
      evaluationId: evaluation.evaluationId,
      ordinal: start + index + 1,
      status: evaluation.status,
      retryOf: evaluation.retryOf,
      startedAt: evaluation.startedAt,
      finishedAt: finishedAt(),
      inputs: (evaluation.inputIds ?? []).flatMap(
        (id) => state.inputs?.find((input) => input.inputId === id) ?? [],
      ),
      ...(result
        ? {
            disposition: result.disposition ?? "advance",
            conclusion: { text: result.progress, evidence: result.evidence, applied },
          }
        : {}),
      outputs: [
        ...taskOutputs.map((output) => ({
          ...output,
          kind: "task" as const,
          target: output.taskId,
          status: applied ? "applied" : "rejected",
        })),
        ...signals.map((item) => ({
          id: item.input.requestId,
          kind: "signal" as const,
          target: item.input.target,
          operation: item.input.operation,
          title: item.input.definition.when,
          status: item.status,
          attempts: item.attempts,
          error: item.error,
        })),
        ...(!applied
          ? (result?.signalChanges?.map((change, index) => ({
              id: `${evaluation.evaluationId}:signal:${index}`,
              kind: "signal" as const,
              target: `/signals/${change.id.startsWith(`${slug}--`) ? change.id : `${slug}--${change.id}`}`,
              operation: change.operation,
              title: change.id,
              status: "rejected",
            })) ?? [])
          : []),
      ],
      error: "error" in evaluation ? evaluation.error : undefined,
      agentRun: { sessionId: slug, requestId: evaluation.evaluationId },
    };
  });
  const assigned = new Set(evaluations.flatMap((evaluation) => evaluation.inputIds ?? []));
  return yield* Schema.decodeUnknownEffect(GoalTimelinePage)({
    groups,
    pendingInputs: state.inputs?.filter((input) => !assigned.has(input.inputId)) ?? [],
    total: evaluations.length,
    nextBefore: start > 0 ? start + 1 : null,
  }).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "unavailable", message: "Invalid timeline projection" }),
    ),
  );
});
