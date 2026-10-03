import type { BusinessNotification } from "@aster/api-contracts";
import { Match } from "effect";
import type { GoalState } from "../goals/state.js";
import { businessNotification } from "./event.js";

/** Transcript, compaction and task bookkeeping are not Goal progress notifications. */
export const goalNotifications = (options: {
  path: string;
  revision: number;
  at: string;
  previous: GoalState;
  next: GoalState;
}): readonly BusinessNotification[] => {
  const { previous, next } = options;
  const existing = previous.businessOutbox ?? [];
  const outcome = Match.value(next).pipe(
    Match.when(
      (s) => !!s.lastError && s.lastError !== previous.lastError,
      (s) => ({ kind: "NeedsAttention" as const, text: s.lastError! }),
    ),
    Match.when(
      (s) => s.status !== previous.status && s.status === "completed",
      (s) => ({ kind: "GoalProgress" as const, text: `Goal completed. ${s.progress}`.trim() }),
    ),
    Match.when(
      (s) => !!s.progress.trim() && s.progress !== previous.progress,
      (s) => ({ kind: "GoalProgress" as const, text: s.progress }),
    ),
    Match.orElse(() => undefined),
  );
  if (!outcome) return existing;
  const parent = previous.pendingHandoff;
  const causal = parent?.causal ?? next.causal;
  return [
    ...existing,
    businessNotification({
      source: options.path,
      revision: options.revision,
      at: options.at,
      ...outcome,
      causationId: parent?.requestId ?? previous.pendingRequestId ?? causal?.rootRequestId,
      // A Goal evaluation consumes a turn before its conclusion can wake Personal.
      causal: causal && {
        ...causal,
        remainingAgentTurns: Math.max(0, causal.remainingAgentTurns - 1),
      },
    }),
  ];
};
