import type { ContextRecord } from "../context/model.js";
import type { SignalDefinition } from "../config/schema.js";
import type { CausalChain } from "@aster/api-contracts";
import { Match } from "effect";

/** An accepted recurring definition authorizes one bounded reaction per due
 * occurrence. One-shot follow-ups keep their parent budget, so an Agent cannot
 * replenish an exhausted chain by continually scheduling another one-shot. */
export const scheduledCausalChain = (
  state: Pick<SignalDefinition, "schedule"> & { readonly causal?: CausalChain },
  occurrenceId: string,
): CausalChain | undefined =>
  Match.value(state.schedule?.type).pipe(
    Match.when("cron", () => ({ rootRequestId: occurrenceId, remainingAgentTurns: 4 })),
    Match.orElse(() => state.causal),
  );

export const signalEnabled = (
  state: { active?: boolean; deleted?: boolean; goal?: string },
  getGoal: (slug: string) => ContextRecord | undefined,
) =>
  state.active !== false &&
  !state.deleted &&
  (!state.goal ||
    (getGoal(state.goal)?.state as { status?: string } | undefined)?.status !== "completed");

export const sourceSignalEligible = (
  state: SignalDefinition & { active?: boolean; deleted?: boolean; goal?: string },
  now: number,
  getGoal: (slug: string) => ContextRecord | undefined,
) =>
  signalEnabled(state, getGoal) &&
  !state.schedule &&
  (!state.notBefore || now >= Date.parse(state.notBefore));

export const sourceSignals = (snapshot: Readonly<Record<string, ContextRecord>>, now: number) =>
  Object.values(snapshot)
    .filter(
      (record) =>
        /^\/signals\/[^/]+$/.test(record.path) && record.projection?.visibility !== "restricted",
    )
    .map((record) => record.state as SignalDefinition)
    .filter((state) => sourceSignalEligible(state, now, (slug) => snapshot[`/goals/${slug}`]));
