import { Schema } from "effect";
import type { PublicContext, CausalChain } from "@aster/api-contracts";
import { SignalDefinition } from "../config/schema.js";
export const scheduledCausalChain = (
  state: SignalDefinition & { causal?: CausalChain },
  requestId: string,
): CausalChain =>
  state.trigger._tag === "Schedule" && state.trigger.schedule.type === "cron"
    ? { rootRequestId: requestId, remainingAgentTurns: 4 }
    : (state.causal ?? { rootRequestId: requestId, remainingAgentTurns: 4 });
export const signalEnabled = (
  state: { active?: boolean; deleted?: boolean; goal?: string },
  getGoal: (slug: string) => PublicContext | undefined,
) =>
  state.active !== false &&
  !state.deleted &&
  (!state.goal ||
    (getGoal(state.goal)?.state as { status?: string } | undefined)?.status !== "completed");
export const sourceSignalEligible = (
  state: SignalDefinition & { active?: boolean; deleted?: boolean; goal?: string },
  getGoal: (slug: string) => PublicContext | undefined,
) => state.trigger._tag === "Context" && signalEnabled(state, getGoal);
export const sourceSignals = (snapshot: Readonly<Record<string, PublicContext>>) =>
  Object.values(snapshot)
    .filter(
      (record) =>
        /^\/signals\/[^/]+$/.test(record.path) && record.projection?.visibility !== "restricted",
    )
    .map((record) =>
      Schema.decodeUnknownSync(
        Schema.Struct({
          ...SignalDefinition.fields,
          active: Schema.optional(Schema.Boolean),
          deleted: Schema.optional(Schema.Boolean),
          goal: Schema.optional(Schema.String),
        }),
      )(record.state),
    )
    .filter((state) => sourceSignalEligible(state, (slug) => snapshot[`/goals/${slug}`]));
