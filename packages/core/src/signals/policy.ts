import type { ContextRecord } from "../context/model.js";
import type { SignalDefinition } from "../config/schema.js";

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
    .filter((record) => /^\/signals\/[^/]+$/.test(record.path))
    .map((record) => record.state as SignalDefinition)
    .filter((state) => sourceSignalEligible(state, now, (slug) => snapshot[`/goals/${slug}`]));
