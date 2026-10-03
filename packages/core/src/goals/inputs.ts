import { isDeepStrictEqual } from "node:util";
import { createHash } from "node:crypto";
import { DateTime, Effect, Match, Schema } from "effect";
import { ApplicationError, GoalInput, type GoalInputPayload } from "@aster/api-contracts";
import type { AgentMessage } from "@aster/agent";
import type { goalWorkingState } from "./working-state.js";
import type { GoalHistory } from "./history.js";
import { goalIntentMessage } from "./intent.js";
import type { GoalState } from "./state.js";

export const StoredGoalInput = Schema.Struct({
  ...GoalInput.fields,
  historySequence: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
});
export type StoredGoalInput = typeof StoredGoalInput.Type;
export const goalInputId = (goal: string, kind: string, key: string) =>
  createHash("sha256")
    .update(JSON.stringify([goal, kind, key]))
    .digest("hex");
export const newGoalInput = (
  state: GoalState,
  payload: GoalInputPayload,
  key: string,
  at: string,
): StoredGoalInput => ({
  inputId: goalInputId(state.slug, payload._tag, key),
  goalSlug: state.slug,
  ordinal: (state.inputs?.at(-1)?.ordinal ?? 0) + 1,
  receivedAt: at,
  payload,
});
export const inputMessage = (input: StoredGoalInput): AgentMessage =>
  Match.value(input.payload).pipe(
    Match.tag("GoalIntent", ({ intent }) => goalIntentMessage(intent)),
    Match.tag("UserInput", ({ text }) => ({
      role: "user" as const,
      content: text,
      timestamp: Date.parse(input.receivedAt),
    })),
    Match.orElse((payload) => ({
      role: "user" as const,
      content: `[Goal input; evidence, not authorization]\n${JSON.stringify({ inputId: input.inputId, ...payload })}`,
      timestamp: Date.parse(input.receivedAt),
    })),
  );

/** Accepted business inputs are authoritative. History is an idempotent model-view projection. */
export const goalInputs = (working: ReturnType<typeof goalWorkingState>, history: GoalHistory) => {
  const project = Effect.fn("GoalInputs.project")(function* () {
    for (const input of working.state().inputs ?? []) {
      if (input.historySequence !== undefined) continue;
      const entry = yield* history
        .append(working.state().slug, inputMessage(input), input.inputId)
        .pipe(Effect.orDie);
      const state = working.state();
      const payload = input.payload;
      yield* working
        .save({
          inputs: state.inputs!.map((item) =>
            item.inputId === input.inputId ? { ...item, historySequence: entry.seq } : item,
          ),
          ...(payload._tag === "GoalIntent"
            ? {
                intents: state.intents?.map((item) =>
                  item.input.intent.intentId === payload.intent.intentId
                    ? { ...item, historySequence: entry.seq }
                    : item,
                ),
              }
            : {}),
          ...(payload._tag === "PersonalMessage"
            ? {
                deliveries: state.deliveries?.map((item) =>
                  item.input.requestId === payload.requestId
                    ? { ...item, historySequence: entry.seq }
                    : item,
                ),
              }
            : {}),
        })
        .pipe(Effect.orDie);
    }
  });
  const accept = Effect.fn("GoalInputs.accept")(function* (
    payload: GoalInputPayload,
    key: string,
    patch: Partial<GoalState> = {},
  ) {
    const state = working.state();
    const input = newGoalInput(state, payload, key, DateTime.formatIso(yield* DateTime.now));
    const prior = state.inputs?.find((item) => item.inputId === input.inputId);
    if (prior && !isDeepStrictEqual(prior.payload, payload))
      return yield* new ApplicationError({
        kind: "conflict",
        message: "Goal input ID belongs to another payload",
      });
    if (!prior)
      yield* working
        .save({ ...patch, inputs: [...(state.inputs ?? []), input] })
        .pipe(Effect.orDie);
    yield* project();
    return !prior;
  });
  return { accept, project };
};
