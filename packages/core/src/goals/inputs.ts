import { isDeepStrictEqual } from "node:util";
import { createHash } from "node:crypto";
import { DateTime, Effect, Match, Schema } from "effect";
import {
  ApplicationError,
  CausalChain,
  GoalInput,
  type GoalInputPayload,
} from "@aster/api-contracts";
import type { AgentMessage } from "@aster/agent";
import type { goalWorkingState } from "./working-state.js";
import type { GoalHistory } from "./history.js";
import { goalIntentMessage } from "./intent.js";
import type { GoalState } from "./state.js";

export const StoredGoalInput = Schema.Struct({
  ...GoalInput.fields,
  status: Schema.Literals(["pending", "running", "completed", "failed", "unknown", "ignored"]),
  relevant: Schema.optional(Schema.Boolean),
  response: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
  retryOf: Schema.optional(Schema.String),
  causal: Schema.optional(CausalChain),
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
  status: "pending",
  goalSlug: state.slug,
  ordinal: (state.inputs?.at(-1)?.ordinal ?? 0) + 1,
  receivedAt: at,
  payload,
});
export const inputMessage = (input: StoredGoalInput): AgentMessage =>
  Match.value(input.payload).pipe(
    Match.tag("GoalIntent", ({ intent }) => goalIntentMessage(intent)),
    Match.tag("GoalStarted", () => ({
      role: "user" as const,
      content:
        "Begin pursuing the configured Goal now. Use relevant read-only tools, gather evidence and provide useful findings before asking about optional preferences.",
      timestamp: Date.parse(input.receivedAt),
    })),
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
      yield* working
        .save({
          inputs: state.inputs!.map((item) =>
            item.inputId === input.inputId ? { ...item, historySequence: entry.seq } : item,
          ),
        })
        .pipe(Effect.orDie);
    }
  });
  const accept = Effect.fn("GoalInputs.accept")(function* (
    payload: GoalInputPayload,
    key: string,
    patch: Partial<GoalState> = {},
    expectedRevision?: number,
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
        .save(
          {
            ...patch,
            inputs: [
              ...(state.inputs ?? []),
              {
                ...input,
                causal: patch.causal,
                ...(state.status === "active"
                  ? {}
                  : { status: "ignored" as const, response: "Feedback recorded after Goal ended" }),
              },
            ],
          },
          expectedRevision,
        )
        .pipe(
          Effect.catchTag("ContextConflict", (error) =>
            expectedRevision === undefined
              ? Effect.die(error)
              : Effect.fail(
                  new ApplicationError({
                    kind: "conflict",
                    message: "Goal revision changed; refresh the target before submitting",
                  }),
                ),
          ),
          Effect.catchTags({ ContextCommitError: Effect.die, ContextValidationError: Effect.die }),
        );
    yield* project();
    return !prior;
  });
  return { accept, project };
};
