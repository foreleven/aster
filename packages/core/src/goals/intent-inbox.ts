import type { GoalRequestRecord } from "./protocol.js";
import { goalInputs, newGoalInput } from "./inputs.js";
import { ApplicationError, type CommandReceipt } from "@aster/api-contracts";
import { DateTime, Effect, Schema } from "effect";
import { isDeepStrictEqual } from "node:util";
import type { ContextRegistry } from "../context/registry.js";
import type { goalWorkingState } from "./working-state.js";
import type { GoalHistory } from "./history.js";
import { GoalIntentInput } from "./intent.js";

/** The mailbox owns acceptance; history can always be rebuilt from the accepted input. */
export const goalIntentInbox = (
  registry: ContextRegistry["Service"],
  working: ReturnType<typeof goalWorkingState>,
  history: GoalHistory,
) => {
  const project = goalInputs(working, history).project;
  const accept = Effect.fn("GoalIntentInbox.accept")(function* (
    raw: GoalIntentInput,
    admission?: GoalRequestRecord,
  ): Effect.fn.Return<
    { readonly receipt: CommandReceipt; readonly created: boolean },
    ApplicationError
  > {
    const input = yield* Schema.decodeUnknownEffect(GoalIntentInput)(raw).pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "invalid-input", message: "Invalid Goal intent" }),
      ),
    );
    const current = working.current();
    const state = working.state();
    if (input.target !== current.path || input.intent.goalSlug !== state.slug)
      return yield* new ApplicationError({
        kind: "invalid-input",
        message: "Goal intent target mismatch",
      });
    const previous = state.intents?.find(
      (item) =>
        item.input.requestId === input.requestId ||
        item.input.intent.intentId === input.intent.intentId,
    );
    if (previous) {
      if (!isDeepStrictEqual(previous.input, input))
        return yield* new ApplicationError({
          kind: "conflict",
          message: "Goal intent ID belongs to another input",
        });
      yield* project();
      return { receipt: previous.receipt, created: false };
    }
    if (state.status !== "active")
      return yield* new ApplicationError({ kind: "conflict", message: "Goal has ended" });
    if (
      !input.intent.content.summary.trim() ||
      !input.intent.relevance.rationale.trim() ||
      !Number.isFinite(Date.parse(input.intent.createdAt))
    )
      return yield* new ApplicationError({
        kind: "invalid-input",
        message: "Goal intent requires dated evidence and rationale",
      });
    if (input.intent.relevance.score < input.intent.relevance.threshold)
      return yield* new ApplicationError({
        kind: "invalid-input",
        message: "Goal intent does not meet its screening threshold",
      });
    if (state.status !== "active")
      return yield* new ApplicationError({ kind: "conflict", message: "Goal has ended" });
    const receipt = { requestId: input.requestId, revision: (current.revision ?? 0) + 1 };
    yield* registry
      .commit(
        {
          ...current,
          state: {
            ...state,
            requests: admission ? [...(state.requests ?? []), admission] : state.requests,
            causal: { rootRequestId: input.causationId, remainingAgentTurns: 4 },
            intents: [...(state.intents ?? []), { input, receipt }],
            inputs: [
              ...(state.inputs ?? []),
              {
                ...newGoalInput(
                  state,
                  { _tag: "GoalIntent", intent: input.intent },
                  input.intent.intentId,
                  DateTime.formatIso(yield* DateTime.now),
                ),
                causal: { rootRequestId: input.causationId, remainingAgentTurns: 4 },
              },
            ],
            pendingEvaluation: true,
          },
        },
        { expectedRevision: input.expectedRevision },
      )
      .pipe(
        Effect.catchTag("ContextConflict", () =>
          Effect.fail(
            new ApplicationError({
              kind: "conflict",
              message: "Goal changed after screening; evaluate its current revision",
            }),
          ),
        ),
        Effect.catchTag("ContextCommitError", Effect.die),
        Effect.catchTag("ContextValidationError", Effect.die),
      );
    yield* project();
    return { receipt, created: true };
  });
  return { accept, project };
};
