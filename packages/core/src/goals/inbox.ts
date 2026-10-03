import { goalInputs, newGoalInput } from "./inputs.js";
import {
  ApplicationError,
  GoalDeliveryInput,
  type GoalDeliveryReceipt,
} from "@aster/api-contracts";
import { DateTime, Effect, Schema } from "effect";
import { isDeepStrictEqual } from "node:util";
import type { ContextRegistry } from "../context/registry.js";
import type { goalWorkingState } from "./working-state.js";
import type { GoalHistory } from "./history.js";

/** Mailbox-only acceptance; history is an idempotent projection of the durable inbox. */
export const goalInbox = (
  registry: ContextRegistry["Service"],
  working: ReturnType<typeof goalWorkingState>,
  history: GoalHistory,
) => {
  const project = goalInputs(working, history).project;
  const accept = Effect.fn("GoalInbox.accept")(function* (
    raw: GoalDeliveryInput,
  ): Effect.fn.Return<
    { readonly receipt: GoalDeliveryReceipt; readonly created: boolean },
    ApplicationError
  > {
    const input = yield* Schema.decodeUnknownEffect(GoalDeliveryInput)(raw).pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "invalid-input", message: "Invalid Goal delivery" }),
      ),
    );
    const current = working.current();
    const state = working.state();
    if (
      input.target !== current.path ||
      !input.text.trim() ||
      !Number.isFinite(Date.parse(input.createdAt))
    )
      return yield* new ApplicationError({
        kind: "invalid-input",
        message: "Invalid Goal delivery target or content",
      });
    const previous = state.deliveries?.find(
      (delivery) => delivery.input.requestId === input.requestId,
    );
    if (previous) {
      if (!isDeepStrictEqual(previous.input, input))
        return yield* new ApplicationError({
          kind: "conflict",
          message: "Goal delivery ID belongs to another input",
        });
      yield* project().pipe(Effect.orDie);
      return { receipt: previous.receipt, created: false };
    }
    const receipt = { requestId: input.requestId, revision: (current.revision ?? 0) + 1 };
    yield* registry
      .commit(
        {
          ...current,
          state: {
            ...state,
            causal: input.causal,
            inputs: [
              ...(state.inputs ?? []),
              newGoalInput(
                state,
                {
                  _tag: "PersonalMessage",
                  source: "/personal",
                  requestId: input.requestId,
                  text: input.text,
                },
                input.requestId,
                DateTime.formatIso(yield* DateTime.now),
              ),
            ],
            deliveries: [...(state.deliveries ?? []), { input, receipt }],
            pendingEvaluation: state.status === "active" || state.pendingEvaluation,
          },
        },
        { expectedRevision: input.expectedRevision },
      )
      .pipe(
        Effect.catchTag("ContextConflict", () =>
          Effect.fail(
            new ApplicationError({
              kind: "conflict",
              message: "Goal revision changed; refresh the target before proposing a new delivery",
            }),
          ),
        ),
        Effect.catchTag("ContextCommitError", Effect.die),
        Effect.catchTag("ContextValidationError", Effect.die),
      );
    yield* project().pipe(Effect.orDie);
    return { receipt, created: true };
  });
  return { accept, project };
};
