import { validateTaskMessage } from "../tasks/admission.js";
import { Effect, Match } from "effect";
import { ApplicationError } from "@aster/api-contracts";
import type { goalWorkingState } from "./working-state.js";
import type { GoalHistory } from "./history.js";
import type { ContextRegistry } from "../context/registry.js";
import type { GoalRequestRecord } from "./protocol.js";
import { goalInputs } from "./inputs.js";

/** Variant-specific authority checks and input/receipt commits share the Goal mailbox. */
export const goalAdmission = (
  registry: ContextRegistry["Service"],
  working: ReturnType<typeof goalWorkingState>,
  history: GoalHistory,
) => {
  const inputs = goalInputs(working, history);
  return Effect.fn("Goal.submitInput")(function* (admission: GoalRequestRecord) {
    const { request, receipt } = admission;
    if (request._tag !== "SubmitInput")
      return yield* new ApplicationError({
        kind: "invalid-input",
        message: "Expected a Goal input",
      });
    const { state, current } = working;
    const input = request.input;
    const active = state().status === "active";
    if (!active && input._tag !== "ExecutionFeedback")
      return yield* new ApplicationError({ kind: "conflict", message: "Goal has ended" });
    const patch = { requests: [...(state().requests ?? []), admission] };
    const causal = { rootRequestId: request.requestId, remainingAgentTurns: 4 };
    return yield* Match.value(input).pipe(
      Match.tag("GoalIntent", ({ delivery }) =>
        Effect.gen(function* () {
          if (delivery.requestId !== request.requestId)
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Intent identity mismatch",
            });
          const intent = delivery.intent;
          if (delivery.target !== current().path || intent.goalSlug !== state().slug)
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Goal intent target mismatch",
            });
          if (
            !intent.content.summary.trim() ||
            !intent.relevance.rationale.trim() ||
            !Number.isFinite(Date.parse(intent.createdAt))
          )
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Goal intent requires dated evidence and rationale",
            });
          if (intent.relevance.score < intent.relevance.threshold)
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Goal intent does not meet its screening threshold",
            });
          if (
            state().inputs?.some(
              (item) =>
                item.payload._tag === "GoalIntent" &&
                item.payload.intent.intentId === intent.intentId,
            )
          )
            return yield* new ApplicationError({
              kind: "conflict",
              message: "Goal intent ID belongs to another request",
            });
          yield* inputs.accept(
            { _tag: "GoalIntent", intent },
            intent.intentId,
            { ...patch, causal: { rootRequestId: delivery.causationId, remainingAgentTurns: 4 } },
            delivery.expectedRevision,
          );
          return receipt;
        }),
      ),
      Match.tag("UserInput", ({ text }) =>
        Effect.gen(function* () {
          yield* inputs.accept({ _tag: "UserInput", text }, request.requestId, {
            ...patch,
            causal,
          });
          return receipt;
        }),
      ),
      Match.tag("TaskMessage", ({ delivery }) =>
        Effect.gen(function* () {
          if (
            delivery.requestId !== request.requestId ||
            delivery.task._tag !== "Goal" ||
            delivery.task.target !== current().path ||
            !registry.get(delivery.source)
          )
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Task destination or source is invalid",
            });
          yield* validateTaskMessage(registry, delivery);
          yield* inputs.accept(
            {
              _tag: "TaskMessage",
              requestId: delivery.requestId,
              source: delivery.source,
              text:
                delivery.task.text +
                (delivery.evidence ? `\n\nEvidence: ${JSON.stringify(delivery.evidence)}` : ""),
            },
            delivery.requestId,
            { ...patch, causal: delivery.causal },
          );
          return receipt;
        }),
      ),
      Match.tag("ExecutionFeedback", (input) =>
        Effect.gen(function* () {
          const run = registry.get(input.runPath)?.state as
            | {
                admission?: { input: { replyTo: string } };
              }
            | undefined;
          if (!run || run.admission?.input.replyTo !== current().path)
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Execution feedback does not belong to this Goal",
            });
          const status = input.status ?? (input.terminal ? "completed" : "running");
          yield* inputs.accept(
            {
              _tag: "ExecutionFeedback",
              runPath: input.runPath,
              status,
              terminal: input.terminal,
              text: input.text,
            },
            request.requestId,
            {
              ...patch,
              causal: input.causal ?? state().causal,
            },
          );
          return receipt;
        }),
      ),
      Match.exhaustive,
    );
  });
};
