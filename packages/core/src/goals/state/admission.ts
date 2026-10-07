import { goalInputs, goalInputId } from "./inputs.js";
import { goalRequestFingerprint, type GoalRequestData, type GoalReceipt } from "../protocol.js";
import type { ContextRegistry } from "../../context/registry.js";
import type { AgentConversations } from "@aster/agent";
import type { GoalStore } from "./store.js";
import { ApplicationError } from "../../operations.js";
import { RemainingAgentTurns, TaskPath } from "../../tasks/contracts.js";

import { Effect, Match, Schema } from "effect";
import { validateTaskMessage } from "../../tasks/state/admission.js";

/** Private admission rules run within the owning GoalState mutation. */
export const goalAdmission = (
  registry: ContextRegistry["Service"],
  working: GoalStore,
  history: AgentConversations["Service"],
) => {
  const inputs = goalInputs(working, history);
  const submit = Effect.fn("Goal.submitInput")(function* (
    request: Extract<GoalRequestData, { _tag: "SubmitInput" }>,
    record: GoalReceipt,
  ) {
    const { receipt } = record;
    const state = yield* working.read;
    const current = yield* working.current;
    const input = request.input;
    const active = state.status === "active";
    if (!active && input._tag !== "ExecutionFeedback")
      return yield* new ApplicationError({ kind: "conflict", message: "Goal has ended" });
    const patch = { receipts: [...state.receipts, record] };
    const remainingAgentTurns = 4;
    return yield* Match.value(input).pipe(
      Match.tag("GoalIntent", ({ delivery }) =>
        Effect.gen(function* () {
          if (delivery.requestId !== request.requestId)
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Intent identity mismatch",
            });
          const intent = delivery.intent;
          if (delivery.target !== current.path)
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
            state.inputs.some(
              (item) =>
                item.inputId === goalInputId(state.definition.slug, "GoalIntent", intent.intentId),
            )
          )
            return yield* new ApplicationError({
              kind: "conflict",
              message: "Goal intent ID belongs to another request",
            });
          yield* inputs.accept({ _tag: "GoalIntent", intent }, intent.intentId, 4, patch);
          return receipt;
        }),
      ),
      Match.tag("UserInput", ({ text }) =>
        Effect.gen(function* () {
          yield* inputs.accept(
            { _tag: "UserInput", text },
            request.requestId,
            remainingAgentTurns,
            patch,
          );
          return receipt;
        }),
      ),
      Match.tag("TaskMessage", ({ delivery }) =>
        Effect.gen(function* () {
          if (
            delivery.requestId !== request.requestId ||
            delivery.task._tag !== "Goal" ||
            delivery.task.target !== current.path ||
            !registry.get(delivery.source)
          )
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Task destination or source is invalid",
            });
          yield* validateTaskMessage(registry, delivery, history);
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
            delivery.remainingAgentTurns,
            patch,
          );
          return receipt;
        }),
      ),
      Match.tag("ExecutionFeedback", (input) =>
        Effect.gen(function* () {
          if (!Schema.is(TaskPath)(input.taskPath))
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Invalid feedback Task path",
            });
          const run = yield* Schema.decodeUnknownEffect(
            Schema.Struct({
              admission: Schema.Struct({
                replyTo: Schema.String,
                remainingAgentTurns: RemainingAgentTurns,
              }),
            }),
          )(registry.get(input.taskPath)?.state).pipe(
            Effect.mapError(
              () =>
                new ApplicationError({
                  kind: "invalid-input",
                  message: "Execution feedback Task is missing or invalid",
                }),
            ),
          );
          if (run.admission.replyTo !== current.path)
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Execution feedback does not belong to this Goal",
            });
          yield* inputs.accept(input, request.requestId, run.admission.remainingAgentTurns, {
            ...patch,
            tasks: state.tasks.includes(input.taskPath)
              ? state.tasks
              : [...state.tasks, input.taskPath],
          });
          return receipt;
        }),
      ),
      Match.exhaustive,
    );
  });

  const end = Effect.fn("Goal.end")(function* (record: GoalReceipt) {
    const state = yield* working.read;
    yield* working
      .save({
        status: "completed",
        receipts: [...state.receipts, record],
        inputs: state.inputs.map((input) =>
          Match.value(input).pipe(
            Match.when({ status: "running" }, (input) => ({
              ...input,
              status: "unknown" as const,
              error: "Goal ended during conversation delivery",
            })),
            Match.when({ status: "pending" }, (input) => ({
              ...input,
              status: "ignored" as const,
            })),
            Match.orElse((input) => input),
          ),
        ),
      })
      .pipe(Effect.orDie);
    return record.receipt;
  });

  const retry = Effect.fn("Goal.retryInput")(function* (
    request: Extract<GoalRequestData, { _tag: "RetryTurn" }>,
    record: GoalReceipt,
    busy: boolean,
  ) {
    const state = yield* working.read;
    const input = state.inputs.find((input) => input.inputId === request.turnId);
    const retryable =
      input?.status === "failed" && !state.inputs.some((item) => item.retryOf === input.inputId);
    if (state.status !== "active" || busy || !retryable)
      return yield* new ApplicationError({
        kind: "conflict",
        message: "Only a failed, unretried input on an idle active Goal can be retried",
      });
    const original = yield* inputs.resolve(input);
    yield* inputs.accept(
      original.payload,
      request.requestId,
      input.remainingAgentTurns,
      { receipts: [...state.receipts, record] },
      input.inputId,
    );
    return record.receipt;
  });

  // The caller acknowledges only after this operation commits. Replayed requests must not restart work.
  return Effect.fn("Goal.acceptRequest")(function* (request: GoalRequestData, busy: boolean) {
    const payloadFingerprint = goalRequestFingerprint(request);
    const state = yield* working.read;
    const previous = state.receipts.find((item) => item.requestId === request.requestId);
    if (previous) {
      if (previous.payloadFingerprint !== payloadFingerprint)
        return yield* new ApplicationError({
          kind: "conflict",
          message: "Goal request identity belongs to another payload",
        });
      return { receipt: previous.receipt, replayed: true };
    }
    const record: GoalReceipt = {
      requestId: request.requestId,
      payloadFingerprint,
      receipt: {
        requestId: request.requestId,
        revision: ((yield* working.current).revision ?? 0) + 1,
      },
    };
    const receipt = yield* Match.value(request).pipe(
      Match.tag("SubmitInput", (request) => submit(request, record)),
      Match.tag("End", () => end(record)),
      Match.tag("RetryTurn", (request) => retry(request, record, busy)),
      Match.exhaustive,
    );
    return { receipt, replayed: false };
  });
};
