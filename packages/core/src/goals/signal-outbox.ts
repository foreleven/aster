import type { GoalRequestRecord } from "./protocol.js";
import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { Effect } from "effect";
import {
  ApplicationError,
  type RetryGoalSignalInput,
  type CommandReceipt,
} from "@aster/api-contracts";
import type { GoalActorContext } from "./task-execution.js";
import type { goalWorkingState } from "./working-state.js";
import type { GoalSignals } from "./signal-coordination.js";
import { signalAttemptLimit, type GoalSignalOperation } from "../signals/goal-command.js";

/** Only mailbox handlers mutate the journal. Replay always uses the original receiver identity. */
export const goalSignalOutbox = (
  working: ReturnType<typeof goalWorkingState>,
  signals: GoalSignals["Service"],
) => {
  const generation = randomUUID();
  const { state, save } = working;
  const dispatch = Effect.fn("Goal.dispatchSignals")(function* (context: GoalActorContext) {
    for (const operation of state().signalOutbox ?? []) {
      if (operation.status !== "pending" || operation.attempts >= signalAttemptLimit(operation))
        continue;
      const sending: GoalSignalOperation = {
        ...operation,
        status: "sending",
        attempts: operation.attempts + 1,
      };
      yield* save({
        signalOutbox: state().signalOutbox!.map((item) =>
          item.input.requestId === operation.input.requestId ? sending : item,
        ),
      });
      yield* context.pipeToSelf(
        signals.applySignal
          ? signals.applySignal(operation.input, context.self)
          : Effect.fail(
              new ApplicationError({
                kind: "unavailable",
                message: "Goal Signal delivery unavailable",
              }),
            ),
        (result) => ({
          _tag: "SignalDeliverySettled",
          requestId: operation.input.requestId,
          generation,
          result,
        }),
      );
    }
  });
  const recover = Effect.fn("Goal.recoverSignals")(function* (context: GoalActorContext) {
    const operations = state().signalOutbox ?? [];
    if (
      operations.some(
        (item) =>
          item.status === "sending" ||
          (item.status === "unknown" && item.attempts < signalAttemptLimit(item)),
      )
    )
      yield* save({
        signalOutbox: operations.map((item) => {
          if (item.status !== "sending" && item.status !== "unknown") return item;
          return {
            ...item,
            status:
              item.attempts < signalAttemptLimit(item)
                ? ("pending" as const)
                : ("unknown" as const),
          };
        }),
      });
    yield* dispatch(context);
  });
  const delivered = Effect.fn("Goal.signalDelivered")(function* (command: {
    readonly requestId: string;
    readonly generation: string;
    readonly result:
      | { readonly _tag: "Success"; readonly value: CommandReceipt }
      | { readonly _tag: "Failure"; readonly error: ApplicationError };
  }) {
    if (command.generation !== generation) return;
    const operations = state().signalOutbox ?? [];
    const original = operations.find((item) => item.input.requestId === command.requestId);
    if (!original || original.status !== "sending") return;
    const { error: _error, ...base } = original;
    const result = command.result;
    const update: GoalSignalOperation =
      result._tag === "Success"
        ? { ...base, status: "delivered", receipt: result.value }
        : {
            ...base,
            status: result.error.kind === "unavailable" ? "unknown" : "rejected",
            error: result.error.message,
          };
    const next = operations.map((item) =>
      item.input.requestId === command.requestId ? update : item,
    );
    yield* save({
      signalOutbox: next,
      evaluations: state().evaluations?.map((item) => {
        if (
          item.status !== "partially_applied" ||
          item.evaluationId !== original.input.evaluationId
        )
          return item;
        return next
          .filter((operation) => operation.input.evaluationId === item.evaluationId)
          .every((operation) => operation.status === "delivered")
          ? { ...item, status: "completed" as const }
          : item;
      }),
    });
  });
  const retry = Effect.fn("Goal.retrySignal")(function* (
    input: RetryGoalSignalInput,
    admission?: GoalRequestRecord,
  ) {
    if (input.slug !== state().slug)
      return yield* new ApplicationError({
        kind: "invalid-input",
        message: "Retry addressed to another Goal",
      });
    const operations = state().signalOutbox ?? [];
    const previous = operations
      .flatMap((operation) => operation.retries ?? [])
      .find((retry) => retry.input.requestId === input.requestId);
    if (previous) {
      if (!isDeepStrictEqual(previous.input, input))
        return yield* new ApplicationError({
          kind: "conflict",
          message: "Retry identity belongs to another request",
        });
      return previous.receipt;
    }
    const operation = operations.find((item) => item.input.requestId === input.operationId);
    if (!operation)
      return yield* new ApplicationError({
        kind: "not-found",
        message: "Signal operation not found",
      });
    if (operation.status !== "unknown" || operation.attempts !== input.expectedAttempts)
      return yield* new ApplicationError({
        kind: "conflict",
        message: "Signal delivery has changed; refresh before retrying",
      });
    const receipt = { requestId: input.requestId, revision: (working.current().revision ?? 0) + 1 };
    yield* save({
      requests: admission ? [...(state().requests ?? []), admission] : state().requests,
      signalOutbox: operations.map((item) =>
        item === operation
          ? {
              ...item,
              status: "pending",
              retries: [...(item.retries ?? []), { input, receipt }],
            }
          : item,
      ),
    }).pipe(Effect.orDie);
    return receipt;
  });
  return { dispatch, recover, delivered, retry };
};
