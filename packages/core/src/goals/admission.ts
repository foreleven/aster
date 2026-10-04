import { Effect, Match } from "effect";
import { ApplicationError } from "@aster/api-contracts";
import type { goalWorkingState } from "./working-state.js";
import type { GoalHistory } from "./history.js";
import type { ContextRegistry } from "../context/registry.js";
import type { GoalRequestRecord } from "./protocol.js";
import { goalInputs } from "./inputs.js";
import { goalInbox } from "./inbox.js";
import { goalIntentInbox } from "./intent-inbox.js";

/** Variant-specific authority checks and input/receipt commits share the Goal mailbox. */
export const goalAdmission = (
  registry: ContextRegistry["Service"],
  working: ReturnType<typeof goalWorkingState>,
  history: GoalHistory,
) => {
  const inputs = goalInputs(working, history);
  const personal = goalInbox(registry, working, history);
  const intents = goalIntentInbox(registry, working, history);
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
      Match.tag("PersonalMessage", ({ delivery }) =>
        Effect.gen(function* () {
          if (delivery.requestId !== request.requestId)
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Delivery identity mismatch",
            });
          return (yield* personal.accept(delivery, admission)).receipt;
        }),
      ),
      Match.tag("GoalIntent", ({ delivery }) =>
        Effect.gen(function* () {
          if (delivery.requestId !== request.requestId)
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Intent identity mismatch",
            });
          return (yield* intents.accept(delivery, admission)).receipt;
        }),
      ),
      Match.tag("UserInput", ({ text }) =>
        Effect.gen(function* () {
          yield* inputs.accept({ _tag: "UserInput", text }, request.requestId, {
            ...patch,
            causal,
            pendingEvaluation: true,
          });
          return receipt;
        }),
      ),
      Match.tag("SignalOccurrence", (input) =>
        Effect.gen(function* () {
          const signal = registry.get(input.signalPath)?.state as { goal?: string } | undefined;
          if (signal?.goal !== state().slug || input.id !== request.requestId)
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Signal occurrence has no matching Goal owner or identity",
            });
          yield* inputs.accept(
            {
              _tag: "SignalOccurrence",
              occurrenceId: input.id,
              signalPath: input.signalPath,
              evidence: input.text,
            },
            input.id,
            {
              ...patch,
              causal: input.causal ?? causal,
              pendingEvaluation: true,
              receivedEvents: [...state().receivedEvents, input.id],
            },
          );
          return receipt;
        }),
      ),
      Match.tag("ExecutionFeedback", (input) =>
        Effect.gen(function* () {
          const run = registry.get(input.runPath)?.state as
            | {
                goalTask?: { goalPath: string; taskId: string };
                signalSlug?: string;
              }
            | undefined;
          const signal = (
            run?.signalSlug ? registry.get(`/signals/${run.signalSlug}`)?.state : undefined
          ) as { goal?: string } | undefined;
          if (!run || (run.goalTask?.goalPath !== current().path && signal?.goal !== state().slug))
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Execution feedback does not belong to this Goal",
            });
          if (input.taskId !== undefined && input.taskId !== run.goalTask?.taskId)
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Execution Task identity mismatch",
            });
          const status = input.status ?? (input.terminal ? "completed" : "running");
          yield* inputs.accept(
            {
              _tag: "ExecutionFeedback",
              runPath: input.runPath,
              taskId: input.taskId,
              evaluationId: input.evaluationId,
              status,
              terminal: input.terminal,
              text: input.text,
            },
            request.requestId,
            {
              ...patch,
              causal: input.causal ?? state().causal,
              pendingEvaluation: (input.terminal && active) || state().pendingEvaluation,
              tasks: state().tasks.map((task) =>
                task.id === input.taskId && task.execution?.runPath === input.runPath
                  ? {
                      ...task,
                      execution: { ...task.execution, status },
                      ...(input.terminal ? { result: input.text } : {}),
                    }
                  : task,
              ),
              receivedEvents: [...state().receivedEvents, request.requestId],
            },
          );
          return receipt;
        }),
      ),
      Match.exhaustive,
    );
  });
};
