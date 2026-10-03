import { Match, Schema, Struct } from "effect";
import type { ApprovalResponse, Task } from "./model.js";
import { RunState } from "./run-state.js";
import { outcomeStatus, type ExecutionOutcome } from "./outcome.js";

export type RunTransition =
  | { readonly type: "Cancelled" | "PreparationFailed" | "RecoveryFailed"; readonly text: string }
  | { readonly type: "TaskPrepared"; readonly task: Task }
  | { readonly type: "Delegating" | "ConfirmationRequested" | "Ready" | "NotExecutable" }
  | {
      readonly type: "ConfirmationResolved";
      readonly requestId: string;
      readonly response: ApprovalResponse;
    }
  | { readonly type: "Submitted"; readonly sessionId: string }
  | { readonly type: "Error" | "WaitingInput"; readonly text: string }
  | { readonly type: "Finished"; readonly outcome: ExecutionOutcome };

/** Derive state and durable event together. Decode the result to enforce phase prerequisites. */
export const transitionRun = (current: RunState, transition: RunTransition) => {
  const next = Match.value(transition).pipe(
    Match.when({ type: "Cancelled" }, ({ text }) => ({
      status: "cancelled" as const,
      outcomeText: text,
    })),
    Match.when({ type: "PreparationFailed" }, ({ text }) => ({
      status: "preparation-failed" as const,
      outcomeText: text,
    })),
    Match.when({ type: "RecoveryFailed" }, ({ text }) => ({
      status: "preparation-failed" as const,
      outcomeText: text,
    })),
    Match.when({ type: "TaskPrepared" }, ({ task }) => ({ status: "checking" as const, task })),
    Match.when({ type: "Delegating" }, () => ({ status: "submitting" as const })),
    Match.when({ type: "ConfirmationRequested" }, () => ({
      status: "awaiting-confirmation" as const,
    })),
    Match.when({ type: "Ready" }, () => ({ status: "ready" as const })),
    Match.when({ type: "NotExecutable" }, () => ({
      status: "blocked" as const,
      outcomeText: "The execution plan is not currently executable",
    })),
    Match.when({ type: "ConfirmationResolved" }, ({ requestId, response }) => ({
      status: response.decision === "approve" ? ("ready" as const) : ("rejected" as const),
      approvals: [...(current.approvals ?? []), requestId],
      ...(response.decision === "reject"
        ? { outcomeText: "The user rejected this execution" }
        : {}),
    })),
    Match.when({ type: "Submitted" }, ({ sessionId }) => ({
      status: "running" as const,
      sessionId,
    })),
    Match.when({ type: "Error" }, ({ text }) => ({
      status: "uncertain" as const,
      outcomeText: text,
    })),
    Match.when({ type: "WaitingInput" }, () => ({ status: "waiting_input" as const })),
    Match.when({ type: "Finished" }, ({ outcome }) => ({
      status: outcomeStatus(outcome),
      outcomeText: outcome.text,
    })),
    Match.exhaustive,
  );
  const event = Match.value(transition).pipe(
    Match.when({ type: "Finished" }, ({ outcome }) => ({ type: outcome._tag, text: outcome.text })),
    Match.when({ type: "NotExecutable" }, () => ({
      type: "NotExecutable",
      text: "System One determined that the current Task is not executable",
    })),
    Match.when({ type: "ConfirmationResolved" }, ({ response }) => ({
      type: "ConfirmationResolved",
      response,
    })),
    Match.orElse((event) => event),
  );
  return {
    state: Schema.decodeUnknownSync(RunState)({
      ...(transition.type === "Submitted" ? Struct.omit(current, ["outcomeText"]) : current),
      ...next,
    }),
    event,
  };
};
