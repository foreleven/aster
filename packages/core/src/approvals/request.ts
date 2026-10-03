import {
  ApplicationError,
  RunPath,
  writebackApprovalId,
  writebackPrompt,
  type ApprovalEntry,
  type ApprovalRequestDeliveryInput,
} from "@aster/api-contracts";
import { Effect, Schema } from "effect";
import type { ContextRegistry } from "../context/registry.js";
import { RunState } from "../tasks/run-state.js";
import { DelegationState } from "../delegation/state.js";
import { GoalState } from "../goals/state.js";
import { taskPrompt } from "../tasks/model.js";
import { runActorPath } from "../tasks/address.js";

/** ApprovalQueue writes only its own state. Execution owners revalidate responses in their mailbox. */
export const requestedApproval = Effect.fn("ApprovalQueue.requestedApproval")(function* (
  registry: ContextRegistry["Service"],
  input: ApprovalRequestDeliveryInput,
): Effect.fn.Return<ApprovalEntry, ApplicationError> {
  const source = registry.get(input.contextPath);
  if (!source)
    return yield* new ApplicationError({
      kind: "not-found",
      message: "Approval source Context not found",
    });
  if ((source.revision ?? 0) !== input.contextRevision)
    return yield* new ApplicationError({
      kind: "conflict",
      message: "Approval source revision changed",
    });
  const unavailable = () =>
    new ApplicationError({
      kind: "conflict",
      message: "No matching pending approval demand in this Context",
    });
  if (Schema.is(RunPath)(source.path)) {
    const run = yield* Schema.decodeUnknownEffect(RunState)(source.state).pipe(
      Effect.mapError(unavailable),
    );
    if (
      run.writeback?.status === "waiting-approval" &&
      input.approvalId === writebackApprovalId(run.writeback.request)
    ) {
      return {
        id: input.approvalId,
        contextPath: source.path,
        target: runActorPath(source.path),
        kind: "approval",
        status: "pending",
        request: {
          id: input.approvalId,
          kind: "approval",
          prompt: writebackPrompt(run.writeback.request),
        },
      };
    }
    if (
      run.status !== "awaiting-confirmation" ||
      !run.task ||
      input.approvalId !== `${source.path}:confirm`
    )
      return yield* unavailable();
    if (run.goalTask) {
      const owner = registry.get(run.goalTask.goalPath);
      const goal = yield* Schema.decodeUnknownEffect(GoalState)(owner?.state).pipe(
        Effect.mapError(unavailable),
      );
      const task = goal.tasks.find((item) => item.id === run.goalTask!.taskId);
      if (
        goal.status !== "active" ||
        task?.status !== "open" ||
        task.revision !== run.goalTask.revision
      )
        return yield* unavailable();
    }
    return {
      id: input.approvalId,
      contextPath: source.path,
      target: runActorPath(source.path),
      kind: "confirmation",
      status: "pending",
      request: { id: input.approvalId, kind: "approval", prompt: taskPrompt(run.task) },
    };
  }
  const delegation = yield* Schema.decodeUnknownEffect(DelegationState)(source.state).pipe(
    Effect.mapError(unavailable),
  );
  const request = delegation.requests[input.approvalId];
  if (
    delegation.status !== "waiting_input" ||
    !delegation.session ||
    !request ||
    delegation.responses[input.approvalId]
  )
    return yield* unavailable();
  const canonical = `${source.path}:${delegation.session.runId ?? delegation.session.sessionId}:${request.id}`;
  if (input.approvalId !== canonical) return yield* unavailable();
  const ownerPath = yield* Schema.decodeUnknownEffect(RunPath)(delegation.request.runPath).pipe(
    Effect.mapError(unavailable),
  );
  return {
    id: input.approvalId,
    contextPath: source.path,
    target: `${runActorPath(ownerPath)}/delegation`,
    kind: request.kind,
    request,
    status: "pending",
  };
});
