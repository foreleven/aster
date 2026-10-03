import { Match, Schema, Struct } from "effect";
import type { ApprovalResponse, ExecutionSession, InputRequest } from "../tasks/model.js";
import { DelegationState } from "./state.js";

export type DelegationTransition =
  | { readonly type: "Attach"; readonly replyPath: string }
  | { readonly type: "Uncertain"; readonly text: string }
  | { readonly type: "Submitted"; readonly session: ExecutionSession }
  | { readonly type: "Completed"; readonly text: string }
  | {
      readonly type: "Failed";
      readonly status: "failed" | "cancelled" | "unknown";
      readonly text: string;
    }
  | { readonly type: "WaitingInput"; readonly id: string; readonly request: InputRequest }
  | { readonly type: "Running" }
  | {
      readonly type: "ApprovalReceived";
      readonly requestId: string;
      readonly response: ApprovalResponse;
    }
  | { readonly type: "ResponseSending" | "ResponseUncertain"; readonly requestId: string }
  | {
      readonly type: "ResponseDelivered";
      readonly requestId: string;
      readonly result:
        { readonly _tag: "Success" } | { readonly _tag: "Failure"; readonly error: Error };
    };

/** No arbitrary state patches: response delivery and execution changes each own their event payload. */
export const transitionDelegation = (
  current: DelegationState,
  transition: DelegationTransition,
) => {
  const responseStatus = (id: string, status: "sending" | "uncertain" | "sent") => ({
    responses: { ...current.responses, [id]: { ...current.responses[id], status } },
  });
  const change = Match.value(transition).pipe(
    Match.when({ type: "Attach" }, ({ replyPath }) => ({ patch: { replyPath } })),
    Match.when({ type: "Uncertain" }, ({ text }) => ({
      patch: { status: "uncertain", error: text },
      event: { type: "Error", text },
    })),
    Match.when({ type: "Submitted" }, ({ session }) => ({
      patch: { status: "running", session },
      event: { type: "Submitted", session },
    })),
    Match.when({ type: "Completed" }, ({ text }) => ({
      patch: { status: "completed", result: text },
      event: { type: "Completed", text },
    })),
    Match.when({ type: "Failed" }, ({ status, text }) => ({
      patch: { status, error: text },
      event: { type: "Error", text },
    })),
    Match.when({ type: "WaitingInput" }, ({ id, request }) => ({
      patch: { status: "waiting_input", requests: { ...current.requests, [id]: request } },
    })),
    Match.when({ type: "Running" }, () => ({ patch: { status: "running" } })),
    Match.when({ type: "ApprovalReceived" }, ({ requestId, response }) => ({
      patch: {
        responses: {
          ...current.responses,
          [requestId]: { request: current.requests[requestId], response, status: "received" },
        },
      },
      event: { type: "ApprovalReceived", requestId, response },
    })),
    Match.when({ type: "ResponseSending" }, ({ requestId }) => ({
      patch: responseStatus(requestId, "sending"),
    })),
    Match.when({ type: "ResponseUncertain" }, ({ requestId }) => ({
      patch: responseStatus(requestId, "uncertain"),
      event: { type: "ResponseUncertain", requestId },
    })),
    Match.when({ type: "ResponseDelivered" }, ({ requestId, result }) => ({
      patch: responseStatus(requestId, result._tag === "Success" ? "sent" : "uncertain"),
      event: {
        type: "ResponseDelivered",
        requestId,
        success: result._tag === "Success",
        ...(result._tag === "Failure" ? { error: result.error.message } : {}),
      },
    })),
    Match.exhaustive,
  );
  return {
    state: Schema.decodeUnknownSync(DelegationState)({
      ...(["Submitted", "Completed", "Running"].includes(transition.type)
        ? Struct.omit(current, ["error"])
        : current),
      ...change.patch,
    }),
    event: "event" in change ? change.event : undefined,
  };
};
