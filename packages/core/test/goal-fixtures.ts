import type { ReplyTo } from "@aster/actor";
import { Effect } from "effect";
import type { GoalCommandReply } from "../src/goals/protocol.js";

/** Tests that observe committed state can discard an explicit admission acknowledgement. */
export const goalTestReply: ReplyTo<GoalCommandReply> = {
  path: "/test/goal-reply",
  incarnation: "test",
  tell: () => Effect.void,
  ask: () => Effect.die("The reply sink cannot receive requests"),
};

/** Inspect receipts and their input projection without a producer-specific persisted inbox. */
export const goalIntentRecords = (state: import("../src/goals/state.js").GoalState) =>
  (state.requests ?? []).flatMap(({ request, receipt }) => {
    if (request._tag !== "SubmitInput" || request.input._tag !== "GoalIntent") return [];
    const input = request.input.delivery;
    return [
      {
        input,
        receipt,
        historySequence: state.inputs?.find(
          (item) =>
            item.payload._tag === "GoalIntent" &&
            item.payload.intent.intentId === input.intent.intentId,
        )?.historySequence,
      },
    ];
  });
export const goalDeliveryRecords = (state: import("../src/goals/state.js").GoalState) =>
  (state.requests ?? []).flatMap(({ request, receipt }) => {
    if (request._tag !== "SubmitInput" || request.input._tag !== "PersonalMessage") return [];
    const input = request.input.delivery;
    return [
      {
        input,
        receipt,
        historySequence: state.inputs?.find(
          (item) =>
            item.payload._tag === "PersonalMessage" && item.payload.requestId === input.requestId,
        )?.historySequence,
      },
    ];
  });
