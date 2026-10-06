import type { ReplyTo } from "@aster/actor";
import { Effect } from "effect";
import type { GoalCommandReply } from "../src/goals/protocol.js";

/** Tests that observe committed state can discard an explicit admission acknowledgement. */
export const goalTestReply: ReplyTo<GoalCommandReply> = {
  path: "/test/goal-reply",
  incarnation: "test",
  awaitStarted: Effect.void,
  tell: () => Effect.void,
  ask: () => Effect.die("The reply sink cannot receive requests"),
};

/** Observe durable input projection independently of compact command receipts. */
export const goalIntentRecords = (state: import("../src/goals/state/snapshot.js").GoalSnapshot) =>
  state.inputs.filter((input) => input.kind === "GoalIntent");
