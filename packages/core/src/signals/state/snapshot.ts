import { GoalPath } from "../../goals/contracts.js";
import { SignalTrigger, SignalTime } from "../contracts.js";
import { Task } from "../../tasks/contracts.js";
import { type PublicContext } from "../../context/contracts.js";
import { Schema } from "effect";
import { contextView } from "../../context/view.js";

export { SignalTime } from "../contracts.js";
export const SignalSnapshot = Schema.Struct({
  owner: Schema.optional(GoalPath),
  trigger: SignalTrigger,
  task: Task,
  status: Schema.Literals(["active", "paused", "deleted"]),
  version: Schema.Int.check(Schema.isGreaterThan(0)),
  nextDue: Schema.optional(Schema.NullOr(SignalTime)),
}).check(
  Schema.makeFilter(
    (state) =>
      state.trigger._tag === "Schedule" ? state.nextDue !== undefined : state.nextDue === undefined,
    { expected: "Scheduled Signals retain a cursor; Context Signals have no timer cursor" },
  ),
);
export type SignalSnapshot = typeof SignalSnapshot.Type;

export const signalEnabled = (
  state: Pick<SignalSnapshot, "status" | "owner">,
  getGoal: (path: string) => PublicContext | undefined,
) =>
  state.status === "active" &&
  (!state.owner ||
    (getGoal(state.owner)?.state as { status?: string } | undefined)?.status === "active");

export const signalView = contextView({
  matches: (path) => /^\/signals\/[^/]+$/.test(path),
  state: SignalSnapshot,
});
export const signalsRootView = contextView({
  matches: (path) => path === "/signals",
  state: Schema.Struct({}),
});
