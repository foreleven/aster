import { randomUUID } from "node:crypto";
import type { ActorSystem, ActorRef } from "@aster/actor";
import { Effect } from "effect";
import type { GoalsRootCommand } from "../src/goals/root.js";
import type { GoalCommandReply, GoalRequestData } from "../src/goals/protocol.js";

export const goalCommand = Effect.fnUntraced(function* (
  actors: Pick<ActorSystem, "select">,
  slug: string,
  command: GoalRequestData,
) {
  const root = yield* actors.select("/user/goals").resolve();
  const reply = yield* (root as ActorRef<GoalsRootCommand>).ask<GoalCommandReply>((replyTo) => ({
    _tag: "Route",
    slug,
    command: { ...command, replyTo },
  }));
  if (reply._tag === "Rejected") return yield* reply.error;
});
export const submitGoal = (
  actors: Pick<ActorSystem, "select">,
  slug: string,
  text: string,
  requestId: string = randomUUID(),
) =>
  goalCommand(actors, slug, { _tag: "SubmitInput", requestId, input: { _tag: "UserInput", text } });
