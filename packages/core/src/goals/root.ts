import { GoalCommand, type GoalMailbox } from "./protocol.js";
import { GoalActor } from "./actor.js";
import { ExternalAgents } from "../tasks/execution/contracts.js";
import { AgentRunner, AgentConversations } from "@aster/agent";
import { GoalSettings } from "../config/settings.js";
import { ContextRegistry } from "../context/registry.js";
import { Effect, Layer, Schema } from "effect";
import { ApplicationError } from "../operations.js";
import { Actor, type ActorRef } from "@aster/actor";

export const GoalsRootCommand = Schema.TaggedStruct("Route", {
  slug: Schema.String,
  command: GoalCommand,
});
export type GoalsRootCommand = typeof GoalsRootCommand.Type;
export class GoalsRootActor extends Actor.Service<
  GoalsRootActor,
  ContextRegistry | GoalSettings | AgentConversations | AgentRunner | ExternalAgents
>()("goals/RootActor", { command: GoalsRootCommand }) {
  static readonly layer = Layer.effect(
    GoalsRootActor,
    Effect.gen(function* () {
      const settings = yield* GoalSettings;
      return GoalsRootActor.of({
        started: (context) =>
          Effect.gen(function* () {
            for (const goal of settings.definitions) {
              const child =
                (yield* context.child(goal.slug)) ??
                (yield* context.spawn(goal.slug, GoalActor, {
                  metadata: { goalActivation: context.metadata.goalActivation },
                }));
              yield* context.watch(child);
            }
          }),
        // Supervision owns retries. A terminal child failure must not stop sibling Goals.
        receiveSignal: (signal) =>
          Effect.logError({
            event: "goal.actor.terminated",
            actorPath: signal.ref.path,
            cause: signal.cause,
          }),
        receive: (command, context) =>
          Effect.gen(function* () {
            const child = settings.definitions.some((goal) => goal.slug === command.slug)
              ? yield* context.child(command.slug)
              : undefined;
            if (child) yield* (child as ActorRef<GoalMailbox>).tell(command.command);
            else
              yield* command.command.replyTo.tell({
                _tag: "Rejected",
                error: new ApplicationError({
                  kind: "not-found",
                  message: "Goal Actor unavailable",
                }),
              });
          }),
      });
    }),
  );
}
