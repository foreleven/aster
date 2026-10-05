import { Actor, ReplyTo, type ActorRef } from "@aster/actor";
import { ApplicationError } from "@aster/api-contracts";
import { Deferred, Effect, Layer, Schema } from "effect";
import { ContextRegistry } from "../context/registry.js";
import { GoalSettings } from "../config/settings.js";
import { AgentConversations } from "@aster/agent";
import { GoalSignals } from "../signals/goal-owner.js";
import { AgentRunner } from "@aster/agent";
import { MemoryRecall } from "../memory/contracts.js";
import { ExternalAgents } from "../tasks/model.js";
import { GoalActor, type GoalMailbox } from "./actors.js";
import { GoalCommand, GoalReadyReply } from "./protocol.js";
export const GoalsRootCommand = Schema.Union([
  Schema.TaggedStruct("AwaitReady", {
    stage: Schema.optional(Schema.Literals(["restored", "activated"])),
    replyTo: ReplyTo<GoalReadyReply>(),
  }),
  Schema.TaggedStruct("Route", { slug: Schema.String, command: GoalCommand }),
  Schema.TaggedStruct("Initialize", {}),
]);
const GoalsRootMailbox = Schema.Union([
  GoalsRootCommand,
  Schema.TaggedStruct("ReadinessSettled", {
    replyTo: ReplyTo<GoalReadyReply>(),
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: GoalReadyReply }),
      Schema.TaggedStruct("Failure", { error: ApplicationError }),
    ]),
  }),
]);
export type GoalsRootCommand = typeof GoalsRootCommand.Type;
export class GoalsRootActor extends Actor.Service<
  GoalsRootActor,
  | ContextRegistry
  | GoalSignals
  | GoalSettings
  | AgentConversations
  | AgentRunner
  | MemoryRecall
  | ExternalAgents
>()("goals/RootActor", { command: GoalsRootMailbox }) {
  static readonly layer = Layer.effect(
    GoalsRootActor,
    Effect.gen(function* () {
      const settings = yield* GoalSettings;
      const activation = yield* Deferred.make<void>();
      return GoalsRootActor.of({
        started: (context) =>
          Effect.gen(function* () {
            for (const goal of settings.definitions)
              if (!(yield* context.child(goal.slug)))
                yield* context.spawn(goal.slug, GoalActor, {
                  metadata: { goalActivation: activation },
                });
          }),
        receive: (command, context) =>
          Effect.gen(function* () {
            if (command._tag === "Route") {
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
              return;
            }
            if (command._tag === "ReadinessSettled") {
              yield* command.replyTo.tell(
                command.result._tag === "Success"
                  ? command.result.value
                  : { _tag: "Failed", error: command.result.error },
              );
              return;
            }
            const children = (yield* context.children()) as readonly ActorRef<GoalMailbox>[];
            if (command._tag === "Initialize") {
              yield* Deferred.succeed(activation, undefined);
              for (const goal of children) yield* goal.tell({ _tag: "Activate" });
              return;
            }
            // Child recovery must never hold the routing mailbox or block Initialize.
            yield* context.pipeToSelf(
              Effect.gen(function* () {
                for (const goal of children) {
                  const result = yield* goal.ask<GoalReadyReply>((replyTo) => ({
                    _tag: "AwaitReady",
                    stage: command.stage,
                    replyTo,
                  }));
                  if (result._tag === "Failed") return result;
                }
                return { _tag: "Ready" } as const;
              }).pipe(
                Effect.mapError(
                  (error) =>
                    new ApplicationError({
                      kind: "unavailable",
                      message: error.message,
                    }),
                ),
              ),
              (result) => ({ _tag: "ReadinessSettled", replyTo: command.replyTo, result }),
            );
          }),
      });
    }),
  );
}
