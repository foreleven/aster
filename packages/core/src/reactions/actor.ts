import { CurrentActors } from "../tools/actors.js";
import { ReactionState } from "./model.js";
import { DurableContext } from "../context/store.js";
import { RecoveryInput, RecoveryReply } from "@aster/api-contracts";
import { randomUUID } from "node:crypto";
import { ReplyTo, type ActorContext } from "@aster/actor";
import { Effect, Layer, Match, Schema, Stream } from "effect";
import { ContextActor } from "../context/actor.js";
import { ContextRegistry } from "../context/registry.js";
import { defineContext } from "../context/definition.js";
import { contextView } from "../context/definition.js";
import { GoalSettings } from "../config/settings.js";
import { ReactionPolicy, ReactionFailure } from "./policy.js";
import { ReactionSnapshot, ReactionPlan, ReactionReply, deliveriesOf } from "./state.js";

export const ReactionCommand = Schema.Union([
  Schema.TaggedStruct("Wake", {}),
  Schema.TaggedStruct("Recover", { input: RecoveryInput, replyTo: ReplyTo<RecoveryReply>() }),
  Schema.TaggedStruct("Continue", { generation: Schema.String }),
  Schema.TaggedStruct("Planned", {
    generation: Schema.String,
    requestId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: ReactionPlan }),
      Schema.TaggedStruct("Failure", { error: ReactionFailure }),
    ]),
  }),
  Schema.TaggedStruct("Delivered", {
    generation: Schema.String,
    requestId: Schema.String,
    deliveryId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: ReactionReply }),
      Schema.TaggedStruct("Failure", { error: ReactionFailure }),
    ]),
  }),
]);
export type ReactionCommand = typeof ReactionCommand.Type;
const publicWork = Schema.Struct({
  event: Schema.Struct({
    id: Schema.String,
    record: Schema.Struct({ path: Schema.String, revision: Schema.Number }),
    createdAt: Schema.String,
  }),
  status: Schema.String,
  attempts: Schema.Int,
  error: Schema.optional(Schema.String),
  deliveries: Schema.optional(
    Schema.Array(
      Schema.Struct({
        command: Schema.Struct({
          _tag: Schema.String,
          input: Schema.Struct({ requestId: Schema.String, target: Schema.String }),
        }),
        status: Schema.String,
        attempts: Schema.Int,
        error: Schema.optional(Schema.String),
        receipt: Schema.optional(Schema.Struct({ requestId: Schema.String, revision: Schema.Int })),
      }),
    ),
  ),
});

/** The sole writer of the reaction inbox, frozen decisions and delivery outcomes. */
export class SystemOneActor extends ContextActor.Service<
  SystemOneActor,
  ReactionPolicy | GoalSettings | DurableContext
>()("context/SystemOneActor", {
  command: ReactionCommand,
  context: defineContext({
    state: ReactionSnapshot,
    message: Schema.Never,
    view: contextView({ state: Schema.Struct({ work: Schema.Array(publicWork) }) }),
  }),
}) {
  static readonly layer = Layer.effect(
    SystemOneActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const changes = yield* registry.subscribe;
      const state = yield* ReactionState;
      const policy = yield* ReactionPolicy;
      let generation: string | undefined;
      const drive = Effect.fn("SystemOne.drive")(function* (
        context: ActorContext<ReactionCommand>,
      ) {
        if (generation) return;
        for (const work of (yield* state.read).work) {
          if (work.status === "pending" || work.status === "planning") {
            const planned = yield* state.startPlanning(work);
            const token = (generation = randomUUID());
            yield* context.pipeToSelf(policy.plan(planned), (result) => ({
              _tag: "Planned",
              generation: token,
              requestId: work.event.id,
              result,
            }));
            return;
          }
          const delivery = deliveriesOf(work).find(
            (item) => item.status === "pending" || (item.status === "unknown" && item.attempts < 3),
          );
          if (!delivery) continue;
          const id = delivery.command.input.requestId;
          yield* state.startDelivery(work.event.id, id);
          const token = (generation = randomUUID());
          yield* context.pipeToSelf(
            policy.deliver(delivery.command).pipe(Effect.provideService(CurrentActors, context)),
            (result) => ({
              _tag: "Delivered",
              generation: token,
              requestId: work.event.id,
              deliveryId: id,
              result,
            }),
          );
          return;
        }
      });
      return SystemOneActor.of({
        started: (context) =>
          Effect.gen(function* () {
            yield* state.restore;
            yield* context.pipeToSelf(
              Stream.runForEach(changes, () => context.self.tell({ _tag: "Wake" })),
              () => ({ _tag: "Wake" }),
            );
            yield* state.ingest();
            yield* drive(context);
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Wake", () => state.ingest().pipe(Effect.andThen(drive(context)))),
            Match.tag("Continue", (command) =>
              Effect.gen(function* () {
                if (command.generation !== generation) return;
                generation = undefined;
                yield* drive(context);
              }),
            ),
            Match.tag("Recover", (command) =>
              Effect.gen(function* () {
                const recovered = yield* state.recover(command.input).pipe(Effect.result);
                if (recovered._tag === "Failure")
                  return yield* command.replyTo.tell({
                    _tag: "Rejected",
                    error: recovered.failure,
                  });
                yield* command.replyTo.tell({ _tag: "Accepted", receipt: recovered.success });
                yield* drive(context);
              }),
            ),
            Match.tag("Planned", (command) =>
              Effect.gen(function* () {
                if (command.generation !== generation) return;
                yield* state.planned(command.requestId, command.result);
                generation = undefined;
                yield* drive(context);
              }),
            ),
            Match.tag("Delivered", (command) =>
              Effect.gen(function* () {
                if (command.generation !== generation) return;
                yield* state.delivered(command.requestId, command.deliveryId, command.result);
                if (command.result._tag === "Failure") {
                  const token = generation;
                  yield* context.pipeToSelf(Effect.sleep("3 seconds"), () => ({
                    _tag: "Continue",
                    generation: token,
                  }));
                } else {
                  generation = undefined;
                  yield* drive(context);
                }
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  ).pipe(Layer.provide(ReactionState.layer));
}
