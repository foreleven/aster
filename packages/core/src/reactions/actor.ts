import { ReactionSettings } from "./settings.js";
import { GetProcessing } from "./queries.js";
import { publicJson } from "../json.js";
import type { MailboxOf } from "@aster/actor";
import { Command as ActorCommand, type ActorContext } from "@aster/actor";
import { Context, Effect, Layer, Match, Schema, Stream } from "effect";
import { randomUUID } from "node:crypto";
import { ContextActor } from "../context/actor.js";
import { ContextEvent } from "../context/model.js";
import { DurableContext } from "../context/store.js";
import { CurrentActors } from "../services/actors.js";
import { RecoveryInput, RecoveryReply } from "./contracts.js";

import { ReactionState } from "./model.js";
import { ReactionFailure, ReactionPolicy } from "./policy.js";
import { ReactionPlan, ReactionReply, deliveriesOf, workStatus } from "./state.js";

export class Ingest extends ActorCommand.Class<Ingest>()("Ingest", {
  payload: { events: Schema.Array(ContextEvent) },
}) {}
export class Recover extends ActorCommand.Class<Recover>()("Recover", {
  payload: { input: RecoveryInput },
  reply: RecoveryReply,
}) {}

export const ReactionCommands = [Ingest, Recover, GetProcessing] as const;
export const ReactionInternal = Schema.TaggedUnion({
  Continue: { deliveryId: Schema.String, generation: Schema.String },
  Planned: {
    generation: Schema.String,
    requestId: Schema.String,
    result: Schema.TaggedUnion({
      Success: { value: ReactionPlan },
      Failure: { error: ReactionFailure },
    }),
  },
  Delivered: {
    generation: Schema.String,
    requestId: Schema.String,
    deliveryId: Schema.String,
    result: Schema.TaggedUnion({
      Success: { value: ReactionReply },
      Failure: { error: ReactionFailure },
    }),
  },
});
export type ReactionCommand = MailboxOf<typeof ReactionCommands, typeof ReactionInternal>;
/** The sole writer of the reaction inbox, frozen decisions and delivery outcomes. */
export const SystemOneActor = ContextActor.define("context/SystemOneActor", {
  commands: ReactionCommands,
  internal: ReactionInternal,
})(
  Effect.gen(function* () {
    const registry = yield* DurableContext;
    const changes = yield* registry.subscribe;
    const services = yield* Layer.buildWithScope(ReactionState.layer, yield* Effect.scope);
    const state = Context.get(services, ReactionState);
    const policy = yield* ReactionPolicy;
    const durable = yield* DurableContext;
    const { matchConcurrency, deliveryConcurrency } = yield* ReactionSettings;
    let planning: string | undefined;
    // Mailbox-owned slots, including retry cooldowns. A slow receiver never blocks matching.
    const delivering = new Map<string, { generation: string; target: string }>();
    const drive = Effect.fn("SystemOne.drive")(function* (context: ActorContext<ReactionCommand>) {
      const snapshot = yield* state.read;
      if (!planning) {
        const work = snapshot.work.find(
          (item) => item.status === "queued" || workStatus(item) === "planning",
        );
        if (work) {
          const planned = yield* state.startPlanning(work);
          const token = (planning = randomUUID());
          yield* context.pipeToSelf(policy.plan(planned, matchConcurrency), (result) => ({
            _tag: "Planned",
            generation: token,
            requestId: work.event.id,
            result,
          }));
        }
      }
      const busyTargets = new Set([...delivering.values()].map((slot) => slot.target));
      for (const work of snapshot.work) {
        for (const delivery of deliveriesOf(work)) {
          if (delivering.size >= deliveryConcurrency) return;
          const { target, requestId: id } = delivery.command.input;
          if (busyTargets.has(target)) continue;
          if (
            delivery.status !== "pending" &&
            !(delivery.status === "unknown" && delivery.attempts < 3)
          )
            continue;
          yield* state.startDelivery(work.event.id, id);
          const token = randomUUID();
          delivering.set(id, { generation: token, target });
          busyTargets.add(target);
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
        }
      }
    });
    return {
      started: (context: ActorContext<ReactionCommand>) =>
        Effect.gen(function* () {
          yield* state.restore;
          yield* context.pipeToSelf(
            Stream.runForEach(changes, (change) =>
              change.events?.length
                ? context.self.tell({ _tag: "Ingest", events: change.events })
                : Effect.void,
            ),
            () => ({ _tag: "Ingest", events: [] }),
          );
          yield* state.ingest(durable.journal());
          yield* drive(context);
        }),
      receive: (command: ReactionCommand, context: ActorContext<ReactionCommand>) =>
        Match.value(command).pipe(
          Match.tag("GetProcessing", ({ replyTo }) =>
            state.snapshot.pipe(
              Effect.flatMap((value) =>
                replyTo.tell(publicJson({ revision: value.revision, state: value.state })),
              ),
            ),
          ),
          Match.tag("Ingest", ({ events }) =>
            state.ingest(events).pipe(Effect.andThen(drive(context))),
          ),
          Match.tag("Continue", (command) =>
            Effect.gen(function* () {
              if (delivering.get(command.deliveryId)?.generation !== command.generation) return;
              delivering.delete(command.deliveryId);
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
              if (command.generation !== planning) return;
              yield* state.planned(command.requestId, command.result);
              planning = undefined;
              yield* drive(context);
            }),
          ),
          Match.tag("Delivered", (command) =>
            Effect.gen(function* () {
              if (delivering.get(command.deliveryId)?.generation !== command.generation) return;
              yield* state.delivered(command.requestId, command.deliveryId, command.result);
              if (command.result._tag === "Failure") {
                yield* context.pipeToSelf(Effect.sleep("3 seconds"), () => ({
                  _tag: "Continue",
                  deliveryId: command.deliveryId,
                  generation: command.generation,
                }));
              } else delivering.delete(command.deliveryId);
              yield* drive(context);
            }),
          ),
          Match.exhaustive,
        ),
    };
  }),
);

export { GetProcessing } from "./queries.js";
