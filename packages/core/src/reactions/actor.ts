import { DurableContext } from "../context/store.js";
import {
  ApplicationError,
  RecoveryInput,
  RecoveryReply,
  type RecoveryReceipt,
} from "@aster/api-contracts";
import { recoveryReplay } from "../commands/recovery.js";
import { randomUUID } from "node:crypto";
import { ReplyTo, type ActorContext } from "@aster/actor";
import { Clock, Effect, Layer, Match, Option, Schema } from "effect";
import { ContextActor } from "../context/actor.js";
import { ContextRegistry } from "../context/registry.js";
import { defineContext } from "../context/definition.js";
import { contextView } from "../context/definition.js";
import { GoalSettings } from "../config/settings.js";
import { ReactionPolicy, ReactionFailure } from "./policy.js";
import {
  ReactionState,
  ReactionWork,
  ReactionPlan,
  ReactionReply,
  deliveriesOf,
  type ReactionPlanning,
} from "./state.js";

export const ReactionCommand = Schema.Union([
  Schema.TaggedStruct("Wake", {}),
  Schema.TaggedStruct("Ready", { replyTo: ReplyTo<void>() }),
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
const path = "/system-one";
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
    state: ReactionState,
    message: Schema.Never,
    view: contextView({ state: Schema.Struct({ work: Schema.Array(publicWork) }) }),
  }),
}) {
  static readonly layer = Layer.effect(
    SystemOneActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const durable = yield* DurableContext;
      const policy = yield* ReactionPolicy;
      const settings = yield* GoalSettings;
      let generation: string | undefined;
      const state = () => Schema.decodeUnknownSync(ReactionState)(registry.get(path)!.state);
      const save = Effect.fn("SystemOne.save")(function* (
        work: readonly ReactionWork[],
        recoveryReceipts: readonly RecoveryReceipt[] = state().recoveryReceipts ?? [],
      ) {
        const current = registry.get(path)!;
        yield* registry
          .commit(
            { ...current, state: { work, recoveryReceipts } },
            { expectedRevision: current.revision ?? 0 },
          )
          .pipe(Effect.orDie);
      });
      const update = (id: string, f: (work: ReactionWork) => ReactionWork) =>
        save(state().work.map((work) => (work.event.id === id ? f(work) : work)));
      const ingest = Effect.fn("SystemOne.ingest")(function* () {
        const existing = state().work;
        const known = new Set(existing.map((work) => work.event.id));
        const incoming: ReactionWork[] = [];
        for (const event of durable.journal()) {
          if (known.has(event.id)) continue;
          known.add(event.id);
          incoming.push({
            event,
            status: "pending",
            attempts: 0,
          });
        }
        if (incoming.length) yield* save([...existing, ...incoming]);
      });
      const drive = Effect.fn("SystemOne.drive")(function* (
        context: ActorContext<ReactionCommand>,
      ) {
        if (generation) return;
        for (const work of state().work) {
          if (work.status === "pending" || work.status === "planning") {
            // Freeze targets when this source reaches screening. Earlier queued work may
            // have legitimately advanced Goal/Signal revisions since source ingestion.
            const { [path]: _ownState, ...evidence } = registry.reader.snapshot();
            const { [work.event.record.path]: _source, ...otherEvidence } = evidence;
            const next: ReactionPlanning = {
              event: work.event,
              input:
                work.status === "pending"
                  ? {
                      evidence: otherEvidence,
                      goals: settings.definitions,
                      screeningAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
                    }
                  : work.input,
              status: "planning",
              attempts: work.attempts + 1,
            };
            yield* update(work.event.id, () => next);
            generation = randomUUID();
            const token = generation;
            yield* context.pipeToSelf(policy.plan(next), (result) => ({
              _tag: "Planned",
              generation: token,
              requestId: work.event.id,
              result,
            }));
            return;
          }
          if (work.status !== "ready") continue;
          const delivery = deliveriesOf(work).find(
            (item) => item.status === "pending" || (item.status === "unknown" && item.attempts < 3),
          );
          if (!delivery) continue;
          const id = delivery.command.input.requestId;
          yield* update(work.event.id, (item) =>
            item.status !== "ready"
              ? item
              : {
                  ...item,
                  deliveries: deliveriesOf(item).map((d) =>
                    d.command.input.requestId === id
                      ? { ...d, status: "sending", attempts: d.attempts + 1 }
                      : d,
                  ),
                },
          );
          generation = randomUUID();
          const token = generation;
          yield* context.pipeToSelf(policy.deliver(delivery.command), (result) => ({
            _tag: "Delivered",
            generation: token,
            requestId: work.event.id,
            deliveryId: id,
            result,
          }));
          return;
        }
      });
      return SystemOneActor.of({
        started: (context) =>
          Effect.gen(function* () {
            if (!registry.get(path))
              yield* registry
                .commit(
                  {
                    path,
                    description: "Context reaction processing",
                    state: { work: [] },
                    messages: [],
                  },
                  { expectedRevision: 0 },
                )
                .pipe(Effect.orDie);
            // Interrupted asks have no durable acknowledgement. Keep their original
            // identity and attempt count when applying the bounded retry policy.
            const recovered = state().work;
            if (recovered.some((work) => deliveriesOf(work).some((d) => d.status === "sending")))
              yield* save(
                recovered.map((work) =>
                  work.status !== "ready"
                    ? work
                    : {
                        ...work,
                        deliveries: deliveriesOf(work).map((d) =>
                          d.status === "sending"
                            ? {
                                ...d,
                                status: "unknown",
                                error: "Delivery interrupted before acknowledgement",
                              }
                            : d,
                        ),
                      },
                ),
              );
            yield* ingest();
            yield* drive(context);
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Ready", (command) => command.replyTo.tell(undefined)),
            Match.tag("Wake", () =>
              Effect.gen(function* () {
                yield* ingest();
                yield* drive(context);
              }),
            ),
            Match.tag("Continue", (command) =>
              Effect.gen(function* () {
                if (command.generation !== generation) return;
                generation = undefined;
                yield* drive(context);
              }),
            ),
            Match.tag("Recover", (command) =>
              Effect.gen(function* () {
                const recovered = yield* Effect.gen(function* () {
                  const input = command.input;
                  const replay = yield* recoveryReplay(
                    input,
                    registry.get(path)!.revision ?? 0,
                    state().recoveryReceipts ?? [],
                  );
                  if (Option.isSome(replay)) return replay.value;
                  const work = state().work.find((item) => item.event.id === input.workId);
                  if (!work)
                    return yield* new ApplicationError({
                      kind: "not-found",
                      message: "Screening work not found",
                    });
                  let next: ReactionWork;
                  if (input._tag === "RetryScreening") {
                    if (work.status !== "failed")
                      return yield* new ApplicationError({
                        kind: "conflict",
                        message: "Only failed screening can be retried",
                      });
                    next = { ...work, status: "planning" };
                  } else {
                    const delivery = deliveriesOf(work).find(
                      (item) => item.command.input.requestId === input.deliveryId,
                    );
                    if (work.status !== "ready" || !delivery || delivery.status !== "unknown")
                      return yield* new ApplicationError({
                        kind: "conflict",
                        message: "Only unknown delivery can be retried",
                      });
                    next = {
                      ...work,
                      deliveries: deliveriesOf(work).map((item) =>
                        item === delivery ? { ...item, status: "pending" } : item,
                      ),
                    };
                  }
                  const receipt = {
                    requestId: input.requestId,
                    revision: input.expectedRevision + 1,
                  };
                  yield* save(
                    state().work.map((item) => (item.event.id === work.event.id ? next : item)),
                    [...(state().recoveryReceipts ?? []), { input, receipt }],
                  );
                  return receipt;
                }).pipe(Effect.result);
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
                yield* update(command.requestId, (work) =>
                  work.status !== "planning"
                    ? work
                    : Match.value(command.result).pipe(
                        Match.tag("Failure", ({ error }) => ({
                          ...work,
                          status: "failed" as const,
                          error: error.message,
                        })),
                        Match.tag("Success", ({ value }) => ({
                          event: work.event,
                          attempts: work.attempts,
                          status: value.commands.length
                            ? ("ready" as const)
                            : ("completed" as const),
                          screenings: value.screenings,
                          deliveries: value.commands.map((command) => ({
                            command,
                            status: "pending" as const,
                            attempts: 0,
                          })),
                        })),
                        Match.exhaustive,
                      ),
                );
                generation = undefined;
                yield* drive(context);
              }),
            ),
            Match.tag("Delivered", (command) =>
              Effect.gen(function* () {
                if (command.generation !== generation) return;
                yield* update(command.requestId, (work) => {
                  if (work.status !== "ready") return work;
                  const deliveries = deliveriesOf(work).map((item) => {
                    if (item.command.input.requestId !== command.deliveryId) return item;
                    return Match.value(command.result).pipe(
                      Match.tag("Failure", ({ error }) => ({
                        ...item,
                        status: "unknown" as const,
                        error: error.message,
                      })),
                      Match.tag("Success", ({ value }) =>
                        Match.value(value).pipe(
                          Match.tag("Accepted", ({ receipt }) =>
                            receipt.requestId === command.deliveryId
                              ? { ...item, status: "delivered" as const, receipt, error: undefined }
                              : {
                                  ...item,
                                  status: "unknown" as const,
                                  error: "Receiver returned another delivery identity",
                                },
                          ),
                          Match.tag("Rejected", ({ error }) => ({
                            ...item,
                            status: "rejected" as const,
                            error: error.message,
                          })),
                          Match.exhaustive,
                        ),
                      ),
                      Match.exhaustive,
                    );
                  });
                  return {
                    ...work,
                    deliveries,
                    status: deliveries.every(
                      (d) => d.status === "delivered" || d.status === "rejected",
                    )
                      ? "completed"
                      : "ready",
                  };
                });
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
  );
}
