import {
  ApplicationError,
  RecoveryInput,
  RecoveryReply,
  type RecoveryReceipt,
} from "@aster/api-contracts";
import { recoveryReplay } from "./recovery.js";
import { randomUUID } from "node:crypto";
import { ReplyTo, type ActorContext } from "@aster/actor";
import { Clock, Effect, Layer, Match, Option, Schema } from "effect";
import { ContextActor } from "./actor.js";
import { ContextRegistry } from "./registry.js";
import { defineContext } from "./model.js";
import { contextView } from "./view.js";
import { GoalSettings } from "../config/settings.js";
import { ReactionPolicy, ReactionFailure } from "./reaction-policy.js";
import { ReactionState, ReactionWork, ReactionPlan, ReactionReply } from "./reaction-state.js";

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
    requestId: Schema.String,
    source: Schema.String,
    revision: Schema.Number,
    createdAt: Schema.String,
  }),
  status: ReactionWork.fields.status,
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
  ReactionPolicy | GoalSettings
>()("context/SystemOneActor", {
  command: ReactionCommand,
  context: defineContext({
    identity: "Context reaction processing",
    state: ReactionState,
    message: Schema.Never,
    view: contextView({ state: Schema.Struct({ work: Schema.Array(publicWork) }) }),
  }),
}) {
  static readonly layer = Layer.effect(
    SystemOneActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
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
        save(state().work.map((work) => (work.event.requestId === id ? f(work) : work)));
      const ingest = Effect.fn("SystemOne.ingest")(function* () {
        const existing = state().work;
        const known = new Set(existing.map((work) => work.event.requestId));
        const admittedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
        const incoming: ReactionWork[] = [];
        for (const record of Object.values(registry.snapshot())) {
          for (const event of record.reactionEvents ?? []) {
            if (known.has(event.requestId)) continue;
            known.add(event.requestId);
            incoming.push({
              event,
              snapshot: {},
              goals: [],
              admittedAt,
              status: "pending",
              attempts: 0,
            });
          }
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
            const { [path]: _ownState, ...evidence } = registry.publicSnapshot();
            const next = {
              ...work,
              ...(work.status === "pending"
                ? {
                    snapshot: { ...evidence, [work.event.source]: work.event.record },
                    goals: settings.definitions,
                    admittedAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
                  }
                : {}),
              status: "planning" as const,
              attempts: work.attempts + 1,
            };
            yield* update(work.event.requestId, () => next);
            generation = randomUUID();
            const token = generation;
            yield* context.pipeToSelf(policy.plan(next), (result) => ({
              _tag: "Planned",
              generation: token,
              requestId: work.event.requestId,
              result,
            }));
            return;
          }
          if (work.status !== "ready") continue;
          const delivery = work.deliveries?.find(
            (item) => item.status === "pending" || (item.status === "unknown" && item.attempts < 3),
          );
          if (!delivery) continue;
          const id = delivery.command.input.requestId;
          yield* update(work.event.requestId, (item) => ({
            ...item,
            deliveries: item.deliveries!.map((d) =>
              d.command.input.requestId === id
                ? { ...d, status: "sending", attempts: d.attempts + 1 }
                : d,
            ),
          }));
          generation = randomUUID();
          const token = generation;
          yield* context.pipeToSelf(policy.deliver(delivery.command), (result) => ({
            _tag: "Delivered",
            generation: token,
            requestId: work.event.requestId,
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
            if (recovered.some((work) => work.deliveries?.some((d) => d.status === "sending")))
              yield* save(
                recovered.map((work) => ({
                  ...work,
                  deliveries: work.deliveries?.map((d) =>
                    d.status === "sending"
                      ? {
                          ...d,
                          status: "unknown",
                          error: "Delivery interrupted before acknowledgement",
                        }
                      : d,
                  ),
                })),
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
                  if (input._tag === "RetryNotification")
                    return yield* new ApplicationError({
                      kind: "invalid-input",
                      message: "Recovery addressed to another owner",
                    });
                  const replay = yield* recoveryReplay(
                    input,
                    registry.get(path)!.revision ?? 0,
                    state().recoveryReceipts ?? [],
                  );
                  if (Option.isSome(replay)) return replay.value;
                  const work = state().work.find((item) => item.event.requestId === input.workId);
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
                    const delivery = work.deliveries?.find(
                      (item) => item.command.input.requestId === input.deliveryId,
                    );
                    if (!delivery || delivery.status !== "unknown")
                      return yield* new ApplicationError({
                        kind: "conflict",
                        message: "Only unknown delivery can be retried",
                      });
                    next = {
                      ...work,
                      deliveries: work.deliveries!.map((item) =>
                        item === delivery ? { ...item, status: "pending" } : item,
                      ),
                    };
                  }
                  const receipt = {
                    requestId: input.requestId,
                    revision: input.expectedRevision + 1,
                  };
                  yield* save(
                    state().work.map((item) =>
                      item.event.requestId === work.event.requestId ? next : item,
                    ),
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
                  Match.value(command.result).pipe(
                    Match.tag("Failure", ({ error }) => ({
                      ...work,
                      status: "failed" as const,
                      error: error.message,
                    })),
                    Match.tag("Success", ({ value }) => ({
                      ...work,
                      error: undefined,
                      status: value.commands.length ? ("ready" as const) : ("completed" as const),
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
                  const deliveries = work.deliveries!.map((item) => {
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
