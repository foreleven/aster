import { RecoveryInput, RecoveryReply, RecoveryReceipt } from "@aster/api-contracts";
import { recoveryReplay } from "../context/recovery.js";
import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { ReplyTo, type ActorContext, type ActorRef } from "@aster/actor";
import { BusinessNotification, CommandReceipt, ApplicationError } from "@aster/api-contracts";
import { Effect, Layer, Match, Option, Schema } from "effect";
import { ContextActor } from "../context/actor.js";
import { defineContext } from "../context/model.js";
import { contextView } from "../context/view.js";
import { ContextRegistry } from "../context/registry.js";
import type { PersonalCommand, PersonalReply } from "../personal/actor.js";
import { BusinessOutbox } from "./inbox.js";
import { NotificationSource } from "./event.js";

export const NotificationDelivery = Schema.Struct({
  input: BusinessNotification,
  status: Schema.Literals(["pending", "sending", "unknown", "delivered", "rejected"]),
  attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  receipt: Schema.optional(CommandReceipt),
  error: Schema.optional(Schema.String),
});
export type NotificationDelivery = typeof NotificationDelivery.Type;
export const NotificationState = Schema.Struct({
  deliveries: Schema.Array(NotificationDelivery),
  recoveryReceipts: Schema.optional(Schema.Array(RecoveryReceipt)),
}).check(
  Schema.makeFilter(
    ({ deliveries, recoveryReceipts = [] }) =>
      new Set(recoveryReceipts.map((entry) => entry.input.requestId)).size ===
        recoveryReceipts.length &&
      recoveryReceipts.every(
        ({ input }) =>
          input._tag === "RetryNotification" &&
          deliveries.some((item) => item.input.requestId === input.deliveryId),
      ) &&
      new Set(deliveries.map((d) => d.input.requestId)).size === deliveries.length &&
      deliveries.every(
        (d) =>
          Schema.is(NotificationSource)(d.input.source) &&
          Number.isFinite(Date.parse(d.input.createdAt)) &&
          (d.status === "pending" || d.attempts > 0) &&
          (d.receipt === undefined || d.receipt.requestId === d.input.requestId) &&
          (d.status !== "delivered" || d.receipt !== undefined),
      ),
    { expected: "Unique notification deliveries with matching receipts" },
  ),
);
const Commands = Schema.Union([
  Schema.TaggedStruct("Wake", {}),
  Schema.TaggedStruct("Ready", { replyTo: ReplyTo<void>() }),
  Schema.TaggedStruct("Continue", { generation: Schema.String }),
  Schema.TaggedStruct("Recover", { input: RecoveryInput, replyTo: ReplyTo<RecoveryReply>() }),
  Schema.TaggedStruct("Settled", {
    generation: Schema.String,
    requestId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: CommandReceipt }),
      Schema.TaggedStruct("Failure", { error: ApplicationError }),
    ]),
  }),
]);
export type NotificationCommand = typeof Commands.Type;
type Commands = NotificationCommand;
const path = "/notifications";

/** Internal outbox delivery; Personal receives business commands, never ContextChange. */
export class NotificationsActor extends ContextActor.Service<NotificationsActor>()(
  "notifications/Actor",
  {
    command: Commands,
    context: defineContext({
      identity: "Business notification delivery",
      state: NotificationState,
      message: Schema.Never,
      view: contextView({
        state: Schema.Struct({
          deliveries: Schema.Array(
            Schema.Struct({
              input: Schema.Struct({
                requestId: Schema.String,
                source: Schema.String,
                target: Schema.String,
                kind: Schema.String,
                revision: Schema.Int,
              }),
              status: NotificationDelivery.fields.status,
              attempts: Schema.Int,
              receipt: Schema.optional(CommandReceipt),
              error: Schema.optional(Schema.String),
            }),
          ),
        }),
      }),
    }),
  },
) {
  static readonly layer = Layer.effect(
    NotificationsActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      let generation: string | undefined;
      const state = () => Schema.decodeUnknownSync(NotificationState)(registry.get(path)!.state);
      const save = Effect.fn("Notifications.save")(function* (
        deliveries: readonly NotificationDelivery[],
        recoveryReceipts: readonly RecoveryReceipt[] = state().recoveryReceipts ?? [],
      ) {
        const current = registry.get(path)!;
        yield* registry
          .commit(
            { ...current, state: { deliveries, recoveryReceipts } },
            { expectedRevision: current.revision ?? 0 },
          )
          .pipe(Effect.orDie);
      });
      const ingest = Effect.fn("Notifications.ingest")(function* () {
        const existing = state().deliveries;
        const known = new Map(existing.map((item) => [item.input.requestId, item.input]));
        const pending: NotificationDelivery[] = [];
        for (const record of Object.values(registry.snapshot())) {
          if (!Schema.is(NotificationSource)(record.path)) continue;
          if (!Object.hasOwn(record.state, "businessOutbox")) continue;
          const decoded = yield* Schema.decodeUnknownEffect(BusinessOutbox)(record.state).pipe(
            Effect.orDie,
          );
          for (const input of decoded.businessOutbox) {
            if (input.source !== record.path || input.revision > (record.revision ?? 0))
              return yield* Effect.die(
                new Error("Business notification owner or revision mismatch"),
              );
            const previous = known.get(input.requestId);
            if (previous) {
              if (!isDeepStrictEqual(previous, input))
                return yield* Effect.die(
                  new Error("Committed business notification identity changed"),
                );
              continue;
            }
            known.set(input.requestId, input);
            pending.push({ input, status: "pending", attempts: 0 });
          }
        }
        if (pending.length) yield* save([...existing, ...pending]);
      });
      const drive = Effect.fn("Notifications.drive")(function* (actor: ActorContext<Commands>) {
        if (generation) return;
        const next = state().deliveries.find(
          (d) => d.status === "pending" || (d.status === "unknown" && d.attempts < 3),
        );
        if (!next) return;
        yield* save(
          state().deliveries.map((d) =>
            d.input.requestId === next.input.requestId
              ? { ...d, status: "sending", attempts: d.attempts + 1 }
              : d,
          ),
        );
        generation = randomUUID();
        const token = generation;
        const deliver = Effect.gen(function* () {
          const ref = yield* actor.select("/user/personal").resolve();
          const reply = yield* (ref as ActorRef<PersonalCommand>).ask<PersonalReply>((replyTo) => ({
            _tag: "Notify",
            input: next.input,
            replyTo,
          }));
          if (reply._tag === "Rejected") return yield* reply.error;
          if (reply._tag !== "Accepted" || reply.receipt.requestId !== next.input.requestId)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Personal notification receipt does not match the event",
            });
          return reply.receipt;
        }).pipe(
          Effect.catchTags({
            ActorNotFound: () =>
              Effect.fail(
                new ApplicationError({
                  kind: "unavailable",
                  message: "Personal Agent unavailable",
                }),
              ),
            AskTimeoutError: () =>
              Effect.fail(
                new ApplicationError({
                  kind: "unavailable",
                  message: "Notification acknowledgement missing; outcome unknown",
                }),
              ),
          }),
        );
        yield* actor.pipeToSelf(deliver, (result) => ({
          _tag: "Settled",
          generation: token,
          requestId: next.input.requestId,
          result,
        }));
      });
      return NotificationsActor.of({
        started: (actor) =>
          Effect.gen(function* () {
            if (!registry.get(path))
              yield* registry
                .commit(
                  {
                    path,
                    description: "Business notification delivery",
                    state: { deliveries: [] },
                    messages: [],
                  },
                  { expectedRevision: 0 },
                )
                .pipe(Effect.orDie);
            if (state().deliveries.some((d) => d.status === "sending"))
              yield* save(
                state().deliveries.map((d) =>
                  d.status === "sending"
                    ? {
                        ...d,
                        status: "unknown",
                        error: "Delivery interrupted before acknowledgement",
                      }
                    : d,
                ),
              );
            yield* ingest();
            yield* drive(actor);
          }),
        receive: (command, actor) =>
          Match.value(command).pipe(
            Match.tag("Ready", ({ replyTo }) => replyTo.tell(undefined)),
            Match.tag("Wake", () =>
              Effect.gen(function* () {
                yield* ingest();
                yield* drive(actor);
              }),
            ),
            Match.tag("Continue", (command) =>
              Effect.gen(function* () {
                if (command.generation !== generation) return;
                generation = undefined;
                yield* drive(actor);
              }),
            ),
            Match.tag("Recover", (command) =>
              Effect.gen(function* () {
                const result = yield* Effect.gen(function* () {
                  const input = command.input;
                  if (input._tag !== "RetryNotification")
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
                  const previous = state().deliveries.find(
                    (item) => item.input.requestId === input.deliveryId,
                  );
                  if (!previous || previous.status !== "unknown")
                    return yield* new ApplicationError({
                      kind: "conflict",
                      message: "Only unknown notification delivery can be retried",
                    });
                  const receipt = {
                    requestId: input.requestId,
                    revision: input.expectedRevision + 1,
                  };
                  yield* save(
                    state().deliveries.map((item) =>
                      item.input.requestId === previous.input.requestId
                        ? { ...item, status: "pending" }
                        : item,
                    ),
                    [...(state().recoveryReceipts ?? []), { input, receipt }],
                  );
                  return receipt;
                }).pipe(Effect.result);
                if (result._tag === "Failure")
                  return yield* command.replyTo.tell({ _tag: "Rejected", error: result.failure });
                yield* command.replyTo.tell({ _tag: "Accepted", receipt: result.success });
                yield* drive(actor);
              }),
            ),
            Match.tag("Settled", (command) =>
              Effect.gen(function* () {
                if (command.generation !== generation) return;
                yield* save(
                  state().deliveries.map((d) =>
                    d.input.requestId !== command.requestId
                      ? d
                      : Match.value(command.result).pipe(
                          Match.tag("Success", ({ value }) => ({
                            ...d,
                            error: undefined,
                            status: "delivered" as const,
                            receipt: value,
                          })),
                          Match.tag("Failure", ({ error }) => ({
                            ...d,
                            status:
                              error.kind === "unavailable"
                                ? ("unknown" as const)
                                : ("rejected" as const),
                            error: error.message,
                          })),
                          Match.exhaustive,
                        ),
                  ),
                );
                if (
                  command.result._tag === "Failure" &&
                  command.result.error.kind === "unavailable"
                ) {
                  const token = generation;
                  yield* actor.pipeToSelf(Effect.sleep("3 seconds"), () => ({
                    _tag: "Continue",
                    generation: token,
                  }));
                } else {
                  generation = undefined;
                  yield* drive(actor);
                }
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}
