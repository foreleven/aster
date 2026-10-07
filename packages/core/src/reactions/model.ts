import { Clock, Context, Effect, Layer, Option, Ref, Schema, Match } from "effect";
import { ApplicationError, type RecoveryInput, type RecoveryReceipt } from "@aster/api-contracts";
import { ContextRegistry } from "../context/registry.js";
import { DurableContext } from "../context/store.js";
import { GoalSettings } from "../config/settings.js";
import { recoveryReplay } from "../commands/recovery.js";
import {
  ReactionSnapshot,
  deliveriesOf,
  type ReactionPlanning,
  type ReactionWork,
} from "./state.js";
import type { ReactionPlan, ReactionReply } from "./state.js";
import type { ReactionFailure } from "./policy.js";

type Outcome<A> =
  | { readonly _tag: "Success"; readonly value: A }
  | { readonly _tag: "Failure"; readonly error: ReactionFailure };

const path = "/system-one";
const makeReactionState = Effect.gen(function* () {
  const registry = yield* ContextRegistry;
  const durable = yield* DurableContext;
  const settings = yield* GoalSettings;
  const snapshot = yield* Ref.make<ReactionSnapshot | undefined>(undefined);
  const read = Ref.get(snapshot).pipe(
    Effect.flatMap((state) =>
      state ? Effect.succeed(state) : Effect.die(new Error("Reaction state not restored")),
    ),
  );
  const save = Effect.fn("Reactions.commit")(function* (
    work: readonly ReactionWork[],
    receipts?: readonly RecoveryReceipt[],
  ) {
    const current = registry.get(path)!;
    const state = { work, recoveryReceipts: receipts ?? (yield* read).recoveryReceipts ?? [] };
    yield* registry
      .commit({ ...current, state }, { expectedRevision: current.revision })
      .pipe(Effect.orDie);
    yield* Ref.set(snapshot, state);
  }, Effect.uninterruptible);
  const update = Effect.fnUntraced(function* (id: string, f: (work: ReactionWork) => ReactionWork) {
    yield* save((yield* read).work.map((work) => (work.event.id === id ? f(work) : work)));
  });
  const ingest = Effect.fn("SystemOne.ingest")(function* () {
    const existing = (yield* read).work;
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

  return {
    read,
    ingest,
    restore: Effect.gen(function* () {
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
      yield* Ref.set(
        snapshot,
        Schema.decodeUnknownSync(ReactionSnapshot)(registry.get(path)!.state),
      );
      const recovered = (yield* read).work;
      if (recovered.some((work) => deliveriesOf(work).some((d) => d.status === "sending")))
        yield* save(
          recovered.map((work) =>
            work.status !== "ready" && work.status !== "failed"
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
    }),
    startPlanning: Effect.fn("Reactions.startPlanning")(function* (
      work: Extract<ReactionWork, { status: "pending" | "planning" }>,
    ) {
      // Freeze targets when this source reaches screening. Earlier queued work may
      // have legitimately advanced Goal/Signal revisions since source ingestion.
      const { [path]: _ownState, ...evidence } = registry.reader.snapshot();
      const { [work.event.record.path]: _source, ...otherEvidence } = evidence;
      const next: ReactionPlanning = {
        ...work,
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
      return next;
    }),
    startDelivery: Effect.fn("Reactions.startDelivery")(function* (workId: string, id: string) {
      yield* update(workId, (work) => {
        if (work.status !== "ready" && work.status !== "failed") return work;
        return {
          ...work,
          deliveries: work.deliveries.map((delivery) =>
            delivery.command.input.requestId === id
              ? { ...delivery, status: "sending", attempts: delivery.attempts + 1 }
              : delivery,
          ),
        };
      });
    }),
    planned: Effect.fn("Reactions.planned")(function* (
      requestId: string,
      result: Outcome<ReactionPlan>,
    ) {
      yield* update(requestId, (work) =>
        work.status !== "planning"
          ? work
          : Match.value(result).pipe(
              Match.tag("Failure", ({ error }): ReactionWork => ({
                event: work.event,
                attempts: work.attempts,
                input: work.input,
                status: "failed",
                error: error.message,
                failedTargets: work.input.targets ?? [],
                screenings: work.retained?.screenings ?? [],
                deliveries: work.retained?.deliveries ?? [],
              })),
              Match.tag("Success", ({ value }): ReactionWork => {
                const decisions = {
                  event: work.event,
                  attempts: work.attempts,
                  screenings: [...(work.retained?.screenings ?? []), ...value.screenings],
                  deliveries: [
                    ...(work.retained?.deliveries ?? []),
                    ...value.commands.map((command) => ({
                      command,
                      status: "pending" as const,
                      attempts: 0,
                    })),
                  ],
                };
                if (value.failures.length)
                  return {
                    ...decisions,
                    status: "failed",
                    input: work.input,
                    failedTargets: value.failures.map((failure) => failure.target),
                    error: value.failures
                      .map((failure) => `${failure.target}: ${failure.error}`)
                      .join("; "),
                  };
                return {
                  ...decisions,
                  status: decisions.deliveries.every(
                    (item) => item.status === "delivered" || item.status === "rejected",
                  )
                    ? "completed"
                    : "ready",
                };
              }),
              Match.exhaustive,
            ),
      );
    }),
    delivered: Effect.fn("Reactions.delivered")(function* (
      requestId: string,
      deliveryId: string,
      result: Outcome<ReactionReply>,
    ) {
      yield* update(requestId, (work) => {
        if (work.status !== "ready" && work.status !== "failed") return work;
        const deliveries = deliveriesOf(work).map((item) => {
          if (item.command.input.requestId !== deliveryId) return item;
          return Match.value(result).pipe(
            Match.tag("Failure", ({ error }) => ({
              ...item,
              status: "unknown" as const,
              error: error.message,
            })),
            Match.tag("Success", ({ value }) =>
              Match.value(value).pipe(
                Match.tag("Accepted", ({ receipt }) =>
                  receipt.requestId === deliveryId
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
        if (work.status === "failed") return { ...work, deliveries };
        return {
          ...work,
          deliveries,
          status: deliveries.every((d) => d.status === "delivered" || d.status === "rejected")
            ? "completed"
            : "ready",
        };
      });
    }),
    recover: Effect.fn("Reactions.recover")(function* (input: RecoveryInput) {
      const replay = yield* recoveryReplay(
        input,
        registry.get(path)!.revision ?? 0,
        (yield* read).recoveryReceipts ?? [],
      );
      if (Option.isSome(replay)) return replay.value;
      const work = (yield* read).work.find((item) => item.event.id === input.workId);
      if (!work)
        return yield* new ApplicationError({
          kind: "not-found",
          message: "Screening work not found",
        });
      let next: ReactionWork;
      if (input._tag === "RetryScreening") {
        if (
          work.status !== "failed" ||
          deliveriesOf(work).some((entry) => entry.status === "sending")
        )
          return yield* new ApplicationError({
            kind: "conflict",
            message: "Only failed screening can be retried",
          });
        next = {
          event: work.event,
          attempts: work.attempts,
          status: "planning",
          input: {
            ...work.input,
            targets: work.failedTargets.length ? work.failedTargets : undefined,
          },
          retained: { screenings: work.screenings, deliveries: work.deliveries },
        };
      } else {
        const delivery = deliveriesOf(work).find(
          (item) => item.command.input.requestId === input.deliveryId,
        );
        if (
          (work.status !== "ready" && work.status !== "failed") ||
          !delivery ||
          delivery.status !== "unknown"
        )
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
        (yield* read).work.map((item) => (item.event.id === work.event.id ? next : item)),
        [...((yield* read).recoveryReceipts ?? []), { input, receipt }],
      );
      return receipt;
    }),
  };
});

/** Actor-local state transitions commit before the private Ref changes. */
export class ReactionState extends Context.Service<
  ReactionState,
  Effect.Success<typeof makeReactionState>
>()("reactions/State") {
  static readonly layer = Layer.effect(ReactionState, makeReactionState);
}
