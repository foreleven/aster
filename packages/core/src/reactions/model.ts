import { Context, Effect, Layer, Option, Ref, Schema, Match } from "effect";
import { ApplicationError, type RecoveryInput } from "@aster/api-contracts";
import { ContextRegistry } from "../context/registry.js";
import type { ContextEvent } from "../context/model.js";
import { reactionTargets } from "./policy.js";
import { GoalSettings } from "../config/settings.js";
import { recoveryReplay } from "../commands/recovery.js";
import {
  ReactionSnapshot,
  deliveriesOf,
  targetPath,
  workStatus,
  type FrozenReaction,
  type ReactionWork,
  type ReactionDelivery,
  type ReactionPlan,
  type ReactionReply,
} from "./state.js";
import type { ReactionFailure } from "./policy.js";

type Outcome<A> =
  | { readonly _tag: "Success"; readonly value: A }
  | { readonly _tag: "Failure"; readonly error: ReactionFailure };

const mapDeliveries = (
  work: ReactionWork,
  f: (delivery: ReactionDelivery) => ReactionDelivery,
): ReactionWork =>
  work.status === "queued"
    ? work
    : {
        ...work,
        targets: work.targets.map((target) =>
          target.result._tag === "Matched"
            ? { ...target, result: { ...target.result, delivery: f(target.result.delivery) } }
            : target,
        ),
      };

const path = "/system-one";
const makeReactionState = Effect.gen(function* () {
  const registry = yield* ContextRegistry;
  const settings = yield* GoalSettings;
  const snapshot = yield* Ref.make<ReactionSnapshot | undefined>(undefined);
  const read = Ref.get(snapshot).pipe(
    Effect.flatMap((state) =>
      state ? Effect.succeed(state) : Effect.die(new Error("Reaction state not restored")),
    ),
  );
  const save = Effect.fn("Reactions.commit")(function* (next: ReactionSnapshot) {
    const current = registry.get(path)!;
    // Keep unfinished work and bounded diagnostics; watermarks prevent replay of pruned events.
    const completed = next.work.filter((work) => workStatus(work) === "completed").slice(-100);
    const work = next.work.filter(
      (item) => workStatus(item) !== "completed" || completed.includes(item),
    );
    const ids = new Set(work.map((item) => item.event.id));
    const state = {
      ...next,
      work,
      recoveryReceipts: next.recoveryReceipts.filter((entry) => ids.has(entry.input.workId)),
    };
    yield* registry
      .commit({ ...current, state }, { expectedRevision: current.revision })
      .pipe(Effect.orDie);
    yield* Ref.set(snapshot, state);
  }, Effect.uninterruptible);
  const update = Effect.fnUntraced(function* (id: string, f: (work: ReactionWork) => ReactionWork) {
    const current = yield* read;
    yield* save({
      ...current,
      work: current.work.map((work) => (work.event.id === id ? f(work) : work)),
    });
  });
  const ingest = Effect.fn("SystemOne.ingest")(function* (events: readonly ContextEvent[]) {
    const current = yield* read;
    const work = [...current.work];
    const sourceRevisions = { ...current.sourceRevisions };
    let changed = false;
    for (const event of events) {
      const source = event.record.path;
      if (event.record.revision <= (sourceRevisions[source] ?? 0)) continue;
      sourceRevisions[source] = event.record.revision;
      changed = true;
      const next: ReactionWork = { event, status: "queued" };
      // Frozen work never coalesces, including targets waiting for operator recovery.
      const pending = work.findIndex(
        (item) => item.status === "queued" && item.event.record.path === source,
      );
      if (pending < 0) work.push(next);
      else work[pending] = next;
    }
    if (changed) yield* save({ ...current, work, sourceRevisions });
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
              state: { work: [], sourceRevisions: {}, recoveryReceipts: [] },
              messages: [],
            },
            { expectedRevision: 0 },
          )
          .pipe(Effect.orDie);
      yield* Ref.set(
        snapshot,
        Schema.decodeUnknownSync(ReactionSnapshot)(registry.get(path)!.state),
      );
      const current = yield* read;
      if (current.work.some((work) => deliveriesOf(work).some((d) => d.status === "sending")))
        yield* save({
          ...current,
          work: current.work.map((work) =>
            mapDeliveries(work, (d) =>
              d.status === "sending"
                ? { ...d, status: "unknown", error: "Delivery interrupted before acknowledgement" }
                : d,
            ),
          ),
        });
    }),
    startPlanning: Effect.fn("Reactions.startPlanning")(function* (work: ReactionWork) {
      if (work.status === "frozen") return work;
      const frozen: FrozenReaction = {
        event: work.event,
        status: "frozen",
        targets: reactionTargets(registry.reader, settings.definitions).map((input) => ({
          input,
          result: { _tag: "Pending" },
        })),
      };
      yield* update(work.event.id, () => frozen);
      return frozen;
    }),
    startDelivery: Effect.fn("Reactions.startDelivery")(function* (workId: string, id: string) {
      yield* update(workId, (work) =>
        mapDeliveries(work, (delivery) =>
          delivery.command.input.requestId === id
            ? { ...delivery, status: "sending", attempts: delivery.attempts + 1 }
            : delivery,
        ),
      );
    }),
    planned: Effect.fn("Reactions.planned")(function* (id: string, outcome: Outcome<ReactionPlan>) {
      const work = (yield* read).work.find((item) => item.event.id === id);
      if (!work || work.status === "queued") return;
      const pending = work.targets.filter(({ result }) => result._tag === "Pending");
      if (outcome._tag === "Success") {
        const paths = new Set(outcome.value.map((item) => item.target));
        if (
          paths.size !== pending.length ||
          outcome.value.length !== pending.length ||
          pending.some(({ input }) => !paths.has(targetPath(input)))
        )
          return yield* Effect.die(
            new Error("Reaction plan must settle every pending target exactly once"),
          );
      }
      yield* update(id, (current) =>
        current.status === "queued"
          ? current
          : {
              ...current,
              targets: current.targets.map((target) => {
                if (target.result._tag !== "Pending") return target;
                const result =
                  outcome._tag === "Failure"
                    ? { _tag: "Failed" as const, error: outcome.error.message }
                    : outcome.value.find((item) => item.target === targetPath(target.input))!
                        .result;
                return { ...target, result };
              }),
            },
      );
    }),
    delivered: Effect.fn("Reactions.delivered")(function* (
      id: string,
      deliveryId: string,
      result: Outcome<ReactionReply>,
    ) {
      yield* update(id, (work) =>
        mapDeliveries(work, (item) => {
          if (item.command.input.requestId !== deliveryId) return item;
          return Match.value(result).pipe(
            Match.tag("Failure", ({ error }): ReactionDelivery => ({
              ...item,
              status: "unknown",
              error: error.message,
            })),
            Match.tag("Success", ({ value }) =>
              Match.value(value).pipe(
                Match.tag("Accepted", ({ receipt }): ReactionDelivery =>
                  receipt.requestId === deliveryId
                    ? {
                        command: item.command,
                        attempts: item.attempts,
                        status: "delivered",
                        receipt,
                      }
                    : {
                        ...item,
                        status: "unknown",
                        error: "Receiver returned another delivery identity",
                      },
                ),
                Match.tag("Rejected", ({ error }): ReactionDelivery => ({
                  ...item,
                  status: "rejected",
                  error: error.message,
                })),
                Match.exhaustive,
              ),
            ),
            Match.exhaustive,
          );
        }),
      );
    }),
    recover: Effect.fn("Reactions.recover")(function* (input: RecoveryInput) {
      const current = yield* read;
      const replay = yield* recoveryReplay(
        input,
        registry.get(path)!.revision,
        current.recoveryReceipts,
      );
      if (Option.isSome(replay)) return replay.value;
      const work = current.work.find((item) => item.event.id === input.workId);
      if (!work)
        return yield* new ApplicationError({
          kind: "not-found",
          message: "Screening work not found",
        });
      let next: ReactionWork;
      if (input._tag === "RetryScreening") {
        if (work.status === "queued" || workStatus(work) !== "failed")
          return yield* new ApplicationError({
            kind: "conflict",
            message: "Only failed screening can be retried",
          });
        next = {
          ...work,
          targets: work.targets.map((target) =>
            target.result._tag === "Failed" ? { ...target, result: { _tag: "Pending" } } : target,
          ),
        };
      } else {
        const delivery = deliveriesOf(work).find(
          (item) => item.command.input.requestId === input.deliveryId,
        );
        if (!delivery || delivery.status !== "unknown")
          return yield* new ApplicationError({
            kind: "conflict",
            message: "Only unknown delivery can be retried",
          });
        next = mapDeliveries(work, (item) =>
          item === delivery
            ? { command: item.command, attempts: item.attempts, status: "pending" }
            : item,
        );
      }
      const receipt = { requestId: input.requestId, revision: input.expectedRevision + 1 };
      yield* save({
        ...current,
        work: current.work.map((item) => (item.event.id === work.event.id ? next : item)),
        recoveryReceipts: [...current.recoveryReceipts, { input, receipt }],
      });
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
