import { Effect, Schema } from "effect";
import {
  ApplicationError,
  type ProcessingOwner,
  type ProcessingSnapshot,
} from "@aster/api-contracts";
import type { ContextRegistry } from "./registry.js";
import { ReactionState } from "./reaction-state.js";
import { NotificationState } from "../notifications/actor.js";

/** Operator diagnostics deliberately exclude frozen source snapshots, credentials and model frames. */
export const inspectProcessing = Effect.fn("Processing.inspect")(function* (
  registry: ContextRegistry["Service"],
  owner: ProcessingOwner,
): Effect.fn.Return<ProcessingSnapshot, ApplicationError> {
  const current = registry.get(`/${owner}`);
  if (!current)
    return yield* new ApplicationError({
      kind: "not-found",
      message: "Processing owner not found",
    });
  if (owner === "notifications") {
    const state = yield* Schema.decodeUnknownEffect(NotificationState)(current.state).pipe(
      Effect.mapError(
        () =>
          new ApplicationError({ kind: "unavailable", message: "Notification state unavailable" }),
      ),
    );
    return {
      owner,
      revision: current.revision ?? 0,
      entries: state.deliveries.map((item) => ({
        id: item.input.requestId,
        kind: "notification",
        source: item.input.source,
        target: item.input.target,
        status: item.status,
        attempts: item.attempts,
        error: item.error,
      })),
    };
  }
  const state = yield* Schema.decodeUnknownEffect(ReactionState)(current.state).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "unavailable", message: "Reaction state unavailable" }),
    ),
  );
  return {
    owner,
    revision: current.revision ?? 0,
    entries: state.work.flatMap((work): ProcessingSnapshot["entries"] => [
      {
        id: work.event.requestId,
        kind: "screening",
        workId: work.event.requestId,
        source: work.event.source,
        target: "/system-one",
        status: work.status,
        attempts: work.attempts,
        error: work.error,
      },
      ...(work.deliveries ?? []).map((item) => ({
        id: item.command.input.requestId,
        kind: "reaction-delivery" as const,
        workId: work.event.requestId,
        source: work.event.source,
        target: item.command.input.target,
        status: item.status,
        attempts: item.attempts,
        error: item.error,
      })),
    ]),
  };
});
