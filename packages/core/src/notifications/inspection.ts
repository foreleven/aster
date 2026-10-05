import { Effect, Schema } from "effect";
import { ApplicationError, type ProcessingSnapshot } from "@aster/api-contracts";
import type { ContextSnapshot } from "../context/model.js";
import { NotificationState } from "./actor.js";
export const inspectNotifications = Effect.fn("Notifications.inspect")(function* (
  current: ContextSnapshot,
): Effect.fn.Return<ProcessingSnapshot, ApplicationError> {
  const owner = "notifications";
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
      error: "error" in item ? item.error : undefined,
    })),
  };
});
