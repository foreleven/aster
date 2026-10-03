import {
  BusinessNotification,
  ApplicationError,
  PersonalMessage,
  PersonalState,
  type PersonalReceipt,
} from "@aster/api-contracts";
import { Effect, Schema } from "effect";
import { isDeepStrictEqual } from "node:util";
import type { ContextRegistry } from "../context/registry.js";
import { NotificationSource } from "./event.js";

export const BusinessOutbox = Schema.Struct({
  businessOutbox: Schema.Array(BusinessNotification),
}).check(
  Schema.makeFilter(
    ({ businessOutbox }) =>
      new Set(businessOutbox.map((input) => input.requestId)).size === businessOutbox.length &&
      businessOutbox.every(
        (input) =>
          Schema.is(NotificationSource)(input.source) &&
          Number.isFinite(Date.parse(input.createdAt)),
      ),
    { expected: "Unique business notifications with valid source paths and timestamps" },
  ),
);

/** A notification must be the exact business event already committed by its source owner. */
export const acceptBusinessNotification = Effect.fn("Personal.acceptNotification")(function* (
  registry: ContextRegistry["Service"],
  raw: BusinessNotification,
): Effect.fn.Return<PersonalReceipt, ApplicationError> {
  const input = yield* Schema.decodeUnknownEffect(BusinessNotification)(raw).pipe(
    Effect.mapError(
      () =>
        new ApplicationError({ kind: "invalid-input", message: "Invalid business notification" }),
    ),
  );
  const current = registry.get("/personal")!;
  const state = Schema.decodeUnknownSync(PersonalState)(current.state);
  const messages = Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(current.messages);
  const previous = messages.find(
    (message) => message.requestId === input.requestId && message.target === "/personal",
  );
  if (previous) {
    if (
      previous.payload._tag !== "ProgressEvent" ||
      !isDeepStrictEqual(previous.payload.notification, input)
    )
      return yield* new ApplicationError({
        kind: "conflict",
        message: "Notification ID belongs to another input",
      });
    return { requestId: input.requestId, revision: previous.revision, sequence: previous.sequence };
  }
  const source = registry.get(input.source);
  const decoded = Schema.decodeUnknownResult(BusinessOutbox)(source?.state);
  const event =
    decoded._tag === "Success"
      ? decoded.success.businessOutbox.find((event) => event.requestId === input.requestId)
      : undefined;
  if (
    !Schema.is(NotificationSource)(input.source) ||
    !source ||
    input.revision > (source.revision ?? 0) ||
    !event ||
    !isDeepStrictEqual(event, input) ||
    !Number.isFinite(Date.parse(input.createdAt))
  )
    return yield* new ApplicationError({
      kind: "invalid-input",
      message: "Notification is not a committed source business event",
    });
  // A per-root budget also bounds branching: multiple Tasks cannot multiply one
  // accepted objective into an unlimited stream of automatic Agent invocations.
  const admitted = messages.filter(
    (message) =>
      message.payload._tag === "ProgressEvent" &&
      message.payload.processing === "queued" &&
      message.causal?.rootRequestId === input.causal.rootRequestId,
  ).length;
  const automatic = input.causal.remainingAgentTurns > 0 && admitted < 8;
  const message: PersonalMessage = {
    requestId: input.requestId,
    causationId: input.causationId,
    source: input.source,
    target: "/personal",
    revision: (current.revision ?? 0) + 1,
    sequence: messages.length + 1,
    createdAt: input.createdAt,
    causal: input.causal,
    payload: {
      _tag: "ProgressEvent",
      text: input.text,
      notification: input,
      processing: automatic ? "queued" : "display-only",
    },
  };
  yield* registry
    .commit(
      {
        ...current,
        state: {
          ...state,
          pendingRequestIds: automatic
            ? [...state.pendingRequestIds, input.requestId]
            : state.pendingRequestIds,
        },
        messages: [...messages, message],
      },
      { expectedRevision: current.revision ?? 0 },
    )
    .pipe(Effect.orDie);
  return { requestId: input.requestId, revision: message.revision, sequence: message.sequence };
});
