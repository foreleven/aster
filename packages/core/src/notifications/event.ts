import { createHash } from "node:crypto";
import { BusinessNotification, RunPath, type CausalChain } from "@aster/api-contracts";
import { Schema } from "effect";

export const NotificationSource = Schema.Union([
  RunPath,
  Schema.String.check(Schema.isPattern(/^\/(goals|signals)\/[a-z0-9][a-z0-9-]*$/)),
]);

/** The owner supplies the committed revision; one stable identity names each business event. */
export const businessNotification = (options: {
  source: string;
  revision: number;
  at: string;
  kind: BusinessNotification["kind"];
  text: string;
  eventId?: string;
  causal?: CausalChain;
  causationId?: string;
}): BusinessNotification => {
  const requestId = createHash("sha256")
    .update(
      JSON.stringify([
        "business-notification-v1",
        options.source,
        options.revision,
        options.kind,
        options.eventId ?? null,
      ]),
    )
    .digest("hex");
  const causal = options.causal ?? { rootRequestId: requestId, remainingAgentTurns: 4 };
  return {
    requestId,
    causationId: options.causationId ?? causal.rootRequestId,
    source: options.source,
    target: "/personal",
    revision: options.revision,
    createdAt: options.at,
    causal,
    kind: options.kind,
    text: options.text,
  };
};
