import type { BusinessNotification } from "@aster/api-contracts";
import type { SignalState } from "../signals/state.js";
import { businessNotification } from "./event.js";

/** Only newly committed occurrences publish; delivery acknowledgement and timers do not. */
export const signalNotifications = (options: {
  path: string;
  revision: number;
  at: string;
  previous: SignalState;
  next: SignalState;
}): readonly BusinessNotification[] => {
  const known = new Set(options.previous.occurrences?.map((occurrence) => occurrence.id));
  return [
    ...(options.previous.businessOutbox ?? []),
    ...(options.next.occurrences ?? [])
      .filter((occurrence) => !known.has(occurrence.id))
      .map((occurrence) =>
        businessNotification({
          source: options.path,
          revision: options.revision,
          at: options.at,
          kind: "SignalMatched",
          eventId: occurrence.id,
          causationId: occurrence.id,
          causal: occurrence.causal ?? options.next.causal,
          text: occurrence.text,
        }),
      ),
  ];
};
