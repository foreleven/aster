import type { BusinessNotification } from "@aster/api-contracts";
import { Match } from "effect";
import type { RunState } from "../tasks/run-state.js";
import { businessNotification } from "./event.js";

/** Only business outcomes and explicit attention states create notifications. */
export const runNotifications = (options: {
  path: string;
  revision: number;
  at: string;
  previous: RunState;
  next: RunState;
}): readonly BusinessNotification[] => {
  const { previous, next, path, revision, at } = options;
  const existing = previous.businessOutbox ?? [];
  if (previous.status === next.status && previous.outcomeText === next.outcomeText) return existing;
  const kind = Match.value(next.status).pipe(
    Match.whenOr("awaiting-confirmation", "waiting_input", () => "NeedsAttention" as const),
    Match.whenOr(
      "completed",
      "failed",
      "cancelled",
      "blocked",
      "rejected",
      "preparation-failed",
      "uncertain",
      () => "RunResult" as const,
    ),
    Match.orElse(() => undefined),
  );
  if (!kind) return existing;
  const parent = next.resumptions?.at(-1)?.input ?? next.admission?.input;
  const text =
    next.outcomeText ??
    Match.value(next.status).pipe(
      Match.when("awaiting-confirmation", () => "Execution is waiting for your confirmation."),
      Match.when(
        "waiting_input",
        () => "Execution needs your input. Read the pending request before responding.",
      ),
      Match.orElse((status) => `Execution ${status}.`),
    );
  return [
    ...existing,
    businessNotification({
      causationId: parent?.requestId,
      source: path,
      revision,
      at,
      causal: parent?.causal ?? next.causal,
      kind,
      text,
    }),
  ];
};
