import { Button } from "../components/ui/button";
import { ErrorNotice } from "../components/feedback";
import { useState } from "react";
import { useAtomValue, useAtomSet } from "@effect/atom-react";
import { Cause, Exit, Schema } from "effect";
import { ApplicationError } from "@aster/core/contracts";
import { contextQueryKeys } from "@aster/api";
import { pendingTurnRetries } from "../api/timeline";
import { retryGoalTurn } from "../api/client";
export function RetryTurn({ slug, turnId }: { slug: string; turnId: string }) {
  const pending = useAtomValue(pendingTurnRetries);
  const setPending = useAtomSet(pendingTurnRetries);
  const retry = useAtomSet(retryGoalTurn, { mode: "promiseExit" });
  const busy = useAtomValue(retryGoalTurn).waiting;
  const [error, setError] = useState("");
  const key = `${slug}:${turnId}`;
  async function submit() {
    if (busy) return;
    const input = pending[key] ?? { slug, turnId, requestId: crypto.randomUUID() };
    setPending((previous) => ({ ...previous, [key]: input }));
    setError("");
    const result = await retry({
      payload: input,
      reactivityKeys: contextQueryKeys(`/goals/${slug}`),
    });
    if (Exit.isFailure(result)) {
      const failure = Cause.squash(result.cause);
      setError(failure instanceof Error ? failure.message : String(failure));
      if (!Schema.is(ApplicationError)(failure) || failure.kind === "unavailable") return;
    }
    setPending((previous) => {
      const next = { ...previous };
      delete next[key];
      return next;
    });
  }
  return (
    <div>
      <Button variant="outline" disabled={busy} onClick={() => void submit()}>
        {busy ? "Retrying…" : "Retry turn"}
      </Button>
      <ErrorNotice error={error} />
    </div>
  );
}
