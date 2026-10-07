import { Button } from "../components/ui/button";
import { ErrorNotice } from "../components/feedback";
import { useMemo, useState } from "react";
import { useAtomRefresh, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Cause, Exit, Match, Schema } from "effect";
import { Atom } from "effect/reactivity";
import { ApplicationError, type RecoveryInput } from "@aster/core/contracts";
import {
  QueryKeys,
  contextQueryKeys,
  type ProcessingOwner,
  type ProcessingSnapshot,
} from "@aster/api";
import { ApplicationClient, resultError, resultValue } from "../api/client";

const queries = Atom.family((owner: ProcessingOwner) =>
  ApplicationClient.query(
    "InspectProcessing",
    { owner },
    {
      reactivityKeys: [QueryKeys.all, QueryKeys.context(`/${owner}`)],
    },
  ),
);
const recover = ApplicationClient.mutation("RecoverProcessing");
const pending = Atom.make<Record<string, RecoveryInput>>({}).pipe(Atom.keepAlive);
type Entry = ProcessingSnapshot["entries"][number];

function RecoveryAction({
  owner,
  revision,
  entry,
  refresh,
}: {
  owner: ProcessingOwner;
  revision: number;
  entry: Entry;
  refresh: () => void;
}) {
  const inputs = useAtomValue(pending);
  const setInputs = useAtomSet(pending);
  const send = useAtomSet(recover, { mode: "promiseExit" });
  const busy = useAtomValue(recover).waiting;
  const [error, setError] = useState("");
  const key = `${owner}:${entry.kind}:${entry.id}`;
  const eligible =
    entry.kind === "screening" ? entry.status === "failed" : entry.status === "unknown";
  if (!eligible && !inputs[key]) return null;
  async function submit() {
    if (busy) return;
    const identity = { requestId: crypto.randomUUID(), expectedRevision: revision };
    const input =
      inputs[key] ??
      Match.value(entry.kind).pipe(
        Match.when("screening", (): RecoveryInput => ({
          ...identity,
          _tag: "RetryScreening",
          workId: entry.id,
        })),
        Match.when("reaction-delivery", (): RecoveryInput => ({
          ...identity,
          _tag: "RetryReactionDelivery",
          workId: entry.workId!,
          deliveryId: entry.id,
        })),
        Match.exhaustive,
      );
    setInputs((previous) => ({ ...previous, [key]: input }));
    setError("");
    const result = await send({ payload: input, reactivityKeys: contextQueryKeys(`/${owner}`) });
    let settled = Exit.isSuccess(result);
    if (Exit.isFailure(result)) {
      const cause = Cause.squash(result.cause);
      setError(cause instanceof Error ? cause.message : String(cause));
      const failure = Cause.findError(result.cause);
      settled =
        failure._tag === "Success" &&
        Schema.is(ApplicationError)(failure.success) &&
        failure.success.kind !== "unavailable";
      if (settled) refresh();
    }
    if (settled)
      setInputs((previous) => {
        const next = { ...previous };
        delete next[key];
        return next;
      });
  }
  return (
    <div>
      <Button variant="outline" disabled={busy} onClick={() => void submit()}>
        {inputs[key]
          ? "Check recovery receipt"
          : entry.kind === "screening"
            ? "Retry screening"
            : "Retry delivery"}
      </Button>
      <ErrorNotice error={error} />
    </div>
  );
}

export function ProcessingDetails({
  owner,
  navigate,
}: {
  owner: ProcessingOwner;
  navigate: (path: string) => void;
}) {
  const query = useMemo(() => queries(owner), [owner]);
  const result = useAtomValue(query);
  const refresh = useAtomRefresh(query);
  const snapshot = resultValue(result);
  const error = resultError(result);
  return (
    <section
      className="flex min-w-0 flex-col gap-4"
      aria-label="Processing recovery"
      aria-busy={result.waiting}
    >
      <h2>Screening and delivery</h2>
      <p className="text-sm text-muted-foreground">
        Recovery reuses saved evidence and commands. It does not replace an existing decision.
      </p>
      <ErrorNotice error={error} retry={refresh} />
      {!snapshot && !error && <p role="status">Loading processing…</p>}
      {snapshot?.entries.length === 0 && <p>No processing records yet.</p>}
      {snapshot?.entries.toReversed().map((entry) => (
        <article
          className="flex flex-col gap-3 rounded-lg border p-4"
          key={`${entry.kind}:${entry.id}`}
        >
          <header className="flex justify-between gap-3">
            <strong>
              {entry.kind === "screening" ? "Screening" : "Delivery"} · {entry.status}
            </strong>
            {entry.attempts !== undefined && <span>{entry.attempts} attempts</span>}
          </header>
          <p className="flex flex-wrap items-center gap-2">
            <Button
              className="h-auto px-0 whitespace-normal [overflow-wrap:anywhere]"
              variant="link"
              onClick={() => navigate(entry.source)}
            >
              {entry.source}
            </Button>{" "}
            →{" "}
            <Button
              className="h-auto px-0 whitespace-normal [overflow-wrap:anywhere]"
              variant="link"
              onClick={() => navigate(entry.target)}
            >
              {entry.target}
            </Button>
          </p>
          {entry.error && <p className="text-sm text-destructive">{entry.error}</p>}
          {entry.matches && entry.matches.length > 0 && (
            <ul aria-label="Target matching results">
              {entry.matches.map((match) => (
                <li key={match.target}>
                  <Button variant="link" onClick={() => navigate(match.target)}>
                    {match.target}
                  </Button>{" "}
                  · {match._tag}: {match._tag === "Failed" ? match.error : match.reason}
                </li>
              ))}
            </ul>
          )}
          <RecoveryAction
            owner={owner}
            revision={snapshot.revision}
            entry={entry}
            refresh={refresh}
          />
          <details>
            <summary>Record identity</summary>
            <p className="break-all">{entry.id}</p>
          </details>
        </article>
      ))}
    </section>
  );
}
