import { useMemo, useState } from "react";
import { useAtomRefresh, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Cause, Exit, Match, Schema } from "effect";
import { Atom } from "effect/reactivity";
import {
  ApplicationError,
  QueryKeys,
  contextQueryKeys,
  type ProcessingOwner,
  type ProcessingSnapshot,
  type RecoveryInput,
} from "@aster/api-contracts";
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
      <button className="outline-action" disabled={busy} onClick={() => void submit()}>
        {inputs[key]
          ? "Check recovery receipt"
          : entry.kind === "screening"
            ? "Retry screening"
            : "Retry delivery"}
      </button>
      {error && (
        <p className="goals-error" role="alert">
          {error}
        </p>
      )}
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
    <section aria-label="Processing recovery" aria-busy={result.waiting}>
      <h2>Screening and delivery</h2>
      <p className="quiet-message">
        Recovery reuses saved evidence and commands. It does not replace an existing decision.
      </p>
      {error && (
        <p className="goals-error" role="alert">
          {error}
          <button onClick={refresh}>Refresh processing</button>
        </p>
      )}
      {!snapshot && !error && <p role="status">Loading processing…</p>}
      {snapshot?.entries.length === 0 && <p>No processing records yet.</p>}
      {snapshot?.entries.toReversed().map((entry) => (
        <article className="evaluation-card" key={`${entry.kind}:${entry.id}`}>
          <header className="evaluation-header">
            <strong>
              {entry.kind === "screening" ? "Screening" : "Delivery"} · {entry.status}
            </strong>
            {entry.attempts !== undefined && <span>{entry.attempts} attempts</span>}
          </header>
          <p>
            <button className="text-link" onClick={() => navigate(entry.source)}>
              {entry.source}
            </button>{" "}
            →{" "}
            <button className="text-link" onClick={() => navigate(entry.target)}>
              {entry.target}
            </button>
          </p>
          {entry.error && <p className="goals-error">{entry.error}</p>}
          {entry.matches && entry.matches.length > 0 && (
            <ul aria-label="Target matching results">
              {entry.matches.map((match) => (
                <li key={match.target}>
                  <button className="text-link" onClick={() => navigate(match.target)}>
                    {match.target}
                  </button>{" "}
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
            <p>{entry.id}</p>
          </details>
        </article>
      ))}
    </section>
  );
}
