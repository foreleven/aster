import { useMemo, useState } from "react";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { Cause, Exit, Match, Schema } from "effect";
import { ApplicationError, contextQueryKeys, type GoalInput } from "@aster/api-contracts";
import { goalTimeline, pendingTurnRetries } from "../api/timeline";
import { resultError, resultValue, retryGoalTurn } from "../api/client";
import { clockLabel, dateLabel, EmptyState } from "./presentation";
import { Markdown } from "../components/markdown";
type Inspect = (path: string) => void;
const matchesInput = (input: GoalInput, filter: string) =>
  filter === "all" ||
  filter === "progress" ||
  (filter === "notes" && input.payload._tag === "UserInput") ||
  (filter === "signals" && input.payload._tag === "TaskMessage") ||
  ((filter === "tasks" || filter === "results") && input.payload._tag === "ExecutionFeedback");
function InputCard({ input, inspect }: { input: GoalInput; inspect: Inspect }) {
  const link = (path: string, label = path) => (
    <button className="text-link" onClick={() => inspect(path)}>
      {label}
    </button>
  );
  return (
    <article className="timeline-input" data-input-id={input.inputId}>
      {Match.value(input.payload).pipe(
        Match.tag("GoalIntent", ({ intent }) => (
          <>
            <header>
              <strong>Context update · {intent.source.name}</strong>
              <span className="goal-pill tone-amber">
                Score {(intent.relevance.score * 100).toFixed(0)}%
              </span>
            </header>
            <p>{intent.content.summary}</p>
            <p className="input-rationale">
              <strong>Why it matters</strong> {intent.relevance.rationale}
            </p>
            {link(intent.source.contextPath, "View source")}
            <details>
              <summary>Screening details</summary>
              <p>{intent.source.actorPath}</p>
              <p>
                Revision {intent.content.summaryRevision} · Screening{" "}
                {intent.relevance.screeningRecordId}
              </p>
            </details>
          </>
        )),
        Match.tag("UserInput", ({ text }) => (
          <>
            <strong>Your note</strong>
            <p>{text}</p>
          </>
        )),
        Match.tag("TaskMessage", ({ text, source }) => (
          <>
            <strong>Task message</strong>
            <p>{text}</p>
            {link(source)}
          </>
        )),
        Match.tag("ExecutionFeedback", ({ runPath, status, text }) => (
          <>
            <strong>Execution feedback · {status}</strong>
            <p>{text}</p>
            {link(runPath, "View execution")}
          </>
        )),
        Match.tag("GoalStarted", () => (
          <>
            <strong>Goal started</strong>
            <p>Begin pursuing the Goal.</p>
          </>
        )),
        Match.exhaustive,
      )}
      <time dateTime={input.receivedAt}>{clockLabel(input.receivedAt)}</time>
    </article>
  );
}

function RetryTurn({ slug, turnId }: { slug: string; turnId: string }) {
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
      <button className="outline-action" disabled={busy} onClick={() => void submit()}>
        {busy ? "Retrying…" : "Retry turn"}
      </button>
      {error && <p className="goals-error">{error}</p>}
    </div>
  );
}

export function Timeline({
  slug,
  filter,
  inspect,
}: {
  slug: string;
  filter: string;
  inspect: Inspect;
}) {
  const atoms = useMemo(() => goalTimeline(slug), [slug]);
  const result = useAtomValue(atoms.feed);
  const more = useAtomSet(atoms.before);
  const page = resultValue(result);
  const error = resultError(result);
  if (error) return <p className="goals-error">{error}</p>;
  if (!page) return <p className="quiet-message">Loading conversation…</p>;
  return (
    <div className="timeline-list goal-timeline">
      {page.nextBefore !== null && (
        <button className="outline-action" onClick={() => more(page.nextBefore!)}>
          Load earlier messages
        </button>
      )}
      {!page.groups.length && (
        <EmptyState title="No conversation yet">
          User input and feedback will appear here.
        </EmptyState>
      )}
      {page.groups
        .filter((group) => matchesInput(group.input, filter))
        .map((group) => (
          <article
            key={group.requestId}
            id={`input-${group.requestId}`}
            className="timeline-event conversation-entry"
          >
            <header>
              <strong>{group.status}</strong>
              <time>
                {dateLabel(group.input.receivedAt)} · {clockLabel(group.input.receivedAt)}
              </time>
            </header>
            <InputCard input={group.input} inspect={inspect} />
            {group.response && (
              <section className="conversation-response">
                <Markdown>{group.response}</Markdown>
              </section>
            )}
            {group.error && <p className="goals-error">{group.error}</p>}
            {group.status === "failed" &&
              !page.groups.some((item) => item.retryOf === group.requestId) && (
                <RetryTurn slug={slug} turnId={group.requestId} />
              )}
          </article>
        ))}
    </div>
  );
}
