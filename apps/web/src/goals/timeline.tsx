import { useMemo, useState } from "react";
import { useAtomRefresh, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Cause, Exit, Match, Schema } from "effect";
import {
  ApplicationError,
  contextQueryKeys,
  type GoalInput,
  type GoalTimelineGroup,
} from "@aster/api-contracts";
import { goalTimeline, pendingSignalRetries, pendingTurnRetries } from "../api/timeline";
import { resultError, resultValue, retryGoalSignal, retryGoalTurn } from "../api/client";
import { clockLabel, dateLabel, EmptyState } from "./presentation";
import { Markdown } from "../components/markdown";

type Inspect = (path: string) => void;
const labels: Record<GoalTimelineGroup["status"], string> = {
  pending: "Queued",
  running: "Working",
  failed: "Failed",
  reconciliation_required: "Needs reconciliation",
  completed: "Applied",
  partially_applied: "Delivery pending",
};
const dispositionLabels = { advance: "Progress", no_change: "No change", ignored: "Not relevant" };
const matchesInput = (input: GoalInput, filter: string) =>
  filter === "all" ||
  (filter === "notes" && input.payload._tag === "UserInput") ||
  (filter === "signals" && input.payload._tag === "SignalOccurrence") ||
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
        Match.tag("PersonalMessage", ({ text, source }) => (
          <>
            <strong>Personal Agent</strong>
            <p>{text}</p>
            {link(source)}
          </>
        )),
        Match.tag("SignalOccurrence", ({ signalPath, evidence }) => (
          <>
            <strong>Signal occurrence</strong>
            <p>{evidence}</p>
            {link(signalPath)}
          </>
        )),
        Match.tag("ExecutionFeedback", ({ runPath, status, text, evaluationId }) => (
          <>
            <strong>Execution feedback · {status}</strong>
            <p>{text}</p>
            {link(runPath, "View execution")}
            {evaluationId && <p className="quiet-message">From evaluation {evaluationId}</p>}
          </>
        )),
        Match.tag("GoalStarted", () => (
          <>
            <strong>Goal started</strong>
            <p>Begin pursuing the Goal.</p>
          </>
        )),
        Match.tag("Continuation", ({ objective }) => (
          <>
            <strong>Continue research</strong>
            <p>{objective}</p>
          </>
        )),
        Match.tag("Startup", ({ reason }) => (
          <>
            <strong>Goal check</strong>
            <p>{reason}</p>
          </>
        )),
        Match.exhaustive,
      )}
      <time dateTime={input.receivedAt}>{clockLabel(input.receivedAt)}</time>
    </article>
  );
}

function RetrySignal({
  slug,
  operationId,
  attempts,
}: {
  slug: string;
  operationId: string;
  attempts: number;
}) {
  const pending = useAtomValue(pendingSignalRetries);
  const setPending = useAtomSet(pendingSignalRetries);
  const retry = useAtomSet(retryGoalSignal, { mode: "promiseExit" });
  const busy = useAtomValue(retryGoalSignal).waiting;
  const [error, setError] = useState("");
  const key = `${slug}:${operationId}`;
  async function submit() {
    if (busy) return;
    const input = pending[key] ?? {
      slug,
      operationId,
      expectedAttempts: attempts,
      requestId: crypto.randomUUID(),
    };
    setPending((previous) => ({ ...previous, [key]: input }));
    setError("");
    const result = await retry({
      payload: input,
      reactivityKeys: contextQueryKeys(`/goals/${slug}`),
    });
    let settled = Exit.isSuccess(result);
    if (Exit.isFailure(result)) {
      const failure = Cause.squash(result.cause);
      setError(failure instanceof Error ? failure.message : String(failure));
      const typed = Cause.findError(result.cause);
      settled =
        typed._tag === "Success" &&
        Schema.is(ApplicationError)(typed.success) &&
        typed.success.kind !== "unavailable";
    }
    if (settled)
      setPending((previous) => {
        const next = { ...previous };
        delete next[key];
        return next;
      });
  }
  return (
    <div className="signal-retry">
      <button className="outline-action" disabled={busy} onClick={() => void submit()}>
        {pending[key] ? "Check retry receipt" : "Retry delivery"}
      </button>
      <p className="quiet-message">
        Reuses the original Signal command. {attempts} delivery attempts recorded.
      </p>
      {error && (
        <p className="goals-error" role="alert">
          {error}
        </p>
      )}
    </div>
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

function EvaluationCard({
  slug,
  group,
  filter,
  inspect,
}: {
  slug: string;
  group: GoalTimelineGroup;
  filter: string;
  inspect: Inspect;
}) {
  const inputs = group.inputs.filter((input) => matchesInput(input, filter));
  const outputs = group.outputs.filter(
    (output) => filter === "all" || filter === `${output.kind}s`,
  );
  const showConclusion = filter === "all" || filter === "results" || filter === "progress";
  if (!inputs.length && !outputs.length && !(showConclusion && group.conclusion)) return null;
  return (
    <article className="timeline-event evaluation-card" id={`evaluation-${group.evaluationId}`}>
      <header className="evaluation-header">
        <div>
          <strong>Agent turn {group.ordinal}</strong>
          <time dateTime={group.startedAt}>
            {dateLabel(group.startedAt)} · {clockLabel(group.startedAt)}
          </time>
        </div>
        <span
          className={`goal-pill ${group.status === "failed" || group.status === "reconciliation_required" ? "tone-red" : "tone-neutral"}`}
        >
          {labels[group.status]}
        </span>
      </header>
      {group.status === "failed" && <RetryTurn slug={slug} turnId={group.evaluationId} />}
      {group.retryOf && <p className="quiet-message">Retry of {group.retryOf}</p>}
      <div className="evaluation-inputs">
        {inputs.map((input) => (
          <InputCard key={input.inputId} input={input} inspect={inspect} />
        ))}
      </div>
      {showConclusion && group.conclusion && (
        <section className="evaluation-conclusion">
          <strong>
            {group.disposition ? dispositionLabels[group.disposition] : "Conclusion"}
            {!group.conclusion.applied && " · Not applied"}
          </strong>
          <Markdown>{group.conclusion.text}</Markdown>
          {group.conclusion.evidence.length > 0 && (
            <details className="event-context">
              <summary>
                View {group.conclusion.evidence.length} context{" "}
                {group.conclusion.evidence.length === 1 ? "item" : "items"}
              </summary>
              {group.conclusion.evidence.map((path) => (
                <button key={path} className="text-link" onClick={() => inspect(path)}>
                  {path}
                </button>
              ))}
            </details>
          )}
        </section>
      )}
      {group.nextStep && (
        <section className="evaluation-next-step">
          {Match.value(group.nextStep).pipe(
            Match.tag("Continue", ({ objective }) => (
              <p>
                <strong>Next work:</strong> {objective}
              </p>
            )),
            Match.tag("WaitForInput", ({ questions }) => (
              <>
                <strong>Needs your input</strong>
                <ul>
                  {questions.map((question, index) => (
                    <li key={index}>{question}</li>
                  ))}
                </ul>
              </>
            )),
            Match.tag("WaitForEvent", ({ references }) => (
              <p>
                <strong>Waiting for:</strong> {references.join(", ")}
              </p>
            )),
            Match.tag("Complete", ({ evidence }) => (
              <p>
                <strong>Goal criteria satisfied:</strong> {evidence.join(", ")}
              </p>
            )),
            Match.exhaustive,
          )}
        </section>
      )}
      {outputs.map((output) => (
        <section className="timeline-output" key={output.id}>
          <header>
            <strong>
              {output.kind === "task" ? "Task" : "Signal"} · {output.title}
            </strong>
            <span>
              {output.operation} · {output.status}
            </span>
          </header>
          {output.kind === "signal" && (
            <button className="text-link" onClick={() => inspect(output.target)}>
              View signal
            </button>
          )}
          {output.runPath && (
            <>
              <p className="quiet-message">Execution has its own status.</p>
              <button className="text-link" onClick={() => inspect(output.runPath!)}>
                View execution
              </button>
            </>
          )}
          {output.error && <p className="goals-error">{output.error}</p>}
          {output.kind === "signal" &&
            output.status === "unknown" &&
            output.attempts !== undefined && (
              <RetrySignal slug={slug} operationId={output.id} attempts={output.attempts} />
            )}
        </section>
      ))}
      {group.error && filter !== "notes" && <p className="goals-error">{group.error}</p>}
      {filter !== "notes" && (
        <details className="evaluation-run">
          <summary>Agent run details</summary>
          <dl>
            <dt>Session</dt>
            <dd>{group.agentRun.sessionId}</dd>
            <dt>Request</dt>
            <dd>{group.agentRun.requestId}</dd>
          </dl>
        </details>
      )}
    </article>
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
  const setBefore = useAtomSet(atoms.before);
  const retry = useAtomRefresh(atoms.feed);
  const refresh = useAtomRefresh(atoms.latest);
  const page = resultValue(result);
  const error = resultError(result);
  const pending = page?.pendingInputs.filter((input) => matchesInput(input, filter)) ?? [];
  const groups =
    page?.groups
      .toReversed()
      .filter(
        (group) =>
          group.inputs.some((input) => matchesInput(input, filter)) ||
          group.outputs.some((output) => filter === "all" || filter === `${output.kind}s`) ||
          ((filter === "all" || filter === "results" || filter === "progress") && group.conclusion),
      ) ?? [];
  return (
    <div className="goal-timeline" aria-busy={result.waiting}>
      {error && (
        <div role="alert" className="goals-error">
          {error}
          <button
            onClick={() => {
              refresh();
              retry();
            }}
          >
            Retry timeline
          </button>
        </div>
      )}
      {!page && !error && (
        <p role="status" className="quiet-message">
          Loading timeline…
        </p>
      )}
      {pending.length > 0 && (
        <section className="timeline-pending">
          <h3>Awaiting evaluation</h3>
          {pending.map((input) => (
            <InputCard key={input.inputId} input={input} inspect={inspect} />
          ))}
        </section>
      )}
      {page && !groups.length && !pending.length && (
        <EmptyState title={filter === "all" ? "No messages yet" : "No matching events"}>
          New activity appears here automatically.
        </EmptyState>
      )}
      {groups.map((group) => (
        <EvaluationCard
          key={group.evaluationId}
          slug={slug}
          group={group}
          filter={filter}
          inspect={inspect}
        />
      ))}
      {page?.nextBefore && (
        <button
          className="outline-action load-history"
          disabled={result.waiting}
          onClick={() => setBefore(page.nextBefore ?? undefined)}
        >
          {result.waiting ? "Loading…" : "Load earlier records"}
        </button>
      )}
    </div>
  );
}
