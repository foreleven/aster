import { useState } from "react";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import { Cause, Exit, Schema } from "effect";
import {
  ApplicationError,
  contextQueryKeys,
  type ResumeTaskDeliveryInput,
} from "@aster/api-contracts";
import { resumeTask } from "../api/client";
const pendingResumes = Atom.make<Record<string, ResumeTaskDeliveryInput>>({}).pipe(Atom.keepAlive);
import type { ContextView } from "../dashboard/model";

export function TaskControls({
  context,
  navigate,
}: {
  context: ContextView;
  navigate: (path: string) => void;
}) {
  const pending = useAtomValue(pendingResumes);
  const setPending = useAtomSet(pendingResumes);
  const resume = useAtomSet(resumeTask, { mode: "promiseExit" });
  const busy = useAtomValue(resumeTask).waiting;
  const [error, setError] = useState("");
  async function submit() {
    if (busy || context.revision === undefined) return;
    const input = pending[context.path] ?? {
      requestId: crypto.randomUUID(),
      target: context.path,
      expectedRevision: context.revision,
    };
    setPending((previous) => ({ ...previous, [context.path]: input }));
    setError("");
    const result = await resume({ payload: input, reactivityKeys: contextQueryKeys(context.path) });
    if (Exit.isFailure(result)) {
      const failure = Cause.squash(result.cause);
      setError(failure instanceof Error ? failure.message : String(failure));
      if (!Schema.is(ApplicationError)(failure) || failure.kind === "unavailable") return;
    }
    setPending((previous) =>
      Object.fromEntries(Object.entries(previous).filter(([key]) => key !== context.path)),
    );
  }
  const publication = context.state.writeback;
  return (
    <section className="context-summary" aria-label="Task controls">
      {(["failed", "uncertain"].includes(context.state.status ?? "") || pending[context.path]) && (
        <>
          <button
            className="outline-action"
            disabled={context.revision === undefined || busy}
            onClick={() => void submit()}
          >
            {pending[context.path] ||
            context.state.resumptions?.some((item) => item.status === "pending")
              ? "Reconcile resumption"
              : "Resume execution"}
          </button>
          <p>Checks the original execution before attempting to continue it.</p>
        </>
      )}
      {error && <p role="alert">{error}</p>}
      {context.state.resumptions?.map((item) => (
        <p key={item.input.requestId}>
          Resumption {item.status}
          {"error" in item && item.error ? `: ${item.error}` : ""}
        </p>
      ))}
      {publication && (
        <article className="context-work-card" aria-label="External publication">
          <h2>External publication</h2>
          <p>Status: {publication.status}</p>
          <button
            className="outline-action"
            onClick={() => navigate(publication.request.action.channelPath)}
          >
            {publication.request.action.channelPath}
          </button>
          <p>Sending as: {publication.request.action.identity}</p>
          <p className="whitespace-pre-wrap">{publication.request.content}</p>
          {publication.status === "waiting-approval" && (
            <>
              <p>
                The local result is complete. Publishing this exact content requires separate
                approval.
              </p>
              <button className="outline-action" onClick={() => navigate("/approvals")}>
                Review publication approval
              </button>
            </>
          )}
          {publication.status === "unknown" && (
            <p>
              Delivery is unconfirmed. Automatic resend is disabled; inspect the destination before
              taking further action.
            </p>
          )}
          {publication.externalId && <p>Receipt: {publication.externalId}</p>}
          {publication.error && <p className="context-failure">{publication.error}</p>}
          <small>Operation: {publication.request.requestId}</small>
        </article>
      )}
    </section>
  );
}
