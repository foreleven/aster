import { useState } from "react";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import { Cause, Exit, Schema } from "effect";
import {
  ApplicationError,
  contextQueryKeys,
  type TaskRecoveryInput,
  type WritebackOperation,
} from "@aster/api-contracts";
import { checkTask, retryTask } from "../api/client";
import type { ContextView } from "../dashboard/model";
const pendingRequests = Atom.make<
  Record<string, { input: TaskRecoveryInput; action: "check" | "retry" }>
>({}).pipe(Atom.keepAlive);
export function TaskControls({
  context,
}: {
  context: ContextView;
  navigate: (path: string) => void;
}) {
  const pending = useAtomValue(pendingRequests);
  const setPending = useAtomSet(pendingRequests);
  const check = useAtomSet(checkTask, { mode: "promiseExit" });
  const retry = useAtomSet(retryTask, { mode: "promiseExit" });
  const checking = useAtomValue(checkTask).waiting;
  const retrying = useAtomValue(retryTask).waiting;
  const [error, setError] = useState("");
  async function submit(action: "check" | "retry") {
    if (checking || retrying || context.revision === undefined) return;
    const request = pending[context.path] ?? {
      action,
      input: {
        requestId: crypto.randomUUID(),
        target: context.path,
        expectedRevision: context.revision,
      },
    };
    setPending((previous) => ({ ...previous, [context.path]: request }));
    setError("");
    const result = await (request.action === "check" ? check : retry)({
      payload: request.input,
      reactivityKeys: contextQueryKeys(context.path),
    });
    if (Exit.isFailure(result)) {
      const failure = Cause.squash(result.cause);
      setError(failure instanceof Error ? failure.message : String(failure));
      if (!Schema.is(ApplicationError)(failure) || failure.kind === "unavailable") return;
    }
    setPending((previous) =>
      Object.fromEntries(Object.entries(previous).filter(([key]) => key !== context.path)),
    );
  }
  return (
    <section className="context-summary" aria-label="Task controls">
      {(["failed", "uncertain"].includes(context.state.status ?? "") || pending[context.path]) && (
        <>
          <button
            className="outline-action"
            disabled={checking || retrying}
            onClick={() => void submit("check")}
          >
            {pending[context.path] ? "Check request receipt" : "Check original execution"}
          </button>
          {!pending[context.path] && context.state.status === "failed" && (
            <button
              className="outline-action"
              disabled={checking || retrying}
              onClick={() => void submit("retry")}
            >
              Retry failed execution
            </button>
          )}
          <p>
            Checking does not submit the work again. Retry is available only for confirmed failures.
          </p>
        </>
      )}
      {error && <p role="alert">{error}</p>}
    </section>
  );
}
export function PublicationDetails({
  publication,
  navigate,
}: {
  publication: WritebackOperation;
  navigate: (path: string) => void;
}) {
  return (
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
            The local result is complete. Publishing this exact content requires separate approval.
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
  );
}
