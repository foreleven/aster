import { useAtomValue } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import { QueryKeys } from "@aster/api-contracts";
import { ApplicationClient, resultError, resultValue } from "../api/client";

const inspection = Atom.family((path: string) =>
  ApplicationClient.query(
    "InspectTask",
    { path },
    { reactivityKeys: [QueryKeys.all, QueryKeys.context(path)] },
  ),
);

export function TaskDetails({
  path,
  navigate,
}: {
  path: string;
  navigate: (path: string) => void;
}) {
  const result = useAtomValue(inspection(path));
  const view = resultValue(result);
  const error = resultError(result);
  if (error) return <p role="alert">{error}</p>;
  if (!view) return <p role="status">Loading execution…</p>;
  return (
    <section className="context-work-section" aria-label="Task execution">
      <h2>Execution</h2>
      <p>
        {view.agent} · {view.status} · Revision {view.revision}
      </p>
      <p>{view.instructions}</p>
      {!view.hasExecution && (
        <p>No execution handle is recorded. The task has not been resubmitted.</p>
      )}
      {view.error && <p className="context-failure">{view.error}</p>}
      {view.result && (
        <section>
          <h3>Result</h3>
          <p className="whitespace-pre-wrap">{view.result}</p>
        </section>
      )}
      {view.resumptions?.map((item) => (
        <p key={item.requestId}>Resumption: {item.status}</p>
      ))}
      {view.requests.map((request) => (
        <article className="context-work-card" key={request.id}>
          <strong>
            {request.kind === "approval" ? "Approval request" : "Additional information"}
          </strong>
          <p>{request.prompt}</p>
          <small>Response: {request.responseStatus}</small>
          <button className="outline-action" onClick={() => navigate("/approvals")}>
            View approvals
          </button>
        </article>
      ))}
      {view.messages.map((message) => (
        <article key={message.id}>
          <small>{message.kind}</small>
          <p className="whitespace-pre-wrap">{message.text}</p>
        </article>
      ))}
      {view.sources.length > 0 && (
        <section>
          <h3>Sources</h3>
          {view.sources.map((source) => (
            <p key={source}>
              {source.startsWith("/") ? (
                <button className="outline-action" onClick={() => navigate(source)}>
                  {source}
                </button>
              ) : (
                source
              )}
            </p>
          ))}
        </section>
      )}
    </section>
  );
}
