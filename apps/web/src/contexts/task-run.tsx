import type { PersonalCommands } from "./personal";
import type { ContextView } from "../dashboard/model";
import { runStages } from "../lib/dashboard";

export function TaskRunDetails({
  context,
  navigate,
  personal,
}: {
  context: ContextView;
  navigate: (path: string) => void;
  personal: PersonalCommands;
}) {
  const task = context.state.task;
  const publication = context.state.writeback;
  return (
    <section className="context-summary" aria-label="Task Run">
      <h2>Prepared Task</h2>
      {(["failed", "uncertain"].includes(context.state.status ?? "") ||
        personal.resumptions[context.path]) && (
        <>
          <button
            className="outline-action"
            disabled={!personal.available || personal.busy}
            onClick={() => void personal.resumeRun(context)}
          >
            {personal.resumptions[context.path] ||
            context.state.resumptions?.some((item) => item.status === "pending")
              ? "Reconcile resumption"
              : "Resume execution"}
          </button>
          <p>Checks the original execution before attempting to continue it.</p>
        </>
      )}
      {personal.error && <p role="alert">{personal.error}</p>}
      {context.state.resumptions?.map((item) => (
        <p key={item.input.requestId}>
          Resumption {item.status}
          {"error" in item && item.error ? `: ${item.error}` : ""}
        </p>
      ))}
      {typeof task === "object" ? (
        <>
          <p className="whitespace-pre-wrap">{task.instructions}</p>
          {task.input.map((item, index) => (
            <article className="context-work-card" key={index}>
              <p className="whitespace-pre-wrap">{item.content}</p>
              {item.sources.map((source, sourceIndex) =>
                source.startsWith("/") ? (
                  <button
                    className="outline-action"
                    key={sourceIndex}
                    onClick={() => navigate(source)}
                  >
                    {source}
                  </button>
                ) : (
                  <p key={sourceIndex}>{source}</p>
                ),
              )}
            </article>
          ))}
        </>
      ) : (
        <p>No prepared Task recorded yet.</p>
      )}
      <ol aria-label="Run stages">
        {runStages(context).map((stage) => (
          <li key={stage.title}>
            {stage.title}:{" "}
            {stage.done ? "Completed" : stage.active ? "Current stage" : "Not reached"}
          </li>
        ))}
      </ol>
      {context.state.status === "awaiting-confirmation" && (
        <p>This Task requires your confirmation before execution.</p>
      )}
      {context.state.outcomeText && (
        <>
          <h2>Outcome</h2>
          <p className="whitespace-pre-wrap">{context.state.outcomeText}</p>
        </>
      )}
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
