import { ProcessingDetails } from "./processing";
import { DelegationDetails } from "./delegation";
import { TaskRunDetails } from "./task-run";
import React, { useState } from "react";
import { useAtomValue } from "@effect/atom-react";
import { approvalEntries } from "../dashboard/state";
import { ChevronRight, Menu, Send } from "lucide-react";
import type { ContextView } from "../dashboard/model";
import { summaryText } from "../dashboard/model";
import { Messages, Status } from "../dashboard/shared";
import { Approvals } from "../dashboard/approvals";
import { references, time, isRunPath } from "../lib/dashboard";
import { contextTitle } from "./navigation";
import type { PersonalCommands } from "./personal";

const linkedPaths = (context: ContextView) => [
  ...references(context),
  ...(context.state.tasks ?? []).flatMap((task) => [
    ...(task.evidence ?? []),
    ...(task.execution ? [task.execution.runPath] : []),
  ]),
];

export function ContextWorkspace({
  context,
  contexts,
  navigate,
  showNavigation,
  personal,
}: {
  context: ContextView;
  contexts: readonly ContextView[];
  navigate: (path: string) => void;
  showNavigation: () => void;
  personal: PersonalCommands;
}) {
  const [error, setError] = useState("");
  const approvals = useAtomValue(approvalEntries);
  const state = context.personalState;
  const restricted = context.projection?.visibility === "restricted";
  const isDelegation = !restricted && /^\/delegations\/[^/]+$/.test(context.path);
  const isPersonal = !restricted && context.path === "/personal";
  const forward = new Set(linkedPaths(context));
  const related = contexts.filter(
    (candidate) =>
      candidate.path !== context.path &&
      (forward.has(candidate.path) ||
        linkedPaths(candidate).includes(context.path) ||
        candidate.path.startsWith(`${context.path}/`) ||
        context.path.startsWith(`${candidate.path}/`)),
  );
  const messages = isPersonal
    ? [...context.messages].sort(
        (a, b) => (a.personal?.sequence ?? 0) - (b.personal?.sequence ?? 0),
      )
    : context.messages;
  return (
    <>
      <main className="goal-main context-main">
        <header className="breadcrumbs">
          <button
            className="icon-button mobile-goals-toggle"
            aria-label="Choose context"
            onClick={showNavigation}
          >
            <Menu size={18} />
          </button>
          <span>Contexts</span>
          <ChevronRight size={14} />
          <span className="breadcrumb-title">{context.path}</span>
          <span className="context-revision">
            {context.revision === undefined
              ? "Revision unavailable"
              : `Revision ${context.revision}`}
          </span>
        </header>
        <div className="goal-body context-body">
          <a className="mobile-work-link" href="#context-work">
            View related work <ChevronRight size={14} />
          </a>
          <header className="context-header">
            <p className="context-eyebrow">
              {isPersonal ? "YOUR WORKSPACE ASSISTANT" : context.path.split("/")[1]}
            </p>
            <h1>{contextTitle(context)}</h1>
            <p>
              {isPersonal
                ? "Discuss priorities, explore your context, and follow work across your goals."
                : context.description}
            </p>
            {context.state.status && <Status value={context.state.status} />}
          </header>
          {restricted && (
            <p role="status" className="quiet-message">
              This Context exposes its path and revision only. Its contents are not available in the
              public view.
            </p>
          )}
          {summaryText(context.state.summary) && (
            <section className="context-summary">
              <h2>Current summary</h2>
              <p>{summaryText(context.state.summary)}</p>
            </section>
          )}
          {!restricted && isRunPath(context.path) && (
            <TaskRunDetails context={context} navigate={navigate} personal={personal} />
          )}
          {!restricted &&
            (context.path === "/system-one" || context.path === "/notifications" ? (
              <ProcessingDetails
                owner={context.path === "/system-one" ? "system-one" : "notifications"}
                navigate={navigate}
              />
            ) : isDelegation ? (
              <DelegationDetails path={context.path} navigate={navigate} />
            ) : context.path === "/approvals" ? (
              <Approvals inspect={navigate} report={setError} />
            ) : (
              <section className="context-messages" aria-label="Context messages">
                <h2>{isPersonal ? "Conversation" : "Messages"}</h2>
                <Messages messages={messages} inspect={navigate} />
              </section>
            ))}
          {error && (
            <p role="alert" className="goals-error">
              {error}
            </p>
          )}
          {!restricted && !isPersonal && !isDelegation && (
            <details className="context-state">
              <summary>View Context state</summary>
              <pre>{JSON.stringify(context.rawState, null, 2)}</pre>
            </details>
          )}
        </div>
        {isPersonal && (
          <div className="composer-area">
            {personal.error && (
              <p role="alert" className="goals-error">
                {personal.error}
              </p>
            )}
            {personal.pending && !personal.busy && (
              <p className="quiet-message">
                Submission not confirmed. Retry sends the same message with its original request ID.
              </p>
            )}
            <form
              className="goal-composer"
              onSubmit={(event) => {
                event.preventDefault();
                void personal.submit();
              }}
            >
              <textarea
                aria-label="Message Personal Agent"
                placeholder="What would you like to work on?"
                rows={2}
                value={personal.text}
                disabled={!personal.available || personal.busy || !!personal.pending}
                onChange={(event) => personal.setText(event.target.value)}
              />
              <button
                className="send-button"
                aria-label={personal.pending ? "Retry submission" : "Send message"}
                disabled={!personal.available || personal.busy || !personal.text.trim()}
              >
                <Send size={18} />
              </button>
            </form>
            <p className="composer-hint">
              {state?.pendingRequestIds.length
                ? `${state.pendingRequestIds.length} input(s) awaiting completion`
                : "Messages and results are saved in this Context."}
            </p>
          </div>
        )}
      </main>
      <aside className="work-panel context-related" aria-label="Related work" id="context-work">
        <header className="work-panel-heading">
          <h2>Related work</h2>
          <p>Linked contexts, runs, and deliveries.</p>
        </header>
        {isPersonal && (
          <section className="context-work-section">
            <h3>Agent runs</h3>
            {!state?.runs?.length && <p className="quiet-message">No runs recorded yet.</p>}
            {state?.runs?.map((run, index, runs) => (
              <article
                className="context-work-card"
                key={`${run.requestId}:${run.executionId ?? index}`}
              >
                <div>
                  <strong>Input {run.inputSequence}</strong>
                  <Status value={run.status} />
                </div>
                <p>{time(run.startedAt)}</p>
                {run.error && <p className="context-failure">{run.error}</p>}
                {run.status === "failed" &&
                  state.pendingRequestIds[0] === run.requestId &&
                  !runs
                    .slice(index + 1)
                    .some((next) => next.inputSequence === run.inputSequence) && (
                    <button
                      className="outline-action"
                      disabled={!personal.available || personal.busy}
                      onClick={() => void personal.retryRun(run.requestId)}
                    >
                      Retry input {run.inputSequence}
                    </button>
                  )}
              </article>
            ))}
          </section>
        )}
        {isPersonal && (
          <section className="context-work-section">
            <h3>Deliveries</h3>
            {!state?.outbox?.length && <p className="quiet-message">No deliveries yet.</p>}
            {state?.outbox?.map((item) => (
              <article className="context-work-card" key={item.input.requestId}>
                <div>
                  <strong>{item.input.target.split("/").at(-1)}</strong>
                  <span className={`delivery-status delivery-${item.status}`}>{item.status}</span>
                </div>
                <p>{"text" in item.input ? item.input.text : deliveryText(item.input)}</p>
                {item.input.operation === "requestApproval" && (
                  <>
                    <small>
                      For {item.input.contextPath} · Source revision {item.input.contextRevision}
                    </small>
                    {item.receipt && (
                      <button className="outline-action" onClick={() => navigate("/approvals")}>
                        View requested approval
                      </button>
                    )}
                  </>
                )}
                {item.input.operation === "startTask" && (
                  <small>One-time Task · Confirmation required before execution</small>
                )}
                {item.input.operation === "startTask" && item.receipt && (
                  <button className="outline-action" onClick={() => navigate(item.input.target)}>
                    View Task Run
                  </button>
                )}
                {"definition" in item.input && (
                  <small>
                    {item.input.active ? "Active" : "Paused"} ·{" "}
                    {item.input.definition.schedule?.type ?? "On source changes"} · Confirmation
                    required for each Run
                  </small>
                )}
                {item.attempts !== undefined && <small>Delivery attempts: {item.attempts}</small>}
                {item.error && <p className="context-failure">{item.error}</p>}
                {item.receipt && <small>Accepted at target revision {item.receipt.revision}</small>}
                {item.status === "unknown" && (
                  <button
                    className="outline-action"
                    disabled={!personal.available || personal.busy}
                    onClick={() => void personal.reconcile(item)}
                  >
                    Reconcile delivery
                  </button>
                )}
              </article>
            ))}
          </section>
        )}
        <section className="context-work-section">
          <h3>
            Contexts <span>({related.length})</span>
          </h3>
          <div className="related-contexts">
            {related.map((item) => (
              <button key={item.path} onClick={() => navigate(item.path)}>
                <span>
                  <strong>{contextTitle(item)}</strong>
                  <small>{item.path}</small>
                </span>
                <ChevronRight size={16} />
              </button>
            ))}
          </div>
          {!related.length && <p className="quiet-message">No linked Contexts yet.</p>}
        </section>
        {!isPersonal &&
          context.path !== "/approvals" &&
          approvals.some((entry) => entry.contextPath === context.path) && (
            <Approvals contextPaths={[context.path]} inspect={navigate} report={setError} />
          )}
      </aside>
    </>
  );
}

function deliveryText(
  input: Exclude<import("@aster/api-contracts").PersonalOutboxItem["input"], { text: string }>,
) {
  if (input.operation === "resumeRun") return `Resume Run: ${input.target}`;
  if (input.operation === "startTask") return `Task: ${input.task.instructions}`;
  if (input.operation === "requestApproval") return `Request approval: ${input.approvalId}`;
  if (input.operation === "respondApproval")
    return `Approval ${input.approvalId}: ${input.response.decision ?? "Input supplied"}`;
  return `${input.operation === "createSignal" ? "Create" : "Update"} Signal: ${input.definition.task}`;
}
