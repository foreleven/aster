import { Markdown } from "../components/markdown";
import { ProcessingDetails } from "./processing";
import { TaskDetails } from "./task";
import { TaskControls } from "./task-controls";
import React, { useState } from "react";
import { useAtomValue } from "@effect/atom-react";
import { approvalEntries } from "../dashboard/state";
import { ChevronRight, Menu } from "lucide-react";
import type { ContextView } from "../dashboard/model";
import { summaryText } from "../dashboard/model";
import { Messages, Status } from "../dashboard/shared";
import { Approvals } from "../dashboard/approvals";
import { references, isTaskPath } from "../lib/dashboard";
import { contextTitle } from "./navigation";

const linkedPaths = references;

export function ContextWorkspace({
  context,
  contexts,
  navigate,
  showNavigation,
}: {
  context: ContextView;
  contexts: readonly ContextView[];
  navigate: (path: string) => void;
  showNavigation: () => void;
}) {
  const [error, setError] = useState("");
  const approvals = useAtomValue(approvalEntries);
  const summary = summaryText(context.state.summary);
  const restricted = context.projection?.visibility === "restricted";
  const isTask = !restricted && /^\/tasks\/[^/]+$/.test(context.path);
  const forward = new Set(linkedPaths(context));
  const related = contexts.filter(
    (candidate) =>
      candidate.path !== context.path &&
      (forward.has(candidate.path) ||
        linkedPaths(candidate).includes(context.path) ||
        candidate.path.startsWith(`${context.path}/`) ||
        context.path.startsWith(`${candidate.path}/`)),
  );
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
            <p className="context-eyebrow">{context.path.split("/")[1]}</p>
            <h1>{contextTitle(context)}</h1>
            <p>{context.description}</p>
            {context.state.status && <Status value={context.state.status} />}
          </header>
          {restricted && (
            <p role="status" className="quiet-message">
              This Context exposes its path and revision only. Its contents are not available in the
              public view.
            </p>
          )}
          {summary && (
            <section className="context-summary">
              <h2>Current summary</h2>
              <Markdown>{summary}</Markdown>
            </section>
          )}
          {!restricted && isTaskPath(context.path) && (
            <TaskControls context={context} navigate={navigate} />
          )}
          {!restricted &&
            (context.path === "/system-one" ? (
              <ProcessingDetails owner="system-one" navigate={navigate} />
            ) : isTask ? (
              <TaskDetails path={context.path} navigate={navigate} />
            ) : context.path === "/approvals" ? (
              <Approvals inspect={navigate} report={setError} />
            ) : (
              <section className="context-messages" aria-label="Context messages">
                <h2>Messages</h2>
                <Messages messages={context.messages} inspect={navigate} />
              </section>
            ))}
          {error && (
            <p role="alert" className="goals-error">
              {error}
            </p>
          )}
          {!restricted && !isTask && (
            <details className="context-state">
              <summary>View Context state</summary>
              <pre>{JSON.stringify(context.rawState, null, 2)}</pre>
            </details>
          )}
        </div>
      </main>
      <aside className="work-panel context-related" aria-label="Related work" id="context-work">
        <header className="work-panel-heading">
          <h2>Related work</h2>
          <p>Linked contexts, tasks, and deliveries.</p>
        </header>
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
        {context.path !== "/approvals" &&
          approvals.some((entry) => entry.contextPath === context.path) && (
            <Approvals contextPaths={[context.path]} inspect={navigate} report={setError} />
          )}
      </aside>
    </>
  );
}
