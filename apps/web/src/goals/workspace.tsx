import React, { useRef, useState } from "react";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { AlertDialog, DropdownMenu } from "radix-ui";
import {
  Archive,
  ChevronRight,
  Clock3,
  Ellipsis,
  ExternalLink,
  Menu,
  Orbit,
  Paperclip,
  Pause,
  Pencil,
  Send,
} from "lucide-react";
import { contextQueryKeys } from "@aster/api-contracts";
import { sendGoalMessage, endGoal } from "../api/client";
import { summaryText, type ContextView } from "../dashboard/model";
import { references } from "../lib/dashboard";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "../components/ui/tabs";
import { Timeline } from "./timeline";
import { WorkPanel } from "./work-panel";
import { dateLabel, EmptyState, lastActivity, Pill, slugFor, titleFor } from "./presentation";

export function GoalWorkspace({
  goal,
  contexts,
  inspect,
  showGoals,
}: {
  goal: ContextView;
  contexts: readonly ContextView[];
  inspect: (path: string) => void;
  showGoals: () => void;
}) {
  const pendingSubmission = useRef<{ text: string; requestId: string } | undefined>(undefined);
  const [text, setText] = useState("");
  const [error, setError] = useState("");
  const [tab, setTab] = useState("timeline");
  const [filter, setFilter] = useState("all");
  const [confirmEnd, setConfirmEnd] = useState(false);
  const send = useAtomSet(sendGoalMessage, { mode: "promise" });
  const end = useAtomSet(endGoal, { mode: "promise" });
  const sending = useAtomValue(sendGoalMessage).waiting;
  const ending = useAtomValue(endGoal).waiting;
  const busy = sending || ending;
  const canWrite = goal.state.status === "active" && !goal.projectionError;
  const slug = slugFor(goal);
  const related = contexts.filter(
    (context) =>
      context.path !== goal.path &&
      !context.state.deleted &&
      (context.state.goal === slug ||
        context.state.definition?.goal === slug ||
        context.state.sourcePath === goal.path),
  );
  const relatedPaths = [
    ...new Set([
      ...references(goal),
      ...related.map((context) => context.path),
      ...(goal.state.tasks ?? []).flatMap((task) => [
        ...(task.evidence ?? []),
        ...(task.execution ? [task.execution.runPath] : []),
      ]),
    ]),
  ];
  const activity = lastActivity(goal);

  // React is the imperative boundary; typed AtomRpc mutations own transport and invalidation.
  async function submit(kind: "message" | "end") {
    if (!canWrite || busy || (kind === "message" && !text.trim())) return;
    setError("");
    try {
      const reactivityKeys = contextQueryKeys(goal.path);
      if (kind === "message") {
        const message = text.trim();
        if (pendingSubmission.current?.text !== message) {
          pendingSubmission.current = { text: message, requestId: crypto.randomUUID() };
        }
        await send({ payload: { slug, ...pendingSubmission.current }, reactivityKeys });
        pendingSubmission.current = undefined;
        setText("");
      } else {
        await end({ payload: { slug }, reactivityKeys });
        setConfirmEnd(false);
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }

  return (
    <>
      <main className="goal-main">
        <header className="breadcrumbs">
          <button
            className="icon-button mobile-goals-toggle"
            aria-label="Choose goal"
            onClick={showGoals}
          >
            <Menu size={18} />
          </button>
          <span>Goals</span>
          <ChevronRight size={14} />
          <span className="breadcrumb-title">{titleFor(goal)}</span>
          <div className="breadcrumb-actions">
            <DropdownMenu.Root>
              <DropdownMenu.Trigger className="icon-button" aria-label="Goal actions">
                <Ellipsis size={17} />
              </DropdownMenu.Trigger>
              <DropdownMenu.Portal>
                <DropdownMenu.Content className="goals-menu" align="end" sideOffset={6}>
                  <DropdownMenu.Item onSelect={() => inspect(goal.path)}>
                    Inspect goal
                  </DropdownMenu.Item>
                  <DropdownMenu.Item
                    disabled={!canWrite || busy}
                    onSelect={() => setConfirmEnd(true)}
                  >
                    End Goal
                  </DropdownMenu.Item>
                </DropdownMenu.Content>
              </DropdownMenu.Portal>
            </DropdownMenu.Root>
            <span title="Direct editing is not available yet. Send an instruction to adjust this goal.">
              <button className="outline-action" disabled>
                <Pencil size={15} />
                Edit goal
              </button>
            </span>
          </div>
        </header>
        <div className="goal-body">
          <a className="mobile-work-link" href="#goal-work">
            View tasks and signals <ChevronRight size={14} />
          </a>
          <section className="goal-header">
            <div className="goal-title-row">
              <Orbit className="goal-title-icon" size={26} />
              <h1>{titleFor(goal)}</h1>
              <Pill status={goal.state.status} />
            </div>
            <div className="goal-header-actions">
              <span title="Pausing goals is not available yet.">
                <button className="outline-action" disabled>
                  <Pause size={14} />
                  Pause
                </button>
              </span>
              <span title="Archiving goals separately from completion is not available yet. Use Goal actions to end this goal.">
                <button className="outline-action" disabled>
                  <Archive size={14} />
                  Archive
                </button>
              </span>
            </div>
            {activity !== undefined && (
              <div className="goal-meta">
                <Clock3 size={14} />
                Last activity {dateLabel(activity)}
              </div>
            )}
            {(summaryText(goal.state.summary) || goal.state.progress) && (
              <p className="goal-description">
                {summaryText(goal.state.summary) || goal.state.progress}
              </p>
            )}
          </section>
          <Tabs value={tab} onValueChange={setTab} className="goal-tabs">
            <TabsList variant="line" className="goal-tab-list" aria-label="Goal sections">
              <TabsTrigger value="timeline">Timeline</TabsTrigger>
              <TabsTrigger value="notes">Notes</TabsTrigger>
              <TabsTrigger value="related">Related</TabsTrigger>
              <TabsTrigger value="details">Details</TabsTrigger>
            </TabsList>
            <TabsContent value="timeline">
              <div className="timeline-heading">
                <h2>Goal Timeline</h2>
                <select
                  aria-label="Filter timeline events"
                  value={filter}
                  onChange={(event) => setFilter(event.target.value)}
                >
                  <option value="all">All events</option>
                  <option value="notes">Your notes</option>
                  <option value="tasks">Tasks / Executions</option>
                  <option value="signals">Signals</option>
                  <option value="results">Results</option>
                  <option value="progress">Progress</option>
                </select>
              </div>
              <Timeline slug={slug} filter={filter} inspect={inspect} />
            </TabsContent>
            <TabsContent value="notes">
              <div className="timeline-heading">
                <h2>Notes</h2>
              </div>
              <Timeline slug={slug} filter="notes" inspect={inspect} />
            </TabsContent>
            <TabsContent value="related">
              <div className="timeline-heading">
                <h2>Related context</h2>
              </div>
              {relatedPaths.length ? (
                <div className="related-contexts">
                  {relatedPaths.map((path) => (
                    <button key={path} onClick={() => inspect(path)}>
                      <span>
                        <strong>
                          {contexts.find((context) => context.path === path)?.description || path}
                        </strong>
                        <small>{path}</small>
                      </span>
                      <ExternalLink size={16} />
                    </button>
                  ))}
                </div>
              ) : (
                <EmptyState title="No related context yet">
                  Evidence and linked records for this goal will appear here.
                </EmptyState>
              )}
            </TabsContent>
            <TabsContent value="details">
              <div className="timeline-heading">
                <h2>Goal details</h2>
              </div>
              <dl className="goal-details">
                <dt>Status</dt>
                <dd>
                  <Pill status={goal.state.status} />
                </dd>
                <dt>Description</dt>
                <dd>{goal.description}</dd>
                <dt>Progress</dt>
                <dd>{goal.state.progress || "No progress recorded yet."}</dd>
                <dt>Completion criteria</dt>
                <dd>
                  {goal.state.completionCriteria || "This goal stays active until you end it."}
                </dd>
              </dl>
              <p className="quiet-message">
                Send an instruction below to add context or adjust priorities. Direct editing,
                pausing, and separate archiving are not available yet.
              </p>
              <button className="text-link" onClick={() => inspect(goal.path)}>
                Inspect stored state <ExternalLink size={14} />
              </button>
            </TabsContent>
          </Tabs>
        </div>
        <div className="composer-area">
          {error && (
            <p className="goals-error" role="alert">
              {error}
            </p>
          )}
          {canWrite ? (
            <form
              className="goal-composer"
              onSubmit={(event) => {
                event.preventDefault();
                void submit("message");
              }}
            >
              <DropdownMenu.Root>
                <DropdownMenu.Trigger
                  className="attach-context"
                  aria-label="Add context reference"
                  disabled={busy}
                >
                  <Paperclip size={18} />
                </DropdownMenu.Trigger>
                <DropdownMenu.Portal>
                  <DropdownMenu.Content
                    className="goals-menu context-menu"
                    align="start"
                    sideOffset={8}
                  >
                    {contexts
                      .filter((context) => context.path !== goal.path && !context.state.deleted)
                      .map((context) => (
                        <DropdownMenu.Item
                          key={context.path}
                          onSelect={() =>
                            setText((draft) => `${draft}${draft ? "\n" : ""}${context.path}`)
                          }
                        >
                          {context.description || context.path}
                        </DropdownMenu.Item>
                      ))}
                    {contexts.length <= 1 && (
                      <DropdownMenu.Item disabled>No other context available</DropdownMenu.Item>
                    )}
                  </DropdownMenu.Content>
                </DropdownMenu.Portal>
              </DropdownMenu.Root>
              <textarea
                aria-label="Add information to Goal"
                value={text}
                maxLength={8000}
                rows={1}
                disabled={busy}
                onChange={(event) => setText(event.target.value)}
                placeholder="Add a note or give Aster an instruction for this goal…"
                onKeyDown={(event) => {
                  if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
                    event.preventDefault();
                    void submit("message");
                  }
                }}
              />
              <button
                className="send-button"
                aria-label="Send"
                type="submit"
                disabled={!text.trim() || busy}
              >
                <Send size={17} />
              </button>
            </form>
          ) : (
            <p className="closed-goal">
              {goal.projectionError
                ? "This goal cannot be edited until its data can be read."
                : "This goal is not active. Its history remains available."}
            </p>
          )}
        </div>
        <AlertDialog.Root open={confirmEnd} onOpenChange={setConfirmEnd}>
          <AlertDialog.Portal>
            <AlertDialog.Overlay className="goal-dialog-overlay" />
            <AlertDialog.Content className="goal-dialog">
              <AlertDialog.Title>End this goal?</AlertDialog.Title>
              <AlertDialog.Description>
                This marks the goal complete and deactivates its generated signals. Work already
                submitted may still finish. The conversation will remain available in the Context
                tree.
              </AlertDialog.Description>
              {error && (
                <p role="alert" className="goals-error">
                  {error}
                </p>
              )}
              <div className="dialog-actions">
                <AlertDialog.Cancel className="outline-action" disabled={busy}>
                  Keep Goal active
                </AlertDialog.Cancel>
                <button
                  className="primary-action"
                  disabled={busy}
                  onClick={() => void submit("end")}
                >
                  {ending ? "Ending…" : "Confirm ending Goal"}
                </button>
              </div>
            </AlertDialog.Content>
          </AlertDialog.Portal>
        </AlertDialog.Root>
      </main>
      <WorkPanel goal={goal} related={related} contexts={contexts} inspect={inspect} />
    </>
  );
}
