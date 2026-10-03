import React, { useState } from "react";
import { useAtomValue } from "@effect/atom-react";
import { approvalEntries } from "../dashboard/state";
import { Approvals } from "../dashboard/approvals";
import { ChevronDown, Clock3, Link2, Search } from "lucide-react";
import type { ContextView } from "../dashboard/model";
import { references } from "../lib/dashboard";
import { clockLabel, dateLabel, Pill, statusLabel, WorkIcon } from "./presentation";

export function WorkPanel({
  goal,
  related,
  contexts,
  inspect,
}: {
  goal: ContextView;
  related: readonly ContextView[];
  contexts: readonly ContextView[];
  inspect: (path: string) => void;
}) {
  const [error, setError] = useState("");
  const approvals = useAtomValue(approvalEntries);
  const tasks = (goal.state.tasks ?? []).filter((task) => task.status !== "deleted");
  const taskRuns = new Set(
    tasks.flatMap((task) => (task.execution ? [task.execution.runPath] : [])),
  );
  const runs = related.filter(
    (context) => context.path.includes("/runs/") && !taskRuns.has(context.path),
  );
  const signals = related.filter(
    (context) => /^\/signals\/[^/]+$/.test(context.path) && !context.state.deleted,
  );
  const approvalPaths = [...taskRuns, ...runs.map((run) => run.path)];
  const hasApprovals = approvals.some((entry) => approvalPaths.includes(entry.contextPath));
  const nameFor = (path: string) =>
    contexts.find((context) => context.path === path)?.description || path;
  return (
    <aside className="work-panel" aria-label="Goal work" id="goal-work">
      <header className="work-panel-heading">
        <h2>{goal.state.status === "active" ? "In progress" : statusLabel(goal.state.status)}</h2>
        <p>Active work and monitoring for this goal.</p>
      </header>
      {hasApprovals && (
        <details className="work-section goal-approvals" open>
          <summary>
            Needs your input
            <ChevronDown size={16} />
          </summary>
          {error && (
            <p className="goals-error" role="alert">
              {error}
            </p>
          )}
          <Approvals contextPaths={approvalPaths} inspect={inspect} report={setError} />
        </details>
      )}
      <details className="work-section" open>
        <summary>
          Tasks / Executions <span>({tasks.length + runs.length})</span>
          <ChevronDown size={16} />
        </summary>
        <div className="work-cards">
          {!tasks.length && !runs.length && (
            <p className="quiet-message">No tasks yet. Planned work will appear here.</p>
          )}
          {tasks.map((task) => {
            const execution = task.execution;
            const status =
              task.status === "completed" ? "completed" : execution?.status || task.status;
            return (
              <article className="work-card" key={task.id}>
                <WorkIcon status={status} />
                <div className="work-card-body">
                  <div className="work-card-title">
                    <h3>
                      {execution ? (
                        <button
                          className="work-title-link"
                          aria-label={`View execution: ${task.title}`}
                          onClick={() => inspect(execution.runPath)}
                        >
                          {task.title}
                        </button>
                      ) : (
                        task.title
                      )}
                    </h3>
                    <Pill status={status} />
                  </div>
                  <p>{task.instructions}</p>
                  {task.updatedAt && (
                    <small>
                      Updated {dateLabel(task.updatedAt)} · {clockLabel(task.updatedAt)}
                    </small>
                  )}
                  {task.result && (
                    <details className="task-result">
                      <summary>View result</summary>
                      <p>{task.result}</p>
                    </details>
                  )}
                </div>
              </article>
            );
          })}
          {runs.map((run) => (
            <article className="work-card" key={run.path}>
              <WorkIcon status={run.state.status} />
              <div className="work-card-body">
                <div className="work-card-title">
                  <h3>
                    <button
                      className="work-title-link"
                      aria-label={`View execution: ${run.description}`}
                      onClick={() => inspect(run.path)}
                    >
                      {run.description}
                    </button>
                  </h3>
                  <Pill status={run.state.status} />
                </div>
              </div>
            </article>
          ))}
        </div>
      </details>
      <details className="work-section" open>
        <summary>
          Signals <span>({signals.length})</span>
          <ChevronDown size={16} />
        </summary>
        <div className="work-cards">
          {!signals.length && (
            <p className="quiet-message">
              No signals yet. Monitoring for this goal will appear here.
            </p>
          )}
          {signals.map((signal) => {
            const schedule = signal.state.schedule;
            const sources = [
              ...new Set([
                ...(signal.state.occurrences ?? []).map((occurrence) => occurrence.source.path),
                ...references(signal),
              ]),
            ].filter((path) => path !== goal.path);
            return (
              <article className="work-card signal-card" key={signal.path}>
                <Search className="signal-icon" size={23} />
                <div className="work-card-body">
                  <div className="work-card-title">
                    <h3>
                      <button
                        className="work-title-link"
                        aria-label={`View signal: ${signal.description}`}
                        onClick={() => inspect(signal.path)}
                      >
                        {signal.description}
                      </button>
                    </h3>
                  </div>
                  <Pill status={signal.state.active === false ? "paused" : "active"} />
                  <p>
                    {signal.state.when ||
                      (typeof signal.state.task === "string"
                        ? signal.state.task
                        : signal.state.task?.instructions)}
                  </p>
                  <div className="signal-schedule">
                    <Clock3 size={14} />
                    <span>
                      {schedule
                        ? schedule.type === "once"
                          ? `Once · ${dateLabel(schedule.at)} ${clockLabel(schedule.at)}`
                          : `Recurring · ${schedule.timeZone}`
                        : "On relevant updates"}
                    </span>
                  </div>
                  {signal.state.nextDue !== undefined && (
                    <div className="signal-schedule">
                      <Clock3 size={14} />
                      <span>Next check</span>
                      <b>
                        {dateLabel(signal.state.nextDue)} {clockLabel(signal.state.nextDue)}
                      </b>
                    </div>
                  )}
                  {sources.length > 0 && (
                    <div className="signal-context">
                      <h4>
                        <Link2 size={14} /> Context
                      </h4>
                      {sources.map((path) => (
                        <button key={path} className="text-link" onClick={() => inspect(path)}>
                          <ChevronDown size={12} />
                          {nameFor(path)}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              </article>
            );
          })}
        </div>
      </details>
    </aside>
  );
}
