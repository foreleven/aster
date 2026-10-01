import React, { useMemo } from "react";
import { useAtomRefresh, useAtomSet, useAtomValue } from "@effect/atom-react";
import { ChevronRight } from "lucide-react";
import { goalHistory } from "../api/history";
import { resultError, resultValue } from "../api/client";
import { projectMessage } from "../dashboard/model";
import { clockLabel, dateLabel, EmptyState, eventKind } from "./presentation";

export function Timeline({
  slug,
  filter,
  inspect,
}: {
  slug: string;
  filter: string;
  inspect: (path: string) => void;
}) {
  // The family owns its weakly held cache bundle for this mounted Goal.
  const atoms = useMemo(() => goalHistory(slug), [slug]);
  const result = useAtomValue(atoms.feed);
  const setBefore = useAtomSet(atoms.before);
  const retryFeed = useAtomRefresh(atoms.feed);
  const refreshTail = useAtomRefresh(atoms.latest);
  const page = resultValue(result);
  const error = resultError(result);
  const entries = (page?.entries ?? [])
    .toReversed()
    .map((entry) => ({ ...entry, view: projectMessage(entry.message) }))
    .filter((entry) => filter === "all" || eventKind(entry.view).category === filter);
  return (
    <div className="goal-timeline" aria-busy={result.waiting}>
      {error && (
        <div role="alert" className="goals-error">
          {error}
          <button
            onClick={() => {
              refreshTail();
              retryFeed();
            }}
          >
            Retry history
          </button>
        </div>
      )}
      {!page && !error && (
        <p role="status" className="quiet-message">
          Loading history…
        </p>
      )}
      {page && !entries.length && (
        <EmptyState title={filter === "all" ? "No messages yet" : "No matching events"}>
          {filter === "notes"
            ? "Notes and instructions you send to this Goal appear here."
            : "New activity appears here automatically."}
        </EmptyState>
      )}
      {entries.map((entry, index) => {
        const kind = eventKind(entry.view);
        const day = dateLabel(entry.at);
        const showDate = index === 0 || dateLabel(entries[index - 1].at) !== day;
        const lines = entry.view.text?.split("\n") ?? [];
        const hasTitle = lines.length > 1 && lines[0].length < 100 && kind.category !== "notes";
        return (
          <React.Fragment key={entry.seq}>
            {showDate && <h3 className="timeline-day">{day}</h3>}
            <article className="timeline-event message">
              <time dateTime={entry.at} className="timeline-time">
                {clockLabel(entry.at)}
              </time>
              <kind.icon
                size={21}
                className={`timeline-icon tone-${kind.tone}`}
                aria-hidden="true"
              />
              <span className="timeline-kind">{kind.label}</span>
              <div className="timeline-body">
                {entry.view.tool ? (
                  <details>
                    <summary>{entry.view.toolName || "View tool activity"}</summary>
                    <pre>{entry.view.details}</pre>
                  </details>
                ) : (
                  <>
                    {hasTitle && <strong>{lines[0]}</strong>}
                    {entry.view.text !== undefined ? (
                      <p>{hasTitle ? lines.slice(1).join("\n") : entry.view.text}</p>
                    ) : (
                      <details>
                        <summary>View event details</summary>
                        <pre>{entry.view.details}</pre>
                      </details>
                    )}
                  </>
                )}
                {entry.view.references.length > 0 && (
                  <details className="event-context">
                    <summary>
                      View {entry.view.references.length} context{" "}
                      {entry.view.references.length === 1 ? "item" : "items"}{" "}
                      <ChevronRight size={13} />
                    </summary>
                    <div>
                      {entry.view.references.map((path) => (
                        <button className="text-link" key={path} onClick={() => inspect(path)}>
                          {path}
                        </button>
                      ))}
                    </div>
                  </details>
                )}
              </div>
            </article>
          </React.Fragment>
        );
      })}
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
