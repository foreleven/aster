import { useMemo } from "react";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { goalTimeline } from "../api/timeline";
import { resultError, resultValue } from "../api/client";
import { clockLabel, dateLabel, EmptyState } from "./presentation";
import { Markdown } from "../components/markdown";

export function Timeline({ slug, userOnly = false }: { slug: string; userOnly?: boolean }) {
  const atoms = useMemo(() => goalTimeline(slug), [slug]);
  const result = useAtomValue(atoms.feed);
  const more = useAtomSet(atoms.before);
  const page = resultValue(result);
  const error = resultError(result);
  if (error) return <p className="goals-error">{error}</p>;
  if (!page) return <p className="quiet-message">Loading conversation…</p>;
  return (
    <div className="timeline-list goal-timeline">
      {page.nextBefore !== null && (
        <button className="outline-action" onClick={() => more(page.nextBefore!)}>
          Load earlier messages
        </button>
      )}
      {!page.messages.length && (
        <EmptyState title="No conversation yet">Send a message to get started.</EmptyState>
      )}
      {page.messages
        .filter((message) => !userOnly || message.role === "user")
        .map((message) => (
          <article key={message.id} className={`conversation-entry conversation-${message.role}`}>
            <header>
              <strong>{message.role === "user" ? "You" : "Assistant"}</strong>
              <time dateTime={message.at}>
                {dateLabel(message.at)} · {clockLabel(message.at)}
              </time>
            </header>
            <Markdown>{message.text}</Markdown>
          </article>
        ))}
    </div>
  );
}
