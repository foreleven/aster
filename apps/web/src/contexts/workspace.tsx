import { useState } from "react";
import { Schema } from "effect";
import { Markdown } from "../components/markdown";
import { Badge } from "../components/ui/badge";
import { Separator } from "../components/ui/separator";
import { Button } from "../components/ui/button";
import { ErrorNotice, Status } from "../components/feedback";
import { Approvals } from "../approvals/panel";
import { TaskDetails } from "../tasks/details";
import { TaskControls } from "../tasks/controls";
import { ProcessingDetails } from "./processing";
import { ContextList } from "./list";
import {
  contextTitle,
  dateTime,
  isTaskPath,
  kindOf,
  references,
  summaryText,
  taskText,
  type ContextView,
} from "./model";

// Public records are integration-defined. Decode readable fields without importing integrations.
const ReadableRecord = Schema.Struct({
  text: Schema.optional(Schema.String),
  content: Schema.optional(Schema.String),
  at: Schema.optional(Schema.String),
  deleted: Schema.optional(Schema.Boolean),
  sender: Schema.optional(Schema.Struct({ name: Schema.optional(Schema.String) })),
});
function SourceRecord({ value }: { value: unknown }) {
  const decoded = Schema.decodeUnknownResult(ReadableRecord)(value);
  if (decoded._tag === "Success") {
    const record = decoded.success;
    const text = record.text ?? record.content;
    if (record.deleted) return <p className="text-sm text-muted-foreground">Message deleted</p>;
    if (text)
      return (
        <article className="flex min-w-0 flex-col gap-2 rounded-lg border p-4">
          <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
            {record.sender?.name && <span>{record.sender.name}</span>}
            {record.at && <time dateTime={record.at}>{dateTime(record.at)}</time>}
          </div>
          <p className="whitespace-pre-wrap [overflow-wrap:anywhere]">{text}</p>
        </article>
      );
  }
  return (
    <details className="min-w-0 rounded-lg border p-4 text-sm">
      <summary className="text-muted-foreground">Structured record</summary>
      <pre className="mt-3 max-h-96 overflow-auto whitespace-pre-wrap [overflow-wrap:anywhere]">
        {JSON.stringify(value, null, 2)}
      </pre>
    </details>
  );
}

export function ContextWorkspace({
  context,
  contexts,
  navigate,
}: {
  context: ContextView;
  contexts: readonly ContextView[];
  navigate: (path: string) => void;
}) {
  const [error, setError] = useState("");
  const restricted = context.projection?.visibility === "restricted";
  const kind = kindOf(context.path);
  const links = references(context);
  const related = contexts.filter(
    (item) => item.path !== context.path && links.includes(item.path),
  );
  const summary = summaryText(context.state.summary);
  return (
    <article className="mx-auto flex w-full max-w-4xl flex-col gap-6 p-5 md:p-10">
      <header className="flex flex-col gap-3">
        <p className="text-xs text-muted-foreground">{kind.toUpperCase()}</p>
        <h1 className="text-2xl font-medium tracking-tight break-words md:text-3xl">
          {contextTitle(context)}
        </h1>
        <p className="text-sm text-muted-foreground">{context.description}</p>
        <div className="flex gap-2">
          <Status value={context.state.status} />
          {restricted && <Badge variant="outline">Restricted</Badge>}
        </div>
      </header>
      <ErrorNotice error={context.projectionError || error} />
      {restricted ? (
        <p>Only this record’s name, path and revision are available.</p>
      ) : (
        <>
          {summary && <Markdown>{summary}</Markdown>}
          {kind === "task" && isTaskPath(context.path) ? (
            <>
              <TaskControls context={context} />
              <TaskDetails path={context.path} navigate={navigate} />
              <Approvals contextPaths={[context.path]} inspect={navigate} report={setError} />
            </>
          ) : context.path === "/approvals" ? (
            <Approvals inspect={navigate} report={setError} />
          ) : context.path === "/system-one" ? (
            <ProcessingDetails owner="system-one" navigate={navigate} />
          ) : kind === "signal" ? (
            <>
              <section className="flex flex-col gap-3">
                <h2 className="font-medium">When it runs</h2>
                {context.state.trigger?._tag === "Context" ? (
                  <p>{context.state.trigger.when}</p>
                ) : context.state.trigger?._tag === "Schedule" ? (
                  <div className="text-sm">
                    <p>
                      {context.state.trigger.schedule.type === "once"
                        ? dateTime(context.state.trigger.schedule.at)
                        : `${context.state.trigger.schedule.expression} · ${context.state.trigger.schedule.timeZone}`}
                    </p>
                    <p className="mt-2 text-muted-foreground">
                      {context.state.nextDue
                        ? `Next run: ${dateTime(context.state.nextDue)}`
                        : "No next run scheduled."}
                    </p>
                  </div>
                ) : null}
              </section>
              <Separator />
              <section className="flex flex-col gap-3">
                <h2 className="font-medium">What it does</h2>
                <Markdown>
                  {taskText(context.state.task) || "No task description available."}
                </Markdown>
              </section>
              {context.state.owner && (
                <Button
                  variant="outline"
                  className="self-start"
                  onClick={() => navigate(context.state.owner!)}
                >
                  Open owning conversation
                </Button>
              )}
            </>
          ) : context.state.bodyPlainText !== undefined ? (
            <section className="flex flex-col gap-3">
              {context.state.from && (
                <p className="text-sm text-muted-foreground">From: {context.state.from}</p>
              )}
              <p className="whitespace-pre-wrap [overflow-wrap:anywhere]">
                {context.state.bodyPlainText}
              </p>
            </section>
          ) : (
            <section className="flex flex-col gap-3">
              <h2 className="font-medium">Public records</h2>
              {context.messages.length ? (
                context.messages.map((message, index) => (
                  <SourceRecord value={message} key={index} />
                ))
              ) : (
                <p className="text-sm text-muted-foreground">No public messages in this record.</p>
              )}
            </section>
          )}
          {related.length > 0 && (
            <>
              <Separator />
              <section className="flex flex-col gap-3">
                <h2 className="font-medium">Related</h2>
                <ContextList contexts={related} navigate={navigate} />
              </section>
            </>
          )}
          {kind !== "task" && (
            <details className="text-sm">
              <summary className="cursor-pointer text-muted-foreground">
                Public context data
              </summary>
              <pre className="mt-3 max-h-96 overflow-auto rounded-lg bg-muted p-4 text-xs">
                {JSON.stringify(context.rawState, null, 2)}
              </pre>
            </details>
          )}
        </>
      )}
      <footer className="flex flex-wrap gap-2 text-xs text-muted-foreground">
        <span className="break-all">{context.path}</span>
        <span>· Revision {context.revision}</span>
      </footer>
    </article>
  );
}
