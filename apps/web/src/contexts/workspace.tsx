import { useState } from "react";
import { Markdown } from "../components/markdown";
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
  actionText,
  type ContextView,
} from "./model";

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
        </div>
      </header>
      <ErrorNotice error={error} />
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
                {actionText(context.state.action) || "No action description available."}
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
        ) : (
          <section className="flex flex-col gap-3">
            <h2 className="font-medium">Context</h2>
            {context.text ? (
              <Markdown>{context.text}</Markdown>
            ) : (
              <p className="text-sm text-muted-foreground">No context content available.</p>
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
            <summary className="cursor-pointer text-muted-foreground">Context data</summary>
            <pre className="mt-3 max-h-96 overflow-auto rounded-lg bg-muted p-4 text-xs">
              {JSON.stringify(context.rawState, null, 2)}
            </pre>
          </details>
        )}
      </>
      <footer className="flex flex-wrap gap-2 text-xs text-muted-foreground">
        <span className="break-all">{context.path}</span>
        <span>· Revision {context.revision}</span>
      </footer>
    </article>
  );
}
