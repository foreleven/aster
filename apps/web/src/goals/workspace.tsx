import { useMemo } from "react";
import { useAtomValue } from "@effect/atom-react";
import { Asterisk, PanelRight } from "lucide-react";
import { goalTimeline } from "../api/timeline";
import { resultValue } from "../api/client";
import { contextTitle, kindOf, summaryText, type ContextView } from "../contexts/model";
import { Button } from "../components/ui/button";
import {
  Sheet,
  SheetTrigger,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from "../components/ui/sheet";
import { Separator } from "../components/ui/separator";
import { ErrorNotice, Status } from "../components/feedback";
import { Markdown } from "../components/markdown";
import { Composer } from "../assistant/composer";
import { ContextList } from "../contexts/list";
import { Timeline } from "./timeline";
import { RetryTurn } from "./retry-turn";
import { Approvals } from "../approvals/panel";
import { useState } from "react";

export function GoalWorkspace({
  goal,
  contexts,
  navigate,
}: {
  goal: ContextView;
  contexts: readonly ContextView[];
  navigate: (path: string) => void;
}) {
  const slug = goal.path.split("/")[2]!;
  const atoms = useMemo(() => goalTimeline(slug), [slug]);
  const page = resultValue(useAtomValue(atoms.feed));
  const [activityOpen, setActivityOpen] = useState(false);
  const [approvalError, setApprovalError] = useState("");
  const empty = page?.total === 0;
  const canWrite = goal.state.status === "active" && !goal.projectionError;
  const tasks = contexts.filter((context) => goal.state.tasks?.includes(context.path));
  const signals = contexts.filter(
    (context) => kindOf(context.path) === "signal" && context.state.owner === goal.path,
  );
  const spaces = contexts
    .filter((context) => kindOf(context.path) === "goal" && context.path !== goal.path)
    .slice(0, 3);
  const open = (path: string) => {
    setActivityOpen(false);
    navigate(path);
  };
  return (
    <section className="flex min-h-0 flex-1 flex-col" aria-label="Goal conversation">
      <header className="flex shrink-0 items-center justify-between gap-3 px-5 py-4 md:px-8">
        <div className="min-w-0">
          <h1 className="truncate font-medium">{contextTitle(goal)}</h1>
        </div>
        <Sheet open={activityOpen} onOpenChange={setActivityOpen}>
          <SheetTrigger asChild>
            <Button variant="outline" size="sm">
              <PanelRight data-icon="inline-start" />
              Activity
            </Button>
          </SheetTrigger>
          <SheetContent className="overflow-y-auto sm:max-w-lg">
            <SheetHeader>
              <SheetTitle>Activity</SheetTitle>
              <SheetDescription>Work and decisions for this conversation.</SheetDescription>
            </SheetHeader>
            <div className="flex flex-col gap-6 p-5">
              <section className="flex flex-col gap-3">
                <h2 className="font-medium">About this space</h2>
                <p className="text-sm text-muted-foreground">{goal.description}</p>
                <Status value={goal.state.status} />
                {goal.state.summary && <Markdown>{summaryText(goal.state.summary) ?? ""}</Markdown>}
              </section>
              <Separator />
              <section className="flex flex-col gap-3">
                <h2 className="font-medium">Needs you</h2>
                <ErrorNotice error={approvalError} />
                <Approvals
                  contextPaths={goal.state.tasks ?? []}
                  inspect={open}
                  report={setApprovalError}
                />
              </section>
              <Separator />
              <section className="flex flex-col gap-3">
                <h2 className="font-medium">Tasks · {tasks.length}</h2>
                <ContextList contexts={tasks} navigate={open} />
                {!tasks.length && <p className="text-sm text-muted-foreground">No tasks yet.</p>}
              </section>
              <Separator />
              <section className="flex flex-col gap-3">
                <h2 className="font-medium">Following · {signals.length}</h2>
                <ContextList contexts={signals} navigate={open} />
                {!signals.length && (
                  <p className="text-sm text-muted-foreground">No reminders or watches yet.</p>
                )}
              </section>
            </div>
          </SheetContent>
        </Sheet>
      </header>
      {(goal.projectionError || goal.state.lastError) && (
        <div className="px-5 pb-3">
          <ErrorNotice error={goal.projectionError || goal.state.lastError || ""} />
        </div>
      )}
      {goal.state.retryableInputId && (
        <div className="px-5 pb-3">
          <RetryTurn slug={slug} turnId={goal.state.retryableInputId} />
        </div>
      )}
      {empty ? (
        <div className="flex min-h-0 flex-1 flex-col items-center justify-center gap-6 overflow-y-auto px-5 py-8">
          <div className="assistant-mark">
            <Asterisk aria-hidden="true" />
          </div>
          <div className="text-center">
            <h2 className="text-3xl font-medium tracking-tight md:text-4xl">
              A little less to carry.
            </h2>
            <p className="mt-3 text-sm text-muted-foreground">
              Think it through. Make a plan. Take the next step.
            </p>
          </div>
          {spaces.length > 0 && (
            <div className="mt-3 w-full max-w-xl">
              <p className="mb-3 text-xs text-muted-foreground">YOUR SPACES</p>
              <ContextList contexts={spaces} navigate={navigate} />
            </div>
          )}
        </div>
      ) : (
        <Timeline slug={slug} />
      )}
      <div className="mx-auto w-full max-w-3xl shrink-0 px-5 pb-5 pt-3 md:px-8">
        {canWrite ? (
          <Composer slug={slug} contexts={contexts} suggestions={empty} />
        ) : (
          <p className="text-sm text-muted-foreground">This conversation is read-only.</p>
        )}
      </div>
    </section>
  );
}
