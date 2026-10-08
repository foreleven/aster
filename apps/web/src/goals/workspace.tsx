import { useMemo, useState } from "react";
import { useAtomValue } from "@effect/atom-react";
import { Asterisk, PanelRight } from "lucide-react";
import { goalTimeline } from "../api/timeline";
import { resultValue } from "../api/client";
import { contextTitle, kindOf, type ContextView } from "../contexts/model";
import { Button } from "../components/ui/button";
import {
  Sheet,
  SheetTrigger,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from "../components/ui/sheet";
import { ErrorNotice } from "../components/feedback";
import { Composer } from "../assistant/composer";
import { ContextList } from "../contexts/list";
import { Timeline } from "./timeline";
import { RetryTurn } from "./retry-turn";
import { GoalActivity } from "./activity";
import { useIsMobile } from "../hooks/use-mobile";

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
  const compact = useIsMobile(1024);
  const [activityOpen, setActivityOpen] = useState(false);
  const empty = page?.total === 0;
  const canWrite = goal.state.status === "active" && !goal.projectionError;
  const spaces = contexts
    .filter((context) => kindOf(context.path) === "goal" && context.path !== goal.path)
    .slice(0, 3);
  const open = (path: string) => {
    setActivityOpen(false);
    navigate(path);
  };
  return (
    <div className="flex min-h-0 flex-1 overflow-hidden">
      <section className="flex min-h-0 min-w-0 flex-1 flex-col" aria-label="Goal conversation">
        <header className="flex shrink-0 items-center justify-between gap-3 px-5 py-4 md:px-8">
          <div className="min-w-0">
            <h1 className="truncate font-medium">{contextTitle(goal)}</h1>
          </div>
          {compact && (
            <Sheet open={activityOpen} onOpenChange={setActivityOpen}>
              <SheetTrigger asChild>
                <Button variant="outline" size="sm" className="lg:hidden">
                  <PanelRight data-icon="inline-start" />
                  Activity
                </Button>
              </SheetTrigger>
              <SheetContent className="gap-0 overflow-hidden sm:max-w-lg">
                <SheetHeader>
                  <SheetTitle>Activity</SheetTitle>
                  <SheetDescription>Work and decisions for this conversation.</SheetDescription>
                </SheetHeader>
                <GoalActivity goal={goal} contexts={contexts} navigate={open} />
              </SheetContent>
            </Sheet>
          )}
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
      {!compact && (
        <aside
          aria-label="Goal details"
          className="hidden w-80 shrink-0 flex-col border-l lg:flex xl:w-96"
        >
          <GoalActivity goal={goal} contexts={contexts} navigate={navigate} />
        </aside>
      )}
    </div>
  );
}
