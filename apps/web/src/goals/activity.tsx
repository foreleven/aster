import { useState } from "react";
import { kindOf, summaryText, type ContextView } from "../contexts/model";
import { ContextList } from "../contexts/list";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "../components/ui/tabs";
import { Separator } from "../components/ui/separator";
import { ErrorNotice, Status } from "../components/feedback";
import { Markdown } from "../components/markdown";
import { Approvals } from "../approvals/panel";

export function GoalActivity({
  goal,
  contexts,
  navigate,
}: {
  goal: ContextView;
  contexts: readonly ContextView[];
  navigate: (path: string) => void;
}) {
  const [approvalError, setApprovalError] = useState("");
  const tasks = contexts.filter((context) => goal.state.tasks?.includes(context.path));
  const signals = contexts.filter(
    (context) => kindOf(context.path) === "signal" && context.state.owner === goal.path,
  );
  const summary = summaryText(goal.state.summary);

  return (
    <Tabs defaultValue="activity" className="min-h-0 flex-1 gap-0">
      <div className="shrink-0 border-b p-4">
        <TabsList aria-label="Goal details" className="w-full">
          <TabsTrigger value="activity">Activity</TabsTrigger>
          <TabsTrigger value="summary">Summary</TabsTrigger>
        </TabsList>
      </div>
      <TabsContent
        value="activity"
        forceMount
        className="min-h-0 overflow-y-auto overscroll-contain p-5 data-[state=inactive]:hidden"
      >
        <div className="flex flex-col gap-6">
          <section className="flex flex-col gap-3">
            <h2 className="font-medium">About this space</h2>
            <p className="text-sm text-muted-foreground [overflow-wrap:anywhere]">
              {goal.description}
            </p>
            <Status value={goal.state.status} />
          </section>
          <Separator />
          <section className="flex flex-col gap-3">
            <h2 className="font-medium">Needs you</h2>
            <ErrorNotice error={approvalError} />
            <Approvals
              contextPaths={goal.state.tasks ?? []}
              inspect={navigate}
              report={setApprovalError}
            />
          </section>
          <Separator />
          <section className="flex flex-col gap-3">
            <h2 className="font-medium">Tasks · {tasks.length}</h2>
            <ContextList contexts={tasks} navigate={navigate} />
            {!tasks.length && <p className="text-sm text-muted-foreground">No tasks yet.</p>}
          </section>
          <Separator />
          <section className="flex flex-col gap-3">
            <h2 className="font-medium">Following · {signals.length}</h2>
            <ContextList contexts={signals} navigate={navigate} />
            {!signals.length && (
              <p className="text-sm text-muted-foreground">No reminders or watches yet.</p>
            )}
          </section>
        </div>
      </TabsContent>
      <TabsContent
        value="summary"
        className="min-h-0 overflow-y-auto overscroll-contain p-5 text-sm"
      >
        {summary ? (
          <Markdown>{summary}</Markdown>
        ) : (
          <p className="text-muted-foreground">No summary yet.</p>
        )}
      </TabsContent>
    </Tabs>
  );
}
