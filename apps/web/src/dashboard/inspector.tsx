import { Markdown } from "../components/markdown";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { contextQueryKeys } from "@aster/api-contracts";
import { sendGoalMessage, endGoal } from "../api/client";
import { GoalFeed } from "./goal-feed";
import React, { useState } from "react";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from "@/components/ui/sheet";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Field, FieldLabel } from "@/components/ui/field";
import { ArrowRight, Check } from "lucide-react";
import { Status, Messages } from "./shared";
import { runStages } from "@/lib/dashboard";
import { inspectorView } from "./state";
import { projectMessage, summaryText, type DisplayState } from "./model";
import { titleFor } from "../goals/presentation";
export function Inspector({
  path,
  close,
  inspect,
  report,
}: {
  path: string;
  close: () => void;
  inspect: (path: string) => void;
  report: (error: string) => void;
}) {
  const { row, related, events } = useAtomValue(inspectorView(path));
  const [text, setText] = useState(""),
    [confirmEnd, setConfirmEnd] = useState(false);
  const c = row?.context,
    s: DisplayState = c?.state || {},
    restricted = c?.projection?.visibility === "restricted",
    goal = restricted ? null : c?.path.match(/^\/goals\/([^/]+)$/);
  const summary = summaryText(s.summary);
  const send = useAtomSet(sendGoalMessage, { mode: "promise" });
  const end = useAtomSet(endGoal, { mode: "promise" });
  const sending = useAtomValue(sendGoalMessage).waiting;
  const ending = useAtomValue(endGoal).waiting;
  const busy = sending || ending;
  async function act(action: "messages" | "end") {
    if (!c || !goal || c.projectionError) return;
    try {
      const reactivityKeys = contextQueryKeys(c.path);
      if (action === "messages") await send({ payload: { slug: goal[1], text }, reactivityKeys });
      else await end({ payload: { slug: goal[1] }, reactivityKeys });
      setText("");
      setConfirmEnd(false);
    } catch (e) {
      report(e instanceof Error ? e.message : String(e));
    }
  }
  return (
    <Sheet
      open={!!row}
      onOpenChange={(open) => {
        if (!open) close();
      }}
    >
      <SheetContent className="inspector">
        <SheetHeader className="px-0">
          <SheetTitle>
            {goal && c ? titleFor(c) : c?.description || row?.path?.split("/").at(-1)}
          </SheetTitle>
          <SheetDescription className="mono break-all">{row?.path}</SheetDescription>
        </SheetHeader>
        {row && (
          <>
            {restricted && (
              <p role="status" className="quiet-message">
                This Context exposes its path and revision only. Its contents are not available in
                the public view.
              </p>
            )}
            <div className="flex gap-2 my-4">
              <Status value={row.status} />
              {s.status && <Status value={s.status} />}
            </div>
            {row.actor && (
              <dl className="grid grid-cols-3 gap-3 py-4 border-y text-xs">
                <div>
                  <dt className="text-muted-foreground">Mailbox</dt>
                  <dd className="mt-1">{row.actor.mailboxSize}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Processed</dt>
                  <dd className="mt-1">{row.actor.processed}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Current Command</dt>
                  <dd className="mt-1 break-all">{row.actor.currentCommand || "Idle"}</dd>
                </div>
              </dl>
            )}
            {(row.actor?.pendingEffects ?? 0) > 0 && (
              <p className="text-xs text-muted-foreground mt-3">
                {row.actor?.pendingEffects} pending Effects (including timers)
              </p>
            )}
            {row.actor?.lastError && (
              <p className="text-sm text-destructive mt-3">
                Latest runtime error: {row.actor.lastError}
              </p>
            )}
            {!restricted && c?.path.includes("/runs/") && (
              <div className="flow">
                {runStages(c).map((v, i) => (
                  <React.Fragment key={v.title}>
                    {i > 0 && <ArrowRight className="size-3 shrink-0 text-muted-foreground" />}
                    <div
                      className="flow-step text-xs"
                      data-stage={v.done ? "done" : v.active ? "active" : "pending"}
                    >
                      {v.done && <Check className="size-3 mb-1" />}
                      {v.title}
                      <p className="mt-1 text-muted-foreground">
                        {v.done ? "Completed" : v.active ? "Current stage" : "Not reached"}
                      </p>
                    </div>
                  </React.Fragment>
                ))}
              </div>
            )}
            {s.session && (
              <div className="my-4 text-xs flex flex-col gap-2">
                <b>External session</b>
                <p className="mono break-all">session · {s.session.sessionId}</p>
                <p className="mono break-all">run · {s.session.runId || "—"}</p>
              </div>
            )}
            {related.length > 0 && (
              <div className="flex flex-wrap gap-1 my-4">
                {[...new Set(related)].map((p) => (
                  <Button
                    key={p}
                    variant="outline"
                    size="sm"
                    onClick={() => inspect(p)}
                    className="max-w-full truncate"
                  >
                    {p}
                  </Button>
                ))}
              </div>
            )}
            {goal && summary && (
              <div className="my-4 text-sm">
                <b>Current summary</b>
                <div className="mt-2">
                  <Markdown>{summary}</Markdown>
                </div>
              </div>
            )}
            <Tabs defaultValue="messages">
              <TabsList>
                <TabsTrigger value="messages">
                  Messages · {goal ? s.historyCount || 0 : c?.messages.length || 0}
                </TabsTrigger>
                {goal && (
                  <TabsTrigger value="tasks">
                    Tasks · {(s.tasks || []).filter((t) => t.status !== "deleted").length}
                  </TabsTrigger>
                )}
                {!restricted && <TabsTrigger value="state">State</TabsTrigger>}
                <TabsTrigger value="runtime">Runtime details</TabsTrigger>
                <TabsTrigger value="events">Runtime events</TabsTrigger>
              </TabsList>
              <TabsContent value="messages">
                {goal ? (
                  <GoalFeed key={goal[1]} slug={goal[1]} inspect={inspect} />
                ) : (
                  <Messages messages={c?.messages} inspect={inspect} />
                )}
              </TabsContent>
              {goal && (
                <TabsContent value="tasks">
                  {(s.tasks || [])
                    .filter((t) => t.status !== "deleted")
                    .map((task) => (
                      <article className="message" key={task.id}>
                        <div className="flex justify-between">
                          <b>{task.title}</b>
                          <Status value={task.status} />
                        </div>
                        <p className="my-2 whitespace-pre-wrap">{task.instructions}</p>
                        {task.execution && (
                          <Button
                            variant="link"
                            onClick={() => task.execution && inspect(task.execution.runPath)}
                          >
                            Execution: {task.execution.status}
                          </Button>
                        )}
                        {task.result && (
                          <p className="whitespace-pre-wrap text-sm">{task.result}</p>
                        )}
                      </article>
                    ))}
                </TabsContent>
              )}
              <TabsContent value="state">
                <pre className="bg-muted p-4 rounded-md mt-3">
                  {JSON.stringify(c?.rawState, null, 2)}
                </pre>
              </TabsContent>
              <TabsContent value="runtime">
                <pre className="bg-muted p-4 rounded-md mt-3">
                  {row.actor
                    ? JSON.stringify(row.actor, null, 2)
                    : "No Actor is currently running. Showing the persisted Context."}
                </pre>
              </TabsContent>
              <TabsContent value="events">
                <Messages
                  messages={events.map((e) =>
                    projectMessage({ ...e, type: e._tag, at: e.timestamp }),
                  )}
                  inspect={inspect}
                />
              </TabsContent>
            </Tabs>
            {goal && !c?.projectionError && s.status !== "completed" && (
              <form
                className="mt-6 flex flex-col gap-3"
                onSubmit={(e) => {
                  e.preventDefault();
                  act("messages");
                }}
              >
                <Field>
                  <FieldLabel htmlFor="goal-message">Add information to Goal</FieldLabel>
                  <Textarea
                    id="goal-message"
                    value={text}
                    maxLength={8000}
                    onChange={(e) => setText(e.target.value)}
                    placeholder="Add context or adjust priorities…"
                  />
                </Field>
                <div className="flex justify-between gap-2">
                  <Button disabled={busy || !text.trim()}>Send</Button>
                  <Button
                    type="button"
                    variant="outline"
                    disabled={busy}
                    onClick={() => (confirmEnd ? act("end") : setConfirmEnd(true))}
                  >
                    {confirmEnd ? "Confirm ending Goal" : "End Goal"}
                  </Button>
                </div>
                {confirmEnd && (
                  <Button type="button" variant="ghost" onClick={() => setConfirmEnd(false)}>
                    Keep Goal active
                  </Button>
                )}
              </form>
            )}
          </>
        )}
      </SheetContent>
    </Sheet>
  );
}
