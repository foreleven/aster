import { Cause, Exit, Schema } from "effect";
import React, { useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Field, FieldGroup, FieldLabel, FieldDescription } from "@/components/ui/field";
import { Alert, AlertTitle, AlertDescription } from "@/components/ui/alert";
import { Status, Blank } from "./shared";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { ApplicationError, contextQueryKeys } from "@aster/api-contracts";
import { respondPersonalApproval, respondToApproval, invalidateQueries } from "../api/client";
import type { ApprovalEntry, ApprovalResponse } from "@aster/api-contracts";
import {
  approvalEntries,
  approvalDiagnostics,
  contextViews,
  pendingApprovalResponses,
} from "./state";
export function Approvals({
  inspect,
  report,
  contextPaths,
}: {
  inspect: (path: string) => void;
  report: (error: string) => void;
  contextPaths?: readonly string[];
}) {
  const allEntries = useAtomValue(approvalEntries);
  const entries = contextPaths
    ? allEntries.filter((entry) => contextPaths.includes(entry.contextPath))
    : allEntries;
  const { runs, failures: goalFailures } = useAtomValue(approvalDiagnostics);
  const [answers, setAnswers] = useState<Record<string, string>>({});
  const respondMutation = useAtomSet(respondToApproval, { mode: "promise" });
  const personalMutation = useAtomSet(respondPersonalApproval, { mode: "promiseExit" });
  const personalBusy = useAtomValue(respondPersonalApproval).waiting;
  const legacyBusy = useAtomValue(respondToApproval).waiting;
  const busy = personalBusy || legacyBusy;
  const inFlight = useRef(false);
  const contexts = useAtomValue(contextViews);
  const personal = contexts.find((item) => item.path === "/personal");
  const queue = contexts.find((item) => item.path === "/approvals");
  const pending = useAtomValue(pendingApprovalResponses);
  const setPending = useAtomSet(pendingApprovalResponses);
  const invalidate = useAtomSet(invalidateQueries);
  const delivery = (id: string) =>
    personal?.personalState?.outbox?.findLast(
      (item) => "approvalId" in item.input && item.input.approvalId === id,
    );
  const retained = (id: string) => (delivery(id)?.status === "rejected" ? undefined : pending[id]);
  const clear = (id: string) =>
    setPending((current) =>
      Object.fromEntries(Object.entries(current).filter(([key]) => key !== id)),
    );
  async function respond(entry: ApprovalEntry, response: ApprovalResponse) {
    if (inFlight.current) return;
    inFlight.current = true;
    try {
      if (!personal) {
        await respondMutation({
          payload: { id: entry.id, response },
          reactivityKeys: contextQueryKeys("/approvals"),
        });
        return;
      }
      if (
        personal.revision === undefined ||
        queue?.revision === undefined ||
        personal.projectionError
      ) {
        report("Approval revisions are unavailable. Refresh before responding.");
        return;
      }
      const input = retained(entry.id)?.input ?? {
        requestId: crypto.randomUUID(),
        causationId: crypto.randomUUID(),
        approvalId: entry.id,
        expectedRevision: personal.revision,
        approvalsRevision: queue.revision,
        response,
      };
      setPending((current) => ({ ...current, [entry.id]: { input, accepted: false } }));
      const result = await personalMutation({
        payload: input,
        reactivityKeys: [...contextQueryKeys("/personal"), ...contextQueryKeys("/approvals")],
      });
      if (Exit.isSuccess(result)) {
        setPending((current) => ({ ...current, [entry.id]: { input, accepted: true } }));
        report("");
      } else {
        const failure = Cause.squash(result.cause);
        report(failure instanceof Error ? failure.message : String(failure));
        const error = Cause.findError(result.cause);
        if (
          error._tag === "Success" &&
          Schema.is(ApplicationError)(error.success) &&
          ["conflict", "invalid-input"].includes(error.success.kind)
        ) {
          clear(entry.id);
          invalidate([...contextQueryKeys("/personal"), ...contextQueryKeys("/approvals")]);
        }
      }
    } catch (e) {
      report(e instanceof Error ? e.message : String(e));
    } finally {
      inFlight.current = false;
    }
  }
  return (
    <div className="flex flex-col gap-4">
      {entries.length === 0 && (
        <section className="panel p-6 flex flex-col gap-4">
          <Blank
            title="No approval requests yet"
            detail={
              runs === 0
                ? "No Signal occurrences yet. The workflow has not reached task confirmation or external execution."
                : `There are ${runs} Signal occurrences. No tasks currently require human approval.`
            }
          />
          <p className="text-sm text-muted-foreground">
            The queue receives task confirmations in confirm mode and requests for permission or
            information from agents delegated by this system. Auto mode skips confirmation before
            execution.
          </p>
          {goalFailures.map((failure) => (
            <Alert variant="destructive" key={failure.path}>
              <AlertTitle>Goal evaluation incomplete</AlertTitle>
              <AlertDescription>
                <p>{failure.text}</p>
                <Button variant="outline" size="sm" onClick={() => inspect(failure.path)}>
                  View Goal failure records
                </Button>
              </AlertDescription>
            </Alert>
          ))}
        </section>
      )}
      {[...entries]
        .sort((a, b) => Number(a.status !== "pending") - Number(b.status !== "pending"))
        .map((e) => (
          <section className="panel p-6" key={e.id}>
            <div className="flex justify-between gap-3">
              <h2>
                {e.kind === "confirmation"
                  ? "Confirm task execution"
                  : e.kind === "approval"
                    ? "Execution approval"
                    : "Additional information"}
              </h2>
              <Status value={e.status} />
            </div>
            <Button
              className="my-2 max-w-full truncate"
              variant="link"
              size="sm"
              onClick={() => inspect(e.contextPath)}
            >
              {e.contextPath}
            </Button>
            <pre className="bg-muted rounded-md p-4 mb-4 max-h-72 overflow-auto">
              {e.request.prompt}
            </pre>
            {delivery(e.id)?.status === "rejected" && <p role="alert">{delivery(e.id)?.error}</p>}
            {e.status === "pending" &&
            (retained(e.id) || (delivery(e.id) && delivery(e.id)?.status !== "rejected")) ? (
              <div className="flex flex-col gap-2">
                <p>
                  {retained(e.id)?.accepted || delivery(e.id)
                    ? "Your decision is saved in Personal. Follow its delivery status there."
                    : "The response acknowledgement is uncertain. Reconcile your saved decision before making another."}
                </p>
                {retained(e.id) && !retained(e.id)?.accepted && !delivery(e.id) && (
                  <Button
                    disabled={!!busy}
                    onClick={() => respond(e, retained(e.id)!.input.response)}
                  >
                    Reconcile saved decision
                  </Button>
                )}
                <Button variant="link" onClick={() => inspect("/personal")}>
                  View Personal delivery
                </Button>
              </div>
            ) : e.status === "pending" ? (
              e.kind === "input" ? (
                <form
                  onSubmit={(ev) => {
                    ev.preventDefault();
                    respond(
                      e,
                      e.request.questions?.length
                        ? {
                            answers: Object.fromEntries(
                              e.request.questions.map((q) => [
                                q.id,
                                (answers[e.id + q.id] || "").split("\n").filter((v) => v.trim()),
                              ]),
                            ),
                          }
                        : { text: answers[e.id] },
                    );
                  }}
                >
                  <FieldGroup>
                    {(e.request.questions?.length
                      ? e.request.questions
                      : [{ id: "", prompt: "Response" }]
                    ).map((q) => (
                      <Field key={q.id}>
                        <FieldLabel htmlFor={e.id + q.id}>{q.prompt}</FieldLabel>
                        {(q.options?.length ?? 0) > 0 && (
                          <FieldDescription>
                            {q.options?.join(" / ")}; enter one selection per line
                          </FieldDescription>
                        )}
                        <Textarea
                          required
                          id={e.id + q.id}
                          value={answers[e.id + q.id] || ""}
                          onChange={(ev) =>
                            setAnswers({
                              ...answers,
                              [e.id + q.id]: ev.target.value,
                            })
                          }
                        />
                      </Field>
                    ))}
                    <Button disabled={!!busy}>Submit response</Button>
                  </FieldGroup>
                </form>
              ) : (
                <div className="flex gap-2">
                  <Button disabled={!!busy} onClick={() => respond(e, { decision: "approve" })}>
                    Approve execution
                  </Button>
                  <Button
                    variant="outline"
                    disabled={!!busy}
                    onClick={() => respond(e, { decision: "reject" })}
                  >
                    Reject
                  </Button>
                </div>
              )
            ) : (
              <p className="text-sm text-muted-foreground">
                {e.status === "acknowledged"
                  ? "The task has received your decision. View its records for execution results."
                  : "Your decision is saved and awaiting delivery to the Actor."}
              </p>
            )}
          </section>
        ))}
    </div>
  );
}
