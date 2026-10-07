import { Cause, Exit, Schema } from "effect";
import React, { useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Field, FieldGroup, FieldLabel, FieldDescription } from "@/components/ui/field";
import { Alert, AlertTitle, AlertDescription } from "@/components/ui/alert";
import { Status, Blank } from "./shared";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import {
  ApplicationError,
  type PublicApprovalEntry,
  type ApprovalResponse,
} from "@aster/core/contracts";
import { contextQueryKeys } from "@aster/api";
import { respondToApproval } from "../api/client";

import { approvalEntries, approvalDiagnostics, pendingApprovalResponses } from "./state";
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
  const { tasks, failures: goalFailures } = useAtomValue(approvalDiagnostics);
  const [answers, setAnswers] = useState<Record<string, string>>({});
  const respondMutation = useAtomSet(respondToApproval, { mode: "promiseExit" });
  const pending = useAtomValue(pendingApprovalResponses);
  const setPending = useAtomSet(pendingApprovalResponses);
  const busy = useAtomValue(respondToApproval).waiting;
  const inFlight = useRef(false);
  async function respond(entry: PublicApprovalEntry, response: ApprovalResponse) {
    if (inFlight.current) return;
    inFlight.current = true;
    const frozen = pending[entry.id] ?? response;
    setPending((previous) => ({ ...previous, [entry.id]: frozen }));
    try {
      const result = await respondMutation({
        payload: { id: entry.id, response: frozen },
        reactivityKeys: contextQueryKeys("/approvals"),
      });
      if (Exit.isFailure(result)) {
        const failure = Cause.squash(result.cause);
        report(failure instanceof Error ? failure.message : String(failure));
        if (!Schema.is(ApplicationError)(failure) || failure.kind === "unavailable") return;
      } else report("");
      setPending((previous) =>
        Object.fromEntries(Object.entries(previous).filter(([id]) => id !== entry.id)),
      );
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
              tasks === 0
                ? "No Tasks yet. The workflow has not reached task confirmation or external execution."
                : `There are ${tasks} Tasks. No tasks currently require human approval.`
            }
          />
          <p className="text-sm text-muted-foreground">
            The queue contains task confirmations and requests for permission or additional
            information. Results appear in the originating Goal.
          </p>
          {goalFailures.map((failure) => (
            <Alert variant="destructive" key={failure.path}>
              <AlertTitle>Goal conversation interrupted</AlertTitle>
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
            {e.status === "pending" && pending[e.id] ? (
              <div>
                <p>Response not confirmed. Reconcile the saved decision before making another.</p>
                <Button disabled={!!busy} onClick={() => respond(e, pending[e.id]!)}>
                  Reconcile saved decision
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
