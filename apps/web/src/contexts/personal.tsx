import { useRef, useState } from "react";
import { useAtomSet } from "@effect/atom-react";
import { Cause, Exit, Match, Schema } from "effect";
import {
  ApplicationError,
  contextQueryKeys,
  type PersonalInput,
  type PersonalResumeRunInput,
  type PersonalRetryInput,
  type PersonalOutboxItem,
} from "@aster/api-contracts";
import {
  applyPersonalSignal,
  startPersonalTask,
  resumePersonalRun,
  respondPersonalApproval,
  requestPersonalApproval,
  invalidateQueries,
  retryPersonalInput,
  sendPersonalGoalMessage,
  sendPersonalMessage,
} from "../api/client";
import type { ContextView } from "../dashboard/model";

// The app owns this UI state so changing the selected Context cannot discard an uncertain submission.
export function usePersonalCommands(context: ContextView | undefined) {
  const [text, setText] = useState("");
  const [error, setError] = useState("");
  const [pending, setPending] = useState<PersonalInput>();
  const [busy, setBusy] = useState(false);
  const inFlight = useRef(false);
  const [resumptions, setResumptions] = useState<Record<string, PersonalResumeRunInput>>({});
  const resumes = useRef(new Map<string, PersonalResumeRunInput>());
  const retries = useRef(new Map<string, PersonalRetryInput>());
  const send = useAtomSet(sendPersonalMessage, { mode: "promiseExit" });
  const retry = useAtomSet(retryPersonalInput, { mode: "promiseExit" });
  const requestApproval = useAtomSet(requestPersonalApproval, { mode: "promiseExit" });
  const respondApproval = useAtomSet(respondPersonalApproval, { mode: "promiseExit" });
  const resume = useAtomSet(resumePersonalRun, { mode: "promiseExit" });
  const startTask = useAtomSet(startPersonalTask, { mode: "promiseExit" });
  const applySignal = useAtomSet(applyPersonalSignal, { mode: "promiseExit" });
  const deliver = useAtomSet(sendPersonalGoalMessage, { mode: "promiseExit" });
  const invalidate = useAtomSet(invalidateQueries);
  const reactivityKeys = contextQueryKeys("/personal");
  const available =
    context?.revision !== undefined && !!context.personalState && !context.projectionError;

  const report = (cause: Cause.Cause<unknown>) => {
    const failure = Cause.squash(cause);
    setError(failure instanceof Error ? failure.message : String(failure));
    const typed = Cause.findError(cause);
    const rejected =
      typed._tag === "Success" &&
      Schema.is(ApplicationError)(typed.success) &&
      ["conflict", "invalid-input"].includes(typed.success.kind);
    if (rejected) invalidate(reactivityKeys);
    return rejected;
  };
  async function submit() {
    if (!available || context?.revision === undefined || inFlight.current || !text.trim()) return;
    const input = pending ?? {
      requestId: crypto.randomUUID(),
      causationId: crypto.randomUUID(),
      expectedRevision: context.revision,
      text: text.trim(),
    };
    setPending(input);
    inFlight.current = true;
    setBusy(true);
    setError("");
    try {
      const result = await send({ payload: input, reactivityKeys });
      if (Exit.isSuccess(result)) {
        setPending(undefined);
        setText("");
      } else if (report(result.cause)) setPending(undefined);
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  }
  async function retryRun(inputRequestId: string) {
    if (!available || context?.revision === undefined || inFlight.current) return;
    const input = retries.current.get(inputRequestId) ?? {
      requestId: crypto.randomUUID(),
      inputRequestId,
      expectedRevision: context.revision,
    };
    retries.current.set(inputRequestId, input);
    inFlight.current = true;
    setBusy(true);
    setError("");
    try {
      const result = await retry({ payload: input, reactivityKeys });
      if (Exit.isSuccess(result) || report(result.cause)) retries.current.delete(inputRequestId);
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  }
  async function resumeRun(run: ContextView) {
    if (
      !available ||
      context?.revision === undefined ||
      run.revision === undefined ||
      inFlight.current
    )
      return;
    const retained = run.state.resumptions?.find((item) => item.status === "pending")?.input;
    const input = resumes.current.get(run.path) ?? {
      requestId: retained?.requestId ?? crypto.randomUUID(),
      causationId: retained?.causationId ?? crypto.randomUUID(),
      expectedRevision: context.revision,
      runPath: run.path,
      runRevision: retained?.expectedRevision ?? run.revision,
    };
    resumes.current.set(run.path, input);
    setResumptions(Object.fromEntries(resumes.current));
    inFlight.current = true;
    setBusy(true);
    setError("");
    try {
      const result = await resume({
        payload: input,
        reactivityKeys: [...reactivityKeys, ...contextQueryKeys(run.path)],
      });
      if (Exit.isSuccess(result) || report(result.cause)) {
        resumes.current.delete(run.path);
        setResumptions(Object.fromEntries(resumes.current));
      }
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  }
  async function reconcile(item: PersonalOutboxItem) {
    if (
      !available ||
      context?.revision === undefined ||
      inFlight.current ||
      item.status !== "unknown"
    )
      return;
    inFlight.current = true;
    setBusy(true);
    setError("");
    try {
      const input = item.input;
      const common = {
        requestId: input.requestId,
        causationId: input.causationId,
        expectedRevision: context.revision,
      };
      const result = await Match.value(input).pipe(
        Match.when({ operation: "requestApproval" }, (command) =>
          requestApproval({
            payload: {
              ...common,
              approvalId: command.approvalId,
              approvalsRevision: command.expectedRevision,
              contextPath: command.contextPath,
              contextRevision: command.contextRevision,
            },
            reactivityKeys,
          }),
        ),
        Match.when({ operation: "respondApproval" }, (command) =>
          respondApproval({
            payload: {
              ...common,
              approvalId: command.approvalId,
              approvalsRevision: command.expectedRevision,
              response: command.response,
            },
            reactivityKeys,
          }),
        ),
        Match.when({ operation: "resumeRun" }, (command) =>
          resume({
            payload: { ...common, runPath: command.target, runRevision: command.expectedRevision },
            reactivityKeys,
          }),
        ),
        Match.when({ operation: "startTask" }, (command) =>
          startTask({
            payload: { ...common, agent: command.agent, task: command.task },
            reactivityKeys,
          }),
        ),
        Match.orElse((command) =>
          "text" in command
            ? deliver({
                payload: {
                  ...common,
                  goalSlug: command.target.slice("/goals/".length),
                  goalRevision: command.expectedRevision,
                  text: command.text,
                },
                reactivityKeys,
              })
            : applySignal({
                payload: {
                  ...common,
                  operation: command.operation,
                  signalSlug: command.target.slice("/signals/".length),
                  signalRevision: command.expectedRevision,
                  definition: command.definition,
                  active: command.active,
                },
                reactivityKeys,
              }),
        ),
      );
      if (Exit.isFailure(result)) report(result.cause);
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  }
  return {
    text,
    setText,
    error,
    busy,
    pending,
    available,
    submit,
    retryRun,
    resumeRun,
    resumptions,
    reconcile,
  };
}
export type PersonalCommands = ReturnType<typeof usePersonalCommands>;
