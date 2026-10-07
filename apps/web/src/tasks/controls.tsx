import { Button } from "../components/ui/button";
import { ErrorNotice } from "../components/feedback";
import { useState } from "react";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import { Cause, Exit, Schema } from "effect";
import { ApplicationError, type TaskRecoveryInput } from "@aster/core/contracts";
import { contextQueryKeys } from "@aster/api";
import { checkTask, retryTask } from "../api/client";
import type { ContextView } from "../contexts/model";
const pendingRequests = Atom.make<
  Record<string, { input: TaskRecoveryInput; action: "check" | "retry" }>
>({}).pipe(Atom.keepAlive);
export function TaskControls({ context }: { context: ContextView }) {
  const pending = useAtomValue(pendingRequests);
  const setPending = useAtomSet(pendingRequests);
  const check = useAtomSet(checkTask, { mode: "promiseExit" });
  const retry = useAtomSet(retryTask, { mode: "promiseExit" });
  const checking = useAtomValue(checkTask).waiting;
  const retrying = useAtomValue(retryTask).waiting;
  const [error, setError] = useState("");
  async function submit(action: "check" | "retry") {
    if (checking || retrying || context.revision === undefined) return;
    const request = pending[context.path] ?? {
      action,
      input: {
        requestId: crypto.randomUUID(),
        target: context.path,
        expectedRevision: context.revision,
      },
    };
    setPending((previous) => ({ ...previous, [context.path]: request }));
    setError("");
    const result = await (request.action === "check" ? check : retry)({
      payload: request.input,
      reactivityKeys: contextQueryKeys(context.path),
    });
    if (Exit.isFailure(result)) {
      const failure = Cause.squash(result.cause);
      setError(failure instanceof Error ? failure.message : String(failure));
      if (!Schema.is(ApplicationError)(failure) || failure.kind === "unavailable") return;
    }
    setPending((previous) =>
      Object.fromEntries(Object.entries(previous).filter(([key]) => key !== context.path)),
    );
  }
  return (
    <section className="flex flex-wrap items-center gap-3" aria-label="Task controls">
      {(["failed", "uncertain"].includes(context.state.status ?? "") || pending[context.path]) && (
        <>
          <Button
            variant="outline"
            disabled={checking || retrying}
            onClick={() => void submit("check")}
          >
            {pending[context.path] ? "Check request receipt" : "Check original execution"}
          </Button>
          {!pending[context.path] && context.state.status === "failed" && (
            <Button
              variant="outline"
              disabled={checking || retrying}
              onClick={() => void submit("retry")}
            >
              Retry failed execution
            </Button>
          )}
          <p>
            Checking does not submit the work again. Retry is available only for confirmed failures.
          </p>
        </>
      )}
      <ErrorNotice error={error} />
    </section>
  );
}
