import {
  ExternalAgentError,
  type ExternalAgent,
  type PreparedTask,
  type ExecutionSession,
  type ExecutionStatus,
  type InputRequest,
  type ApprovalResponse,
} from "@aster/core";
import { Effect } from "effect";

/** Promise drivers belong to infrastructure; the domain sees only execution Effects. */
export interface ExternalAgentDriver {
  readonly capabilities: string;
  readonly executorPrompt?: string;
  submit(task: PreparedTask, signal: AbortSignal): Promise<ExecutionSession>;
  status(session: ExecutionSession, signal: AbortSignal): Promise<ExecutionStatus>;
  resume(session: ExecutionSession, signal: AbortSignal): Promise<ExecutionSession>;
  wait(session: ExecutionSession, signal: AbortSignal): Promise<ExecutionStatus>;
  respond(
    session: ExecutionSession,
    request: InputRequest,
    response: ApprovalResponse,
    signal: AbortSignal,
  ): Promise<void>;
  close?(): Promise<void>;
}

export interface ManagedExternalAgent extends ExternalAgent {
  /** Release belongs to the infrastructure scope, after domain Actors stop. */
  close(): Effect.Effect<void>;
}

export const adaptExternalAgent = (driver: ExternalAgentDriver): ManagedExternalAgent => {
  const operation = <A>(
    name: ExternalAgentError["operation"],
    run: (signal: AbortSignal) => Promise<A>,
  ) =>
    Effect.tryPromise({
      try: run,
      catch: (cause) =>
        new ExternalAgentError({
          operation: name,
          cause,
          message: cause instanceof Error ? cause.message : String(cause),
        }),
    });
  return {
    capabilities: driver.capabilities,
    ...(driver.executorPrompt === undefined ? {} : { executorPrompt: driver.executorPrompt }),
    submit: (task) => operation("submit", (signal) => driver.submit(task, signal)),
    status: (session) => operation("status", (signal) => driver.status(session, signal)),
    resume: (session) => operation("resume", (signal) => driver.resume(session, signal)),
    wait: (session) => operation("wait", (signal) => driver.wait(session, signal)),
    respond: (session, request, response) =>
      operation("respond", (signal) => driver.respond(session, request, response, signal)),
    close: () => (driver.close ? Effect.promise(() => driver.close!()) : Effect.void),
  };
};
