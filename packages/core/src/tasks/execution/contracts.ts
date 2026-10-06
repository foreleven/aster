import { ApprovalResponse, InputRequest, PreparedTask } from "@aster/api-contracts";
export { ApprovalResponse, InputRequest } from "@aster/api-contracts";
import { Context, Data, Effect, Option, Schema } from "effect";

export { PreparedTask, Task } from "@aster/api-contracts";
export const TaskResult = Schema.Struct({ text: Schema.String });
export type TaskResult = typeof TaskResult.Type;
export const ExecutionSession = Schema.Struct({
  sessionId: Schema.String,
  runId: Schema.optional(Schema.String),
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
});
export type ExecutionSession = typeof ExecutionSession.Type;
export const ExecutionStatus = Schema.Struct({
  state: Schema.Literals([
    "running",
    "completed",
    "waiting_input",
    "failed",
    "cancelled",
    "unknown",
  ]),
  result: Schema.optional(TaskResult),
  requests: Schema.optional(Schema.Array(InputRequest)),
  error: Schema.optional(Schema.String),
  resumable: Schema.optional(Schema.Boolean),
});
export type ExecutionStatus = typeof ExecutionStatus.Type;
export interface ExecutionSubmission {
  /** Stable owner identity for adapters that support durable admission dedupe. */
  readonly requestId: string;
}
/** Domain execution only. Infrastructure Layers own adapter acquisition and shutdown. */
export interface ExternalAgent {
  readonly capabilities: string;
  /** Overrides the built-in prompt when supplied; included in the Task for confirmation. */
  readonly executorPrompt?: string;
  submit(
    task: PreparedTask,
    submission?: ExecutionSubmission,
  ): Effect.Effect<ExecutionSession, ExternalAgentError>;
  /** Read-only admission reconciliation. Missing does not authorize another submission. */
  lookupSubmission?(
    task: PreparedTask,
    submission: ExecutionSubmission,
  ): Effect.Effect<Option.Option<ExecutionSession>, ExternalAgentError>;
  followUp(
    session: ExecutionSession,
    input: { readonly requestId: string; readonly text: string },
  ): Effect.Effect<ExecutionSession, ExternalAgentError>;
  status(session: ExecutionSession): Effect.Effect<ExecutionStatus, ExternalAgentError>;
  resume(session: ExecutionSession): Effect.Effect<ExecutionSession, ExternalAgentError>;
  wait(session: ExecutionSession): Effect.Effect<ExecutionStatus, ExternalAgentError>;
  cancel?(session: ExecutionSession): Effect.Effect<boolean, ExternalAgentError>;
  respond(
    session: ExecutionSession,
    request: InputRequest,
    response: ApprovalResponse,
  ): Effect.Effect<void, ExternalAgentError>;
}
export class ExternalAgents extends Context.Service<
  ExternalAgents,
  Readonly<Record<string, ExternalAgent>>
>()("tasks/ExternalAgents") {}

/** Transport/protocol failures do not imply that an external operation had no side effect. */
export class ExternalAgentError extends Data.TaggedError("ExternalAgentError")<{
  readonly operation:
    "submit" | "status" | "resume" | "wait" | "respond" | "lookup" | "followUp" | "cancel";
  readonly message: string;
  readonly cause?: unknown;
  readonly outcome?: "rejected" | "unknown";
}> {}
