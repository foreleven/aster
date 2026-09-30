import { Schema } from "effect";
import { ApprovalResponse, ExecutionSession, InputRequest, Task } from "../tasks/model.js";

export const DelegationRequest = Schema.Struct({
  runPath: Schema.String,
  agent: Schema.String,
  task: Task,
});
export type DelegationRequest = typeof DelegationRequest.Type;

const ResponseRecord = Schema.Struct({
  request: InputRequest,
  response: ApprovalResponse,
  status: Schema.Literals(["received", "sending", "sent", "uncertain"]),
});
const fields = {
  request: DelegationRequest,
  replyPath: Schema.optional(Schema.String),
  requests: Schema.Record(Schema.String, InputRequest),
  responses: Schema.Record(Schema.String, ResponseRecord),
  result: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
};

/** Unknown submission outcomes may have no session; running/completed work must have one. */
export const DelegationState = Schema.Union([
  Schema.Struct({
    ...fields,
    status: Schema.Literal("submitting"),
    session: Schema.optional(ExecutionSession),
  }),
  Schema.Struct({
    ...fields,
    status: Schema.Literal("uncertain"),
    session: Schema.optional(ExecutionSession),
    error: Schema.String,
  }),
  Schema.Struct({
    ...fields,
    status: Schema.Literals(["running", "waiting_input"]),
    session: ExecutionSession,
  }),
  Schema.Struct({
    ...fields,
    status: Schema.Literals(["failed", "cancelled", "unknown"]),
    session: ExecutionSession,
    error: Schema.String,
  }),
  Schema.Struct({
    ...fields,
    status: Schema.Literal("completed"),
    session: ExecutionSession,
    result: Schema.String,
  }),
]);
export type DelegationState = typeof DelegationState.Type;
