import { Schema } from "effect";
import { ApprovalResponse, PreparedTask } from "@aster/api-contracts";
const BusinessEvent = Schema.Struct({
  type: Schema.Literals([
    "Triggered",
    "TaskPrepared",
    "Ready",
    "Delegating",
    "Submitted",
    "Requested",
    "RequestAdmitted",
    "Resolved",
    "Revoked",
    "Acknowledged",
    "ConfirmationRequested",
    "ConfirmationResolved",
    "NotExecutable",
    "PreparationFailed",
    "RecoveryFailed",
    "Error",
    "WaitingInput",
    "Completed",
    "Failed",
    "Cancelled",
    "Uncertain",
    "ApprovalReceived",
    "ResponseDelivered",
    "ResponseUncertain",
    "ResumeRequested",
    "ResumptionChanged",
    "WritebackChanged",
    "assistant",
    "user",
    "error",
    "summary",
  ]),
  text: Schema.optional(Schema.String),
  at: Schema.optional(Schema.String),
  requestId: Schema.optional(Schema.String),
  causationId: Schema.optional(Schema.String),
  sourcePath: Schema.optional(Schema.String),
  contextPath: Schema.optional(Schema.String),
  revision: Schema.optional(Schema.Number),
  contextRevision: Schema.optional(Schema.Number),
  status: Schema.optional(Schema.String),
  approvalId: Schema.optional(Schema.String),
  references: Schema.optional(Schema.Array(Schema.String)),
  response: Schema.optional(ApprovalResponse),
  success: Schema.optional(Schema.Boolean),
  task: Schema.optional(Schema.Union([Schema.String, PreparedTask])),
});
const ConversationMessage = Schema.Struct({
  role: Schema.Literals(["user", "assistant"]),
  content: Schema.Unknown,
  timestamp: Schema.optional(Schema.Number),
});
const Text = Schema.Struct({ type: Schema.Literal("text"), text: Schema.String });
/** Only business text crosses this boundary; tool arguments/results and provider frames stay private. */
export const publicBusinessMessage = (value: unknown): unknown | undefined => {
  const event = Schema.decodeUnknownResult(BusinessEvent)(value);
  if (event._tag === "Success") return event.success;
  const conversation = Schema.decodeUnknownResult(ConversationMessage)(value);
  if (conversation._tag === "Failure") return undefined;
  const { role, content, timestamp } = conversation.success;
  if (typeof content === "string") return { role, content, timestamp };
  const blocks = Schema.decodeUnknownResult(Schema.Array(Schema.Unknown))(content);
  if (blocks._tag === "Failure") return undefined;
  const text = blocks.success.flatMap((block) => {
    const decoded = Schema.decodeUnknownResult(Text)(block);
    return decoded._tag === "Success" ? [decoded.success] : [];
  });
  return text.length ? { role, content: text, timestamp } : undefined;
};
