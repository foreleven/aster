import { Schema } from "effect";
import {
  ApprovalEntry,
  ApprovalResponse,
  InputRequest,
  PreparedTask,
  PersonalMessage,
  PersonalState,
  RunResumption,
  ExecutionResumption,
  WritebackOperation,
} from "@aster/api-contracts";
import { GoalState } from "../goals/state.js";
import { SignalDefinition } from "../config/schema.js";
import { contextView } from "./view.js";

// Provider metadata and execution handles are intentionally absent from these schemas.
export const PublicInputRequest = Schema.Struct({
  id: InputRequest.fields.id,
  kind: InputRequest.fields.kind,
  prompt: InputRequest.fields.prompt,
  options: InputRequest.fields.options,
  questions: InputRequest.fields.questions,
});
export const PublicApprovalEntry = Schema.Struct({
  ...ApprovalEntry.fields,
  request: PublicInputRequest,
});
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
  const personal = Schema.decodeUnknownResult(PersonalMessage)(value);
  if (personal._tag === "Success") return personal.success;
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
const optionalString = Schema.optional(Schema.String);
const RunView = Schema.Struct({
  writeback: Schema.optional(WritebackOperation),
  status: optionalString,
  signalSlug: optionalString,
  sourcePath: optionalString,
  task: Schema.optional(PreparedTask),
  definition: Schema.optional(SignalDefinition),
  outcomeText: optionalString,
  approvals: Schema.optional(Schema.Array(Schema.String)),
  goalTask: Schema.optional(
    Schema.Struct({ goalPath: Schema.String, taskId: Schema.String, revision: Schema.Number }),
  ),
  resumptions: Schema.optional(Schema.Array(RunResumption)),
});
const DelegationView = Schema.Struct({
  status: optionalString,
  result: optionalString,
  error: optionalString,
  request: Schema.optional(
    Schema.Struct({ runPath: Schema.String, agent: Schema.String, task: PreparedTask }),
  ),
  requests: Schema.optional(Schema.Record(Schema.String, PublicInputRequest)),
  responses: Schema.optional(
    Schema.Record(
      Schema.String,
      Schema.Struct({
        request: PublicInputRequest,
        response: ApprovalResponse,
        status: Schema.String,
      }),
    ),
  ),
  resumptions: Schema.optional(Schema.Array(ExecutionResumption)),
});
const SignalView = Schema.Struct({
  action: SignalDefinition.fields.action,
  slug: optionalString,
  when: optionalString,
  task: optionalString,
  agent: optionalString,
  mode: optionalString,
  goal: optionalString,
  taskId: optionalString,
  owner: optionalString,
  active: Schema.optional(Schema.Boolean),
  deleted: Schema.optional(Schema.Boolean),
  revision: Schema.optional(Schema.Number),
  nextDue: Schema.optional(Schema.Number),
  schedule: SignalDefinition.fields.schedule,
  notBefore: SignalDefinition.fields.notBefore,
  occurrences: Schema.optional(
    Schema.Array(
      Schema.Struct({
        id: Schema.String,
        text: Schema.String,
        delivered: Schema.Boolean,
        source: Schema.Struct({ path: Schema.String }),
      }),
    ),
  ),
});
const GoalView = Schema.Struct({
  nextStep: GoalState.fields.nextStep,
  completionOrigin: GoalState.fields.completionOrigin,
  deactivation: GoalState.fields.deactivation,
  signalOutbox: Schema.optional(
    Schema.Array(
      Schema.Struct({
        input: Schema.Struct({
          requestId: Schema.String,
          evaluationId: Schema.String,
          target: Schema.String,
          operation: Schema.String,
        }),
        status: Schema.String,
        attempts: Schema.Number,
        error: Schema.optional(Schema.String),
        receipt: Schema.optional(
          Schema.Struct({ requestId: Schema.String, revision: Schema.Number }),
        ),
      }),
    ),
  ),
  evaluations: GoalState.fields.evaluations,
  title: GoalState.fields.title,
  status: optionalString,
  description: optionalString,
  completionCriteria: GoalState.fields.completionCriteria,
  summary: optionalString,
  progress: optionalString,
  lastError: optionalString,
  tasks: Schema.optional(GoalState.fields.tasks),
  historyCount: Schema.optional(Schema.Number),
  deliveries: GoalState.fields.deliveries,
});
export const coreContextViews = [
  contextView({
    matches: (path) => path === "/personal",
    state: PersonalState,
    message: PersonalMessage,
  }),
  contextView({
    matches: (path) => /^\/goals\/[^/]+$/.test(path),
    state: GoalView,
    projectMessage: publicBusinessMessage,
  }),
  contextView({
    matches: (path) => /^\/(?:runs\/[^/]+|(?:goals|signals)\/[^/]+\/runs\/[^/]+)$/.test(path),
    state: RunView,
    projectMessage: publicBusinessMessage,
  }),
  contextView({
    matches: (path) => /^\/delegations\/[^/]+$/.test(path),
    state: DelegationView,
    projectMessage: publicBusinessMessage,
  }),
  contextView({
    matches: (path) => /^\/signals\/[^/]+$/.test(path),
    state: SignalView,
    projectMessage: publicBusinessMessage,
  }),
  contextView({
    matches: (path) => path === "/approvals",
    state: Schema.Struct({ entries: Schema.Array(PublicApprovalEntry) }),
    projectMessage: publicBusinessMessage,
  }),
  contextView({
    matches: (path) => ["/goals", "/signals", "/runs"].includes(path),
    state: Schema.Struct({}),
  }),
];
