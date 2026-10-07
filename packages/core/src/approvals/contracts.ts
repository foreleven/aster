import { Schema } from "effect";

export const InputRequest = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literals(["approval", "input"]),
  prompt: Schema.String,
  options: Schema.optional(Schema.Array(Schema.String)),
  questions: Schema.optional(
    Schema.Array(
      Schema.Struct({
        id: Schema.String,
        prompt: Schema.String,
        options: Schema.optional(Schema.Array(Schema.String)),
        // Providers explicitly opt into custom text or restrict a question to one selection.
        allowOther: Schema.optional(Schema.Boolean),
        multiple: Schema.optional(Schema.Boolean),
      }),
    ),
  ),
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
});
export type InputRequest = typeof InputRequest.Type;
export const ApprovalResponse = Schema.Struct({
  decision: Schema.optional(Schema.Literals(["approve", "reject"])),
  text: Schema.optional(Schema.String),
  answers: Schema.optional(Schema.Record(Schema.String, Schema.Array(Schema.String))),
});
export type ApprovalResponse = typeof ApprovalResponse.Type;

export const ApprovalEntry = Schema.Struct({
  id: Schema.String,
  target: Schema.String,
  contextPath: Schema.String,
  kind: Schema.Literals(["confirmation", "approval", "input"]),
  request: InputRequest,
  status: Schema.Literals(["pending", "resolved", "acknowledged", "revoked"]),
  response: Schema.optional(ApprovalResponse),
});
export type ApprovalEntry = typeof ApprovalEntry.Type;

// Provider metadata and execution handles are intentionally absent from these schemas.
export const PublicInputRequest = Schema.Struct({
  id: InputRequest.fields.id,
  kind: InputRequest.fields.kind,
  prompt: InputRequest.fields.prompt,
  options: InputRequest.fields.options,
  questions: InputRequest.fields.questions,
});
export const PublicApprovalEntry = Schema.Struct({
  id: ApprovalEntry.fields.id,
  contextPath: ApprovalEntry.fields.contextPath,
  kind: ApprovalEntry.fields.kind,
  status: ApprovalEntry.fields.status,
  response: ApprovalEntry.fields.response,
  request: PublicInputRequest,
});
export type PublicApprovalEntry = typeof PublicApprovalEntry.Type;
