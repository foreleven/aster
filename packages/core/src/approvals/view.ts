import { Schema } from "effect";
import { contextView } from "../context/definition.js";
import { publicBusinessMessage } from "../reasoning/public-messages.js";
import { ApprovalEntry, InputRequest } from "@aster/api-contracts";
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
export const approvalView = contextView({
  matches: (path) => path === "/approvals",
  state: Schema.Struct({ entries: Schema.Array(PublicApprovalEntry) }),
  projectMessage: publicBusinessMessage,
});
