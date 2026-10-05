import { Schema } from "effect";
import { contextView } from "../context/view.js";
import { publicBusinessMessage } from "../reasoning/public-messages.js";
import { PreparedTask, ApprovalResponse, ExecutionResumption } from "@aster/api-contracts";
import { PublicInputRequest } from "../approvals/view.js";
const optionalString = Schema.optional(Schema.String);
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
export const delegationView = contextView({
  matches: (path) => /^\/delegations\/[^/]+$/.test(path),
  state: DelegationView,
  projectMessage: publicBusinessMessage,
});
