import { Schema } from "effect";
import { contextView } from "../context/definition.js";
import { ApprovalEvent } from "./state.js";
import { PublicApprovalEntry } from "./contracts.js";
export { PublicApprovalEntry } from "./contracts.js";
export const approvalView = contextView({
  matches: (path) => path === "/approvals",
  state: Schema.Struct({ entries: Schema.Array(PublicApprovalEntry) }),
  message: ApprovalEvent,
});
