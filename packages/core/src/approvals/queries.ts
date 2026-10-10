import { Command } from "@aster/actor";
import { Schema } from "effect";
import { ApprovalEntry } from "./contracts.js";

export class GetApprovals extends Command.Class<GetApprovals>()("GetApprovals", {
  payload: {},
  reply: Schema.Array(ApprovalEntry),
}) {}
