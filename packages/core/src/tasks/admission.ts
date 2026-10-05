import { createHash } from "node:crypto";
import {
  TaskDeliveryInput,
  type CausalChain,
  type PersonalTaskProposal,
} from "@aster/api-contracts";

export const personalTaskPath = (requestId: string) =>
  `/runs/personal--${createHash("sha256").update(requestId).digest("hex")}`;
export const personalTaskIntent = (
  proposal: PersonalTaskProposal,
  identity: { requestId: string; causationId: string; createdAt: string; causal?: CausalChain },
): TaskDeliveryInput => ({
  ...proposal,
  ...identity,
  operation: "startTask",
  source: "/personal",
  target: personalTaskPath(identity.requestId),
  expectedRevision: 0,
});

export const taskPath = (source: string, requestId: string) =>
  source === "/personal"
    ? personalTaskPath(requestId)
    : `/runs/goal--${createHash("sha256")
        .update(JSON.stringify([source, requestId]))
        .digest("hex")}`;
