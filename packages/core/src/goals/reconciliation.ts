import type { AgentConversations } from "@aster/agent/harness";
import { Effect } from "effect";
import type { DurableContext } from "../context/store.js";
import { taskOperationReceipt } from "../tasks/operations.js";
import { signalOperationReceipt } from "../signals/operations.js";
import { receivedGoalMessageReceipt } from "./messages.js";
import type { CommandReceipt } from "../operations.js";

/** Positive owner receipts resolve uncertainty; absence never proves rejection or permits resubmission. */
export const goalOperationReceipt: (
  registry: DurableContext["Service"],
  messages: AgentConversations["Service"],
  owner: string,
  operationId: string,
) => Effect.Effect<{ readonly receipt: CommandReceipt; readonly taskPath?: string } | undefined> =
  Effect.fn("Goals.operationReceipt")(function* (
    registry: DurableContext["Service"],
    messages: AgentConversations["Service"],
    owner: string,
    operationId: string,
  ) {
    const task = yield* taskOperationReceipt(registry, owner, operationId);
    if (task) return task;
    const signal = yield* signalOperationReceipt(registry, messages, owner, operationId);
    if (signal) return { receipt: signal };
    const message = yield* receivedGoalMessageReceipt(registry, messages, owner, operationId);
    return message ? { receipt: message } : undefined;
  });
