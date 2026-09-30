import type { Effect } from "effect";
import type { GoalOperationError, GoalReasoningError } from "./errors.js";
import type { GoalToolError } from "./tasks.js";
import type { AgentMessage } from "@aster/agent";
import type { ContextRecord } from "../context/model.js";
import type { SignalDefinition, GoalDefinition } from "../config/schema.js";
import type { GoalPlan } from "./plan.js";
import type { GoalHistory } from "./history.js";
import type { GoalToolRequest } from "./tasks.js";

/** Reasoning and callbacks share the caller fiber environment; only the Agent adapter bridges SDK Promises. */
export interface GoalReasoner {
  plan(input: {
    readonly goal: GoalDefinition;
    readonly current: ContextRecord;
    readonly contexts: Readonly<Record<string, ContextRecord>>;
    readonly signals: readonly SignalDefinition[];
    readonly reason: string;
    readonly messages?: readonly AgentMessage[];
    readonly history?: GoalHistory;
    readonly tool?: (request: GoalToolRequest) => Effect.Effect<unknown, GoalToolError>;
    readonly onMessage?: (message: AgentMessage) => Effect.Effect<void, GoalOperationError>;
  }): Effect.Effect<GoalPlan, GoalReasoningError>;
  compact?(
    summary: string,
    messages: readonly AgentMessage[],
  ): Effect.Effect<string, GoalReasoningError>;
}
