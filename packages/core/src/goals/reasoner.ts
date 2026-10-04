import type { Effect } from "effect";
import type { GoalReasoningError } from "./errors.js";
import type { AgentMessage } from "@aster/agent";
import type { ContextRecord } from "../context/model.js";
import type { SignalDefinition, GoalDefinition } from "../config/schema.js";
import type { StoredGoalPlan } from "./plan.js";
import type { GoalHistory } from "./history.js";

/** Reasoning and callbacks share the caller fiber environment; only the Agent adapter bridges SDK Promises. */
export interface GoalReasoner {
  /** Durable Pi owns the native transcript; GoalHistory receives only business inputs. */
  plan(input: {
    readonly goal: GoalDefinition;
    readonly current: ContextRecord;
    readonly contexts: Readonly<Record<string, ContextRecord>>;
    readonly signals: readonly SignalDefinition[];
    readonly reason: string;
    readonly durable: {
      readonly reconcile?: boolean;
      readonly replayOnly?: boolean;
      readonly sessionId: string;
      readonly requestId: string;
      readonly storageDirectory?: string;
    };
    readonly messages?: readonly AgentMessage[];
    readonly history?: GoalHistory;
  }): Effect.Effect<StoredGoalPlan, GoalReasoningError>;
}

export type GoalReasoningInput = Parameters<GoalReasoner["plan"]>[0];
