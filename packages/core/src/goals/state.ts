import { GoalRequestRecord } from "./protocol.js";
import { GoalNextStep } from "@aster/api-contracts";
import { StoredGoalInput } from "./inputs.js";
import { Schema } from "effect";
import { ReceivedGoalIntent } from "./intent.js";
import { BusinessNotification, CausalChain, GoalDelivery } from "@aster/api-contracts";
import { GoalTask } from "./tasks.js";
import { GoalTitle } from "../config/schema.js";
import { FrozenGoalEvaluation } from "./frozen-evaluation.js";
import { GoalEvaluationRecord, validEvaluationJournal } from "./evaluation-record.js";
import { GoalSignalOperation } from "../signals/goal-command.js";

export const GoalHandoff = Schema.Struct({
  input: Schema.optional(FrozenGoalEvaluation),
  causal: Schema.optional(CausalChain),
  requestId: Schema.NonEmptyString,
  reason: Schema.String,
  through: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}).check(
  Schema.makeFilter(
    (handoff) => !handoff.input || handoff.input.historyThrough === handoff.through,
    { expected: "Frozen evaluation history matches its admitted handoff boundary" },
  ),
);
export type GoalHandoff = typeof GoalHandoff.Type;

export const GoalState = Schema.Struct({
  requests: Schema.optional(Schema.Array(GoalRequestRecord)),
  activated: Schema.optional(Schema.Boolean),
  nextStep: Schema.optional(GoalNextStep),
  completionOrigin: Schema.optional(Schema.Literals(["user", "criteria"])),
  retryTurnId: Schema.optional(Schema.String),
  deactivation: Schema.optional(
    Schema.Struct({
      requestId: Schema.NonEmptyString,
      status: Schema.Literals(["pending", "sending", "delivered", "unknown"]),
      error: Schema.optional(Schema.String),
    }),
  ),
  inputs: Schema.optional(Schema.Array(StoredGoalInput)),
  signalOutbox: Schema.optional(Schema.Array(GoalSignalOperation)),
  evaluations: Schema.optional(Schema.Array(GoalEvaluationRecord)),
  businessOutbox: Schema.optional(Schema.Array(BusinessNotification)),
  agentAdmissions: Schema.optional(
    Schema.Array(
      Schema.Struct({
        rootRequestId: CausalChain.fields.rootRequestId,
        count: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 8 })),
      }),
    ),
  ),
  causal: Schema.optional(CausalChain),
  slug: Schema.String,
  title: Schema.optional(GoalTitle),
  status: Schema.Literals(["active", "completed"]),
  description: Schema.String,
  completionCriteria: Schema.optional(Schema.String),
  summary: Schema.String,
  progress: Schema.String,
  lastError: Schema.optional(Schema.String),
  tasks: Schema.Array(GoalTask),
  historyThrough: Schema.Number,
  /** Last GoalHistory sequence admitted to the durable Pi conversation. */
  agentThrough: Schema.optional(Schema.Number),
  historyCount: Schema.Number,
  pendingEvaluation: Schema.Boolean,
  /** Stable handoff identity retained until the corresponding evaluation is applied. */
  pendingRequestId: Schema.optional(Schema.String),
  /** Immutable admitted history prefix for this request, including across restart. */
  pendingHandoff: Schema.optional(GoalHandoff),
  receivedEvents: Schema.Array(Schema.String),
  intents: Schema.optional(Schema.Array(ReceivedGoalIntent)),
  deliveries: Schema.optional(Schema.Array(GoalDelivery)),
}).check(
  Schema.makeFilter(
    (state) =>
      validEvaluationJournal(state.evaluations ?? []) &&
      new Set(state.requests?.map((item) => item.request.requestId)).size ===
        (state.requests?.length ?? 0) &&
      (state.requests ?? []).every((item) => item.request.requestId === item.receipt.requestId) &&
      (state.inputs ?? []).every(
        (input, index, inputs) =>
          input.goalSlug === state.slug &&
          Number.isFinite(Date.parse(input.receivedAt)) &&
          (index === 0 || inputs[index - 1]!.ordinal < input.ordinal),
      ) &&
      new Set(state.inputs?.map((input) => input.inputId)).size === (state.inputs?.length ?? 0) &&
      (state.evaluations ?? []).every((evaluation) =>
        (evaluation.inputIds ?? []).every((id) =>
          state.inputs?.some((input) => input.inputId === id),
        ),
      ) &&
      (state.signalOutbox ?? []).every(
        (operation) =>
          operation.input.source === `/goals/${state.slug}` &&
          (state.evaluations ?? []).some(
            (evaluation) =>
              evaluation.evaluationId === operation.input.evaluationId &&
              (evaluation.status === "completed" || evaluation.status === "partially_applied"),
          ),
      ) &&
      (state.evaluations ?? []).every((evaluation) => {
        const operations = (state.signalOutbox ?? []).filter(
          (operation) => operation.input.evaluationId === evaluation.evaluationId,
        );
        if (evaluation.status === "partially_applied")
          return (
            operations.length > 0 &&
            operations.some((operation) => operation.status !== "delivered")
          );
        return (
          evaluation.status !== "completed" ||
          operations.every((operation) => operation.status === "delivered")
        );
      }) &&
      new Set(
        state.signalOutbox?.flatMap(
          (operation) => operation.retries?.map((retry) => retry.input.requestId) ?? [],
        ),
      ).size ===
        (state.signalOutbox?.flatMap((operation) => operation.retries ?? []).length ?? 0) &&
      new Set(state.signalOutbox?.map((operation) => operation.input.requestId)).size ===
        (state.signalOutbox?.length ?? 0) &&
      (!state.pendingHandoff?.input || state.pendingHandoff.input.goal.slug === state.slug) &&
      new Set(state.agentAdmissions?.map((entry) => entry.rootRequestId)).size ===
        (state.agentAdmissions?.length ?? 0),
    {
      expected:
        "Valid evaluation journal, Goal-owned frozen input and unique causal admission counters",
    },
  ),
);
export type GoalState = typeof GoalState.Type;

/** Outputs of an admitted evaluation consume one turn of its frozen cause. */
export const goalOutputCause = (state: GoalState): CausalChain | undefined => {
  const admitted = state.pendingHandoff?.causal;
  return admitted
    ? { ...admitted, remainingAgentTurns: Math.max(0, admitted.remainingAgentTurns - 1) }
    : state.causal;
};
