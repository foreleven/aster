import { Type, rejectedToolResult, type AgentMessage, type AgentTool } from "@aster/agent";
import { Data, Effect, Schema } from "effect";
import type { AgentCallbackInvoker } from "@aster/agent";
import { GoalReasoningError } from "./errors.js";
import { contextSize } from "./history.js";
import { GoalPlan, StoredGoalPlan } from "./plan.js";
import type { GoalReasoningInput } from "./reasoner.js";
import { GoalSignalId } from "./tasks.js";
import { goalSignalIdParameter, toolOutput } from "./agent-tools.js";

const finishTurnParameters = Type.Object({
  disposition: Type.Union([
    Type.Literal("advance"),
    Type.Literal("no_change"),
    Type.Literal("ignored"),
  ]),
  progress: Type.String({ maxLength: 6000 }),
  nextStep: Type.Union([
    Type.Object({
      _tag: Type.Literal("Continue"),
      objective: Type.String({ minLength: 1 }),
      previousResultId: Type.String({ minLength: 1 }),
    }),
    Type.Object({
      _tag: Type.Literal("WaitForInput"),
      questions: Type.Array(Type.String({ minLength: 1 }), { minItems: 1 }),
    }),
    Type.Object({
      _tag: Type.Literal("WaitForEvent"),
      references: Type.Array(Type.String({ minLength: 1 }), { minItems: 1 }),
    }),
    Type.Object({
      _tag: Type.Literal("Complete"),
      evidence: Type.Array(Type.String({ minLength: 1 }), { minItems: 1 }),
    }),
  ]),
  evidence: Type.Array(Type.String()),
  signalChanges: Type.Array(
    Type.Union([
      ...(["signal_create", "signal_update"] as const).map((operation) =>
        Type.Object({
          operation: Type.Literal(operation),
          id: goalSignalIdParameter,
          ...(operation === "signal_update" ? { revision: Type.Integer() } : {}),
          definition: Type.Object({
            taskId: Type.Optional(Type.Union([Type.String(), Type.Null()])),
            when: Type.Optional(Type.String()),
            task: Type.Optional(Type.String()),
            notBefore: Type.Optional(Type.Union([Type.String(), Type.Null()])),
            schedule: Type.Optional(
              Type.Union([
                Type.Object({ type: Type.Literal("once"), at: Type.String() }),
                Type.Object({
                  type: Type.Literal("cron"),
                  expression: Type.String(),
                  timeZone: Type.String(),
                }),
                Type.Null(),
              ]),
            ),
          }),
        }),
      ),
      Type.Object({
        operation: Type.Literal("signal_delete"),
        id: goalSignalIdParameter,
        revision: Type.Integer(),
      }),
    ]),
    { maxItems: 16 },
  ),
  taskChanges: Type.Array(
    Type.Union([
      Type.Object({
        operation: Type.Literal("task_create"),
        id: Type.String(),
        title: Type.String(),
        instructions: Type.String(),
        evidence: Type.Optional(Type.Array(Type.String())),
      }),
      Type.Object({
        operation: Type.Literal("task_update"),
        id: Type.String(),
        revision: Type.Integer(),
        title: Type.Optional(Type.String()),
        instructions: Type.Optional(Type.String()),
        status: Type.Optional(Type.Union([Type.Literal("open"), Type.Literal("completed")])),
        evidence: Type.Optional(Type.Array(Type.String())),
      }),
      Type.Object({
        operation: Type.Literal("task_delete"),
        id: Type.String(),
        revision: Type.Integer(),
      }),
      Type.Object({
        operation: Type.Literal("task_execute"),
        id: Type.String(),
        revision: Type.Integer(),
      }),
    ]),
    { maxItems: 32 },
  ),
});

class InvalidGoalProposal extends Data.TaggedError("InvalidGoalProposal")<{
  readonly message: string;
}> {}

const hasValidSignalIds = Schema.is(Schema.Array(Schema.Struct({ id: GoalSignalId })));
type GoalProposal = Omit<GoalPlan, "version" | "turnId" | "resultId">;

const validateProposal = Effect.fnUntraced(function* (
  input: GoalReasoningInput,
  args: GoalProposal,
) {
  if (!hasValidSignalIds(args.signalChanges ?? []))
    return yield* new InvalidGoalProposal({
      message:
        "Invalid Signal ID: use lowercase letters, digits and hyphens, starting with a letter or digit (for example price-watch or goal--price-watch), not a /signals/ path",
    });

  const changesWork = Boolean(args.taskChanges?.length || args.signalChanges?.length);
  const advancesWork = args.nextStep._tag === "Complete" || args.nextStep._tag === "Continue";
  if (args.disposition !== "advance" && (changesWork || advancesWork))
    return yield* new InvalidGoalProposal({
      message: "Ignored or unchanged evaluations cannot propose mutations or completion",
    });

  const proposal = toolOutput({
    ...args,
    version: 2,
    turnId: input.durable.requestId,
    resultId: input.durable.requestId,
  });
  if (contextSize(proposal.content) > 14000)
    return yield* new InvalidGoalProposal({
      message: "The complete result must fit 14000 UTF-8 bytes; shorten the proposals",
    });
  if (contextSize(args.progress) > 6000)
    return yield* new InvalidGoalProposal({
      message: "Summary must fit 6000 UTF-8 bytes; shorten it",
    });
  if (args.evidence.some((path) => !input.contexts[path]))
    return yield* new InvalidGoalProposal({
      message: "Evidence must reference existing Context paths",
    });
  const invalidTaskEvidence = args.taskChanges?.some(
    (change) =>
      (change.operation === "task_create" || change.operation === "task_update") &&
      change.evidence?.some((path) => !input.contexts[path]),
  );
  if (invalidTaskEvidence)
    return yield* new InvalidGoalProposal({
      message: "Task evidence must reference existing Context paths",
    });
  if (
    args.nextStep._tag === "Complete" &&
    (!input.goal.completionCriteria || !args.evidence.length)
  )
    return yield* new InvalidGoalProposal({
      message: "Goal completion requires criteria and evidence",
    });
  return { ...proposal, terminate: true };
});

export const finishTurnTool = (
  input: GoalReasoningInput,
  invoke: AgentCallbackInvoker,
): AgentTool<typeof finishTurnParameters> => ({
  name: "finish_turn",
  replay: "safe",
  label: "Record evaluation conclusions",
  description:
    "Record conclusions, ordered Task proposals and at most one proposal per Signal. The Goal validates the whole result before applying Tasks or publishing Signal commands. Omitted entries remain unchanged. Signal occurrences wake Goal assessment.",
  parameters: finishTurnParameters,
  // Only expected proposal failures become tool replies; callback defects and
  // interruption retain the owning invocation's failure path.
  execute: (_id, args, signal) =>
    invoke(
      validateProposal(input, args).pipe(
        Effect.catchTag("InvalidGoalProposal", (error) =>
          Effect.succeed(rejectedToolResult(error.message)),
        ),
      ),
      signal,
    ),
});

export const decodeGoalAgentResult = Effect.fnUntraced(function* (
  messages: readonly AgentMessage[],
  durable: GoalReasoningInput["durable"],
) {
  const recovering = durable.reconcile || durable.replayOnly;
  const last = messages.findLast(
    (message) =>
      message.role === "toolResult" &&
      !message.isError &&
      (message.toolName === "finish_turn" || (recovering && message.toolName === "submit_plan")),
  );
  if (last?.role !== "toolResult")
    return yield* new GoalReasoningError({
      operation: "plan",
      outcome: "failed",
      message: "Goal Agent returned no plan",
    });
  return yield* Schema.decodeUnknownEffect(recovering ? StoredGoalPlan : GoalPlan)(
    last.details,
  ).pipe(
    Effect.mapError(
      (cause) =>
        new GoalReasoningError({
          operation: "plan",
          outcome: "failed",
          cause,
          message: "Goal Agent returned an invalid structured result",
        }),
    ),
  );
});
