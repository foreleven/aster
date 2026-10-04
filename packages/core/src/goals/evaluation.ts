import { Effect } from "effect";
import type { FrozenGoalEvaluation } from "./frozen-evaluation.js";
import type { GoalReasoner } from "./reasoner.js";
import type { GoalHistory } from "./history.js";
import { inputMessage } from "./inputs.js";
import { GoalOperationError } from "./errors.js";

/** Session execution starts only after the mailbox has persisted the complete admission. */
export const evaluateGoal = Effect.fn("Goal.runTurn")(
  function* (options: {
    readonly reasoner: GoalReasoner;
    readonly history: GoalHistory;
    readonly input: FrozenGoalEvaluation;
    readonly reason: string;
    readonly requestId: string;
    readonly reconcile: boolean;
    readonly replayOnly?: boolean;
  }) {
    const { input, reasoner, history } = options;
    // Legacy history remains available through goal_history; new inputs are frozen verbatim.
    const messages = input.inputs
      ? input.inputs.map(inputMessage)
      : (yield* history.read(input.goal.slug, {
          after: input.historyAfter,
          before: input.historyThrough + 1,
          limit: 200,
        })).map((entry) => entry.message);
    const plan = yield* reasoner.plan({
      goal: input.goal,
      current: input.current,
      contexts: input.contexts,
      signals: input.signals,
      reason: options.reason,
      durable: {
        reconcile: options.reconcile,
        replayOnly: options.replayOnly,
        sessionId: input.goal.slug,
        requestId: options.requestId,
      },
      messages,
      history,
    });
    return { plan, through: input.historyThrough };
  },
  (effect, options) =>
    effect.pipe(
      Effect.mapError(
        (cause) =>
          new GoalOperationError({
            goal: options.input.goal.slug,
            operation: "plan",
            message: cause.message,
            cause,
            outcome: "outcome" in cause ? cause.outcome : undefined,
          }),
      ),
    ),
);
