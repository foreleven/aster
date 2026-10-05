import { Effect } from "effect";
import type { PublicContext } from "@aster/api-contracts";
import type { SignalDefinition } from "../config/schema.js";
import { choice, type SystemOneClient } from "../decisions/system-one.js";
/** One decision-model pass matches every active Context Signal. Timers never enter this path. */
export const makeSystemOneGate =
  (client: SystemOneClient) => (record: PublicContext, signals: readonly SignalDefinition[]) =>
    Effect.gen(function* () {
      if (!signals.length) return [];
      const questions = Object.fromEntries(
        signals.map((signal, index) => [
          `signal_${index}`,
          choice(
            `Does this Context satisfy the Signal condition? ${signal.trigger._tag === "Context" ? signal.trigger.when : "Not a Context Signal"}`,
            {
              yes: "Evidence satisfies the condition; execute its Task.",
              no: "The condition is not satisfied.",
            },
          ),
        ]),
      );
      const response = yield* client.systemOne({
        state: JSON.stringify({ context: record }),
        questions,
      });
      return signals.filter((_, index) => {
        const answer = response.answers[`signal_${index}`];
        return answer?.type === "choice" && answer.choice === "yes";
      });
    });
