import { SignalDetectionError } from "./errors.js";
import type { ActorRef } from "@aster/actor";
import type { PublicContext as ContextRecord } from "@aster/api-contracts";
import { choice, type DecisionError } from "../decisions/system-one.js";
import { Effect } from "effect";
import type { SignalDefinition } from "../config/schema.js";
import type { SignalRootCommand } from "./actors.js";
import type { Task } from "../tasks/model.js";
import type { SystemOneClient } from "../decisions/system-one.js";

export type ExecutionGate = (
  definition: SignalDefinition,
  context: ContextRecord,
  task: Task,
) => Effect.Effect<boolean, DecisionError>;

export type SignalExtractor = (
  sourcePath: string,
  candidates: readonly SignalDefinition[],
  snapshot: Readonly<Record<string, ContextRecord>>,
) => Effect.Effect<readonly string[], SignalDetectionError>;

export const makeSystemOneGate =
  (client: SystemOneClient) =>
  (
    context: ContextRecord,
    signals: ReadonlyArray<SignalDefinition>,
  ): Effect.Effect<ReadonlyArray<SignalDefinition>, DecisionError> =>
    Effect.gen(function* () {
      if (signals.length === 0) return [];
      const questions = Object.fromEntries(
        signals.map((signal, index) => [
          `signal_${index}`,
          choice(
            `Should this Context be evaluated more deeply for this Signal? Condition: ${signal.when}`,
            {
              yes: "The Context could satisfy the condition; send it to the extraction Agent.",
              no: "The Context is unrelated to the condition.",
            },
          ),
        ]),
      );
      const response = yield* client.systemOne({
        state: JSON.stringify({ context }),
        questions,
      });
      return signals.filter((_, index) => {
        const answer = response.answers[`signal_${index}`];
        return answer?.type === "choice" && answer.choice === "yes";
      });
    });

export const detectSignals = (
  sourcePath: string,
  snapshot: Readonly<Record<string, ContextRecord>>,
  definitions: ReadonlyArray<SignalDefinition>,
  gate: ReturnType<typeof makeSystemOneGate>,
  signalRoot: ActorRef<SignalRootCommand>,
  extractor: SignalExtractor,
  onTriggered?: (slugs: readonly string[]) => void,
) =>
  Effect.gen(function* () {
    const record = snapshot[sourcePath];
    if (!record)
      return yield* new SignalDetectionError({
        path: sourcePath,
        message: `Missing Context: ${sourcePath}`,
      });
    const candidates = yield* gate(record, definitions);
    const triggered = yield* extractor(sourcePath, candidates, snapshot);
    onTriggered?.(triggered);
    for (const slug of triggered)
      yield* signalRoot.tell({ _tag: "Trigger", slug, sourceContext: record });
  }).pipe(
    Effect.mapError((cause) =>
      cause instanceof SignalDetectionError
        ? cause
        : new SignalDetectionError({ path: sourcePath, cause, message: cause.message }),
    ),
  );
