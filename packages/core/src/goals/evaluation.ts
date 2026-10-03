import type { ActorRef } from "@aster/actor";
import { Effect, Schema } from "effect";
import { GoalState } from "./state.js";
import type { FrozenGoalEvaluation } from "./frozen-evaluation.js";
import type { ContextRegistry } from "../context/registry.js";
import type { GoalDefinition } from "../config/schema.js";
import type { GoalCommand } from "./actors.js";
import type { GoalRuntime } from "./runtime.js";
import { completePrefix, contextSize, type GoalHistory } from "./history.js";
import { GoalToolError } from "./tasks.js";
import { GoalOperationError } from "./errors.js";

/** Runs outside the mailbox; history and state commit only through generation-tagged Commands. */
export const evaluateGoal = (options: {
  readonly runtime: GoalRuntime["Service"];
  readonly registry: ContextRegistry["Service"];
  readonly definition: GoalDefinition;
  readonly history: GoalHistory;
  readonly self: ActorRef<GoalCommand>;
  readonly generation: string;
  readonly reason: string;
  readonly requestId: string;
  readonly historyThrough?: number;
}) =>
  Effect.gen(function* () {
    const { runtime, registry, definition, history, self, generation, reason } = options;
    const current = () => registry.get(`/goals/${definition.slug}`)!;
    const state = () =>
      current().state as { summary: string; historyThrough: number; agentThrough?: number };
    let active = true;
    // A custom reasoner may retain callbacks. Retired callbacks cannot start new asks,
    // and the mailbox generation check also rejects already queued stale commands.
    const guard = <A, E>(effect: Effect.Effect<A, E>) =>
      Effect.suspend(() => (active ? effect : Effect.interrupt));
    const failure = (cause: {
      readonly message: string;
      readonly outcome?: "failed" | "unknown";
    }) =>
      new GoalOperationError({
        outcome: cause.outcome,
        goal: definition.slug,
        operation: "plan",
        cause,
        message: cause.message,
      });
    return yield* Effect.gen(function* () {
      let frozen = runtime.reasoner.durableSessions
        ? Schema.decodeUnknownSync(GoalState)(current().state).pendingHandoff?.input
        : undefined;
      const target = Math.floor(
        ((runtime.contextTokens ?? 48000) - (runtime.reserveTokens ?? 8192)) / 3,
      );
      const read = () =>
        history.read(definition.slug, {
          after:
            frozen?.historyAfter ??
            (runtime.reasoner.durableSessions
              ? Math.max(state().historyThrough, state().agentThrough ?? 0)
              : state().historyThrough),
          before: frozen
            ? frozen.historyThrough + 1
            : options.historyThrough === undefined
              ? undefined
              : options.historyThrough + 1,
          limit: 200,
        });
      let entries = yield* read();
      while (
        !frozen &&
        entries.length &&
        (contextSize(entries.map((e) => e.message)) > target ||
          entries.at(-1)!.seq < (options.historyThrough ?? (yield* history.count(definition.slug))))
      ) {
        if (!runtime.reasoner.compact)
          return yield* new GoalOperationError({
            goal: definition.slug,
            operation: "plan",
            message: "History needs compaction but no compactor is configured",
          });
        let n = 0,
          size = 0;
        for (const entry of entries) {
          const cost = contextSize(entry.message);
          if (n && size + cost > target) break;
          size += cost;
          n++;
        }
        const messages = entries.map((e) => e.message);
        let boundary = completePrefix(messages, n);
        if (!boundary) {
          for (let i = n + 1; i <= entries.length; i++) {
            boundary = completePrefix(messages, i);
            if (boundary) break;
          }
        }
        if (!boundary)
          return yield* new GoalOperationError({
            goal: definition.slug,
            operation: "plan",
            message: "Cannot compact incomplete tool exchange",
          });
        const prefix = entries.slice(0, boundary);
        const summary = yield* runtime.reasoner.compact(
          state().summary,
          prefix.map((e) => e.message),
        );
        // Advance the history boundary only after the complete summary is durable.
        yield* self.ask<void>((replyTo) => ({
          _tag: "Compacted",
          generation,
          summary,
          through: prefix.at(-1)!.seq,
          replyTo,
        }));
        entries = yield* read();
      }
      const candidate = frozen ?? {
        goal: definition,
        current: registry.project(current()),
        contexts: registry.publicSnapshot(),
        signals: runtime.signals(definition.slug),
        historyAfter: entries[0]
          ? entries[0].seq - 1
          : (options.historyThrough ?? state().historyThrough),
        historyThrough: options.historyThrough ?? entries.at(-1)?.seq ?? state().historyThrough,
      };
      if (runtime.reasoner.durableSessions && !frozen) {
        frozen = yield* self.ask<FrozenGoalEvaluation | undefined>((replyTo) => ({
          _tag: "FreezeEvaluation",
          generation,
          requestId: options.requestId,
          input: candidate,
          replyTo,
        }));
        if (!frozen) return yield* Effect.interrupt;
      }
      const admitted = frozen ?? candidate;
      const plan = yield* runtime.reasoner.plan({
        goal: admitted.goal,
        current: admitted.current,
        contexts: admitted.contexts,
        signals: admitted.signals,
        reason,
        durable: {
          sessionId: definition.slug,
          requestId: options.requestId,
        },
        messages: entries.map((e) => e.message),
        history,
        tool: (request) =>
          guard(
            self
              .ask<{ value?: unknown; error?: string }>((replyTo) => ({
                _tag: "Tool",
                generation,
                request,
                replyTo,
              }))
              .pipe(
                Effect.mapError(
                  (cause) => new GoalToolError({ message: cause.message, outcome: "unknown" }),
                ),
                Effect.flatMap((result) =>
                  result.error
                    ? Effect.fail(new GoalToolError({ message: result.error }))
                    : Effect.succeed(result.value),
                ),
              ),
          ),
        onMessage: runtime.reasoner.durableSessions
          ? undefined
          : (message) =>
              guard(
                self
                  .ask<void>((replyTo) => ({
                    _tag: "Transcript",
                    generation,
                    message,
                    replyTo,
                  }))
                  .pipe(Effect.mapError(failure)),
              ),
      });
      return {
        plan,
        through:
          options.historyThrough ??
          entries.at(-1)?.seq ??
          state().agentThrough ??
          state().historyThrough,
      };
    }).pipe(
      Effect.mapError((cause) => (cause instanceof GoalOperationError ? cause : failure(cause))),
      Effect.ensuring(
        Effect.sync(() => {
          active = false;
        }),
      ),
    );
  });
