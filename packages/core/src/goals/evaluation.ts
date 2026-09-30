import type { ActorRef } from "@aster/actor";
import { Effect } from "effect";
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
}) =>
  Effect.gen(function* () {
    const { runtime, registry, definition, history, self, generation, reason } = options;
    const current = () => registry.get(`/goals/${definition.slug}`)!;
    const state = () => current().state as { summary: string; historyThrough: number };
    let active = true;
    // A custom reasoner may retain callbacks. Retired callbacks cannot start new asks,
    // and the mailbox generation check also rejects already queued stale commands.
    const guard = <A, E>(effect: Effect.Effect<A, E>) =>
      Effect.suspend(() => (active ? effect : Effect.interrupt));
    const failure = (cause: { readonly message: string }) =>
      new GoalOperationError({
        goal: definition.slug,
        operation: "plan",
        cause,
        message: cause.message,
      });
    return yield* Effect.gen(function* () {
      const target = Math.floor(
        ((runtime.contextTokens ?? 48000) - (runtime.reserveTokens ?? 8192)) / 3,
      );
      const read = () =>
        history.read(definition.slug, { after: state().historyThrough, limit: 200 });
      let entries = yield* read();
      while (
        entries.length &&
        (contextSize(entries.map((e) => e.message)) > target ||
          entries.at(-1)!.seq < (yield* history.count(definition.slug)))
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
      return yield* runtime.reasoner.plan({
        goal: definition,
        current: current(),
        contexts: registry.snapshot(),
        signals: runtime.signals(definition.slug),
        reason,
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
                Effect.mapError((cause) => new GoalToolError({ message: cause.message })),
                Effect.flatMap((result) =>
                  result.error
                    ? Effect.fail(new GoalToolError({ message: result.error }))
                    : Effect.succeed(result.value),
                ),
              ),
          ),
        onMessage: (message) =>
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
    }).pipe(
      Effect.mapError((cause) => (cause instanceof GoalOperationError ? cause : failure(cause))),
      Effect.ensuring(
        Effect.sync(() => {
          active = false;
        }),
      ),
    );
  });
