import { Effect, Schema, Stream } from "effect";
import { ApplicationError, contextQueryKeys, RuntimeSnapshot } from "@aster/api-contracts";
export { ApplicationError } from "@aster/api-contracts";
import type { ActorRef } from "@aster/actor";
import type { ContextRegistry } from "../context/registry.js";
import type { GoalHistory } from "../goals/history.js";
import type { GoalCommand, GoalCommandReply, GoalsRootCommand } from "../goals/actors.js";
import { approvalEntries, type ApprovalCommand } from "../approvals/actor.js";
import type { ApprovalResponse } from "../tasks/model.js";

/** Transport-independent queries and commands. Actor paths remain inside core. */
export const makeApplicationApi = (options: {
  readonly registry: ContextRegistry["Service"];
  readonly history?: GoalHistory;
  readonly goals?: ActorRef<GoalsRootCommand>;
  readonly approvals?: ActorRef<ApprovalCommand>;
  readonly inspect: Effect.Effect<unknown>;
}) => {
  const { registry } = options;
  const requireGoal = (slug: string) =>
    Effect.suspend(() =>
      registry.get(`/goals/${slug}`)
        ? Effect.void
        : Effect.fail(new ApplicationError({ kind: "not-found", message: "Goal not found" })),
    );
  const route = Effect.fn("ApplicationApi.route")(function* (
    slug: string,
    command: Extract<GoalCommand, { _tag: "UserMessage" | "End" }>,
  ) {
    yield* requireGoal(slug);
    if (!options.goals)
      return yield* new ApplicationError({ kind: "unavailable", message: "No Goals configured" });
    const reply = yield* options.goals
      .ask<GoalCommandReply>((replyTo) => ({
        _tag: "Route",
        slug,
        command: { ...command, replyTo },
      }))
      .pipe(
        Effect.catchTag("AskTimeoutError", () =>
          Effect.fail(
            new ApplicationError({
              kind: "unavailable",
              message: "Goal acknowledgement timed out; acceptance is unknown",
            }),
          ),
        ),
      );
    if (reply._tag === "Unavailable")
      return yield* new ApplicationError({ kind: "unavailable", message: reply.message });
  });
  return {
    changes: registry.changes,
    // Acquisition subscribes before transports acknowledge readiness to a client.
    subscribeInvalidations: registry.subscribe.pipe(
      Effect.map((changes) =>
        changes.pipe(
          Stream.map(({ path }) => ({ _tag: "Invalidate" as const, keys: contextQueryKeys(path) })),
        ),
      ),
    ),
    contexts: Effect.sync(() => Object.values(registry.snapshot())),
    inspect: options.inspect.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(RuntimeSnapshot)),
      Effect.mapError(
        () =>
          new ApplicationError({ kind: "unavailable", message: "Runtime inspection unavailable" }),
      ),
    ),
    context: (path: string) =>
      Effect.suspend(() => {
        const record = registry.get(path);
        return record
          ? Effect.succeed(record)
          : Effect.fail(new ApplicationError({ kind: "not-found", message: "Context not found" }));
      }),
    dashboard: Effect.gen(function* () {
      return {
        contexts: Object.values(registry.snapshot()),
        runtime: yield* options.inspect,
        at: new Date().toISOString(),
      };
    }),
    goals: {
      list: Effect.sync(() =>
        Object.values(registry.snapshot()).filter((record) => /^\/goals\/[^/]+$/.test(record.path)),
      ),
      sendMessage: (slug: string, text: string) =>
        text.trim()
          ? route(slug, { _tag: "UserMessage", text: text.trim() })
          : Effect.fail(
              new ApplicationError({ kind: "invalid-input", message: "Message text is required" }),
            ),
      end: (slug: string) => route(slug, { _tag: "End" }),
      history: (slug: string, page: { before?: number; limit?: number } = {}) =>
        Effect.gen(function* () {
          yield* requireGoal(slug);
          if (!options.history)
            return yield* new ApplicationError({
              kind: "not-found",
              message: "Goal history unavailable",
            });
          const count = yield* options.history.count(slug);
          const before = page.before ?? count + 1;
          const limit = page.limit ?? 30;
          if (
            !Number.isInteger(before) ||
            before < 1 ||
            !Number.isInteger(limit) ||
            limit < 1 ||
            limit > 100
          )
            return yield* new ApplicationError({
              kind: "invalid-input",
              message: "Invalid history page",
            });
          const after = Math.max(0, before - limit - 1);
          const entries = yield* options.history.read(slug, { after, before, limit });
          return {
            entries,
            total: count,
            nextBefore: after > 0 ? (entries[0]?.seq ?? null) : null,
          };
        }).pipe(
          Effect.catchTag("GoalHistoryError", () =>
            Effect.fail(
              new ApplicationError({ kind: "unavailable", message: "Goal history unavailable" }),
            ),
          ),
        ),
    },
    approvals: {
      list: Effect.sync(() => approvalEntries(registry)),
      respond: (id: string, response: ApprovalResponse) =>
        Effect.gen(function* () {
          if (!options.approvals)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Approval queue unavailable",
            });
          const result = yield* options.approvals
            .ask<{ error?: string }>((replyTo) => ({
              _tag: "Resolve",
              id,
              response,
              replyTo,
            }))
            .pipe(
              Effect.catchTag("AskTimeoutError", () =>
                Effect.fail(
                  new ApplicationError({
                    kind: "unavailable",
                    message: "Approval acknowledgement timed out; acceptance is unknown",
                  }),
                ),
              ),
            );
          if (result.error)
            return yield* new ApplicationError({ kind: "conflict", message: result.error });
        }),
    },
  };
};
export type ApplicationApi = ReturnType<typeof makeApplicationApi>;
