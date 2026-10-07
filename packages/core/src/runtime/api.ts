import { inspectTask } from "../tasks/view.js";
import type { TasksRootCommand } from "../tasks/root.js";
import type { TaskAdmissionReply } from "../tasks/protocol.js";
import { TaskRecoveryInput } from "../tasks/contracts.js";
import { randomUUID } from "node:crypto";
import {
  ContextQueries,
  ContextQueryError,
  type ContextQueryInput,
} from "../context/queries/routes.js";
import { inspectReactions } from "../reactions/inspection.js";
import { RecoveryInput } from "../reactions/contracts.js";
import { type CommandReceipt, ApplicationError } from "../operations.js";
import { goalTimeline } from "../goals/view.js";
import { Effect, Schema, Stream } from "effect";

import { GoalRequestData, type GoalCommand, type GoalCommandReply } from "../goals/protocol.js";
export { ApplicationError } from "../operations.js";
import type { ActorRef } from "@aster/actor";
import { PublicContext as ContextRecord } from "../context/contracts.js";
import type { ContextRegistry } from "../context/registry.js";
import { publicJson } from "../json.js";
import { PublicApprovalEntry } from "../approvals/view.js";
import type { AgentConversations } from "@aster/agent";

import type { GoalsRootCommand } from "../goals/root.js";
import { approvalEntries, type ApprovalCommand, type ApprovalReply } from "../approvals/actor.js";
import type { ApprovalResponse } from "../tasks/execution/contracts.js";

/** Transport-independent queries and commands. Actor paths remain inside core. */
export const makeApplicationApi = <Inspection>(options: {
  readonly registry: ContextRegistry["Service"];
  readonly queries?: ContextQueries["Service"];
  readonly conversations: AgentConversations["Service"];
  readonly goals?: ActorRef<GoalsRootCommand>;
  readonly tasks?: ActorRef<TasksRootCommand>;
  readonly approvals?: ActorRef<ApprovalCommand>;
  readonly inspect: Effect.Effect<Inspection>;
  readonly recoverProcessing?: (
    input: RecoveryInput,
  ) => Effect.Effect<CommandReceipt, ApplicationError>;
}) => {
  const { registry } = options;
  const wireContext = (record: ContextRecord): ContextRecord =>
    Schema.decodeUnknownSync(ContextRecord)(publicJson(record));
  const wireContexts = Effect.sync(() =>
    Object.values(registry.reader.snapshot()).map(wireContext),
  );
  const requireGoal = (slug: string) =>
    Effect.suspend(() =>
      registry.get(`/goals/${slug}`)
        ? Effect.void
        : Effect.fail(new ApplicationError({ kind: "not-found", message: "Goal not found" })),
    );
  const route = Effect.fn("ApplicationApi.route")(function* (
    slug: string,
    command:
      | Omit<Extract<GoalCommand, { _tag: "SubmitInput" }>, "replyTo">
      | Omit<Extract<GoalCommand, { _tag: "End" }>, "replyTo">,
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
    if (reply._tag === "Rejected") return yield* reply.error;
  });
  return {
    inspectTask: (path: string) => inspectTask(registry, options.conversations, path),
    checkTask: (input: TaskRecoveryInput) =>
      Effect.gen(function* () {
        if (!options.tasks)
          return yield* new ApplicationError({
            kind: "unavailable",
            message: "Task owner unavailable",
          });
        const reply = yield* options.tasks
          .ask<TaskAdmissionReply>((replyTo) => ({ _tag: "CheckTask", input, replyTo }))
          .pipe(
            Effect.mapError(
              () =>
                new ApplicationError({
                  kind: "unavailable",
                  message: "Check acknowledgement missing; retain the original identity",
                }),
            ),
          );
        if (reply._tag === "Rejected") return yield* reply.error;
        return reply.receipt;
      }),
    retryTask: (input: TaskRecoveryInput) =>
      Effect.gen(function* () {
        if (!options.tasks)
          return yield* new ApplicationError({
            kind: "unavailable",
            message: "Task owner unavailable",
          });
        const reply = yield* options.tasks
          .ask<TaskAdmissionReply>((replyTo) => ({ _tag: "RetryTask", input, replyTo }))
          .pipe(
            Effect.mapError(
              () =>
                new ApplicationError({
                  kind: "unavailable",
                  message: "Retry acknowledgement missing; retain the original identity",
                }),
            ),
          );
        if (reply._tag === "Rejected") return yield* reply.error;
        return reply.receipt;
      }),
    queryContext: (input: ContextQueryInput) =>
      options.queries
        ? options.queries.query(input)
        : Effect.fail(
            new ContextQueryError({ kind: "unavailable", message: "Context queries unavailable" }),
          ),
    inspectProcessing: () => inspectReactions(registry),
    recoverProcessing: Effect.fn("ApplicationApi.recoverProcessing")(function* (
      raw: RecoveryInput,
    ) {
      const input = yield* Schema.decodeUnknownEffect(RecoveryInput)(raw).pipe(
        Effect.mapError(
          () =>
            new ApplicationError({ kind: "invalid-input", message: "Invalid recovery request" }),
        ),
      );
      if (!options.recoverProcessing)
        return yield* new ApplicationError({
          kind: "unavailable",
          message: "Processing recovery unavailable",
        });
      return yield* options.recoverProcessing(input);
    }),
    changes: registry.changes.pipe(
      Stream.map((change) => ({
        ...change,
        record: wireContext(registry.views.project(change.record)),
      })),
    ),
    // Acquisition subscribes before transports acknowledge readiness to a client.
    subscribeChanges: registry.subscribe.pipe(
      Effect.map((changes) =>
        changes.pipe(
          Stream.map(({ record }) => ({ path: record.path, revision: record.revision })),
        ),
      ),
    ),
    contexts: wireContexts,
    inspect: options.inspect,
    context: (path: string) =>
      Effect.suspend(() => {
        const record = registry.reader.get(path);
        return record
          ? Effect.succeed(wireContext(record))
          : Effect.fail(new ApplicationError({ kind: "not-found", message: "Context not found" }));
      }),
    dashboard: Effect.gen(function* () {
      return {
        contexts: yield* wireContexts,
        runtime: yield* options.inspect,
        at: new Date().toISOString(),
      };
    }),
    goals: {
      retryTurn: Effect.fn("ApplicationApi.retryGoalTurn")(function* (
        slug: string,
        request: Omit<Extract<GoalRequestData, { _tag: "RetryTurn" }>, "_tag">,
      ) {
        const input = yield* Schema.decodeUnknownEffect(
          GoalRequestData.pipe(Schema.toTaggedUnion("_tag")).cases.RetryTurn,
        )({ ...request, _tag: "RetryTurn" }).pipe(
          Effect.mapError(
            () => new ApplicationError({ kind: "invalid-input", message: "Invalid turn retry" }),
          ),
        );
        yield* requireGoal(slug);
        if (!options.goals)
          return yield* new ApplicationError({
            kind: "unavailable",
            message: "No Goals configured",
          });
        const reply = yield* options.goals
          .ask<GoalCommandReply>((replyTo) => ({
            _tag: "Route",
            slug,
            command: {
              _tag: "RetryTurn",
              requestId: input.requestId,
              turnId: input.turnId,
              replyTo,
            },
          }))
          .pipe(
            Effect.mapError(
              () =>
                new ApplicationError({
                  kind: "unavailable",
                  message: "Retry acknowledgement missing; reuse the same request identity",
                }),
            ),
          );
        if (reply._tag === "Rejected") return yield* reply.error;
        return reply.receipt;
      }),
      timeline: (slug: string, page: { before?: number; limit?: number } = {}) =>
        goalTimeline(registry, options.conversations, slug, page),
      list: wireContexts.pipe(
        Effect.map((records) => records.filter((record) => /^\/goals\/[^/]+$/.test(record.path))),
      ),
      sendMessage: (slug: string, text: string, requestId?: string) =>
        text.trim()
          ? route(slug, {
              _tag: "SubmitInput",
              input: { _tag: "UserInput", text: text.trim() },
              requestId: requestId ?? randomUUID(),
            })
          : Effect.fail(
              new ApplicationError({ kind: "invalid-input", message: "Message text is required" }),
            ),
      end: (slug: string, requestId: string = randomUUID()) =>
        route(slug, { _tag: "End", requestId }),
    },
    approvals: {
      list: Effect.sync(() =>
        Schema.decodeUnknownSync(Schema.Array(PublicApprovalEntry))(approvalEntries(registry)),
      ),
      respond: (id: string, response: ApprovalResponse) =>
        Effect.gen(function* () {
          if (!options.approvals)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Approval queue unavailable",
            });
          const result = yield* options.approvals
            .ask<ApprovalReply>((replyTo) => ({
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
          if (result._tag === "Rejected") return yield* result.error;
        }),
    },
  };
};
export type ApplicationApi<Inspection = unknown> = ReturnType<
  typeof makeApplicationApi<Inspection>
>;
