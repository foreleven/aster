import { randomUUID } from "node:crypto";
import type { ActorRef, ReplyTo } from "@aster/actor";
import { AgentConversations } from "@aster/agent/harness";
import {
  AsterRuntime,
  DurableContext,
  QueryExecution,
  ApplicationError,
  goalTimeline,
  inspectTask,
  inspectReactions,
  approvalEntries,
  publicJson,
  type GoalsRootCommand,
  type GoalCommandReply,
  type TasksRootCommand,
  type TaskAdmissionReply,
  type ApprovalCommand,
  type ApprovalReply,
  type ReactionCommand,
  type RecoveryReply,
  readGoalState,
  readTaskState,
  readActor,
  SignalRecord,
  GetSignal,
  goalBusinessState,
} from "@aster/core";
import { ContextText, PublicApprovalEntry } from "@aster/core/contracts";
import { Cause, Effect, Layer, Queue, Schema, Scope, Stream } from "effect";
import { RpcServer } from "effect/rpc";
import { ApplicationRpcs } from "./rpc.js";
import { RuntimeSnapshot } from "./rpcs/runtime.js";
import { contextQueryKeys, QueryKeys, type QueryInvalidation } from "./rpcs/notifications.js";

/** Dynamic Actor selection is the single typed addressing boundary for RPC commands. */
const ask = Effect.fn("Rpc.ask")(function* <C, A>(
  runtime: AsterRuntime["Service"],
  path: string,
  command: (replyTo: ReplyTo<A>) => C,
) {
  const ref = yield* runtime.actors
    .select(path)
    .resolve()
    .pipe(
      Effect.catchTag("ActorNotFound", () =>
        Effect.fail(
          new ApplicationError({
            kind: "unavailable",
            message: "Actor unavailable",
          }),
        ),
      ),
    );
  return yield* (ref as ActorRef<C>).ask(command).pipe(
    Effect.catchTag("AskTimeoutError", () =>
      Effect.fail(
        new ApplicationError({
          kind: "unavailable",
          message: "Acknowledgement missing; acceptance is unknown. Retain the request identity",
        }),
      ),
    ),
  );
});

/** Subscribe before the first refresh. Overflow fails instead of losing invalidations. */
const invalidations = (reader: DurableContext["Service"]) =>
  Stream.unwrap(
    Effect.gen(function* () {
      const sourceScope = yield* Scope.fork(yield* Effect.scope);
      const changes = yield* reader.subscribe.pipe(Effect.provideService(Scope.Scope, sourceScope));
      const queue = yield* Queue.dropping<QueryInvalidation, ApplicationError | Cause.Done>(64);
      yield* Stream.runForEach(changes, ({ record }) =>
        Effect.gen(function* () {
          const accepted = yield* Queue.offer(queue, {
            _tag: "Invalidate",
            keys: contextQueryKeys(record.path),
          });
          if (!accepted)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Subscription fell behind; reconnect and refresh",
            });
        }),
      ).pipe(
        // Release upstream before finishing the queue, even if the transport is blocked.
        Effect.onExit((exit) => Scope.close(sourceScope, exit)),
        Effect.onExit((exit) =>
          exit._tag === "Failure" ? Queue.failCause(queue, exit.cause) : Queue.end(queue),
        ),
        Effect.forkScoped,
      );
      return Stream.make({
        _tag: "Invalidate",
        keys: [QueryKeys.all],
      } satisfies QueryInvalidation).pipe(Stream.concat(Stream.fromQueue(queue)));
    }),
  );

const traced = <A, E, R>(tag: string, requestId: unknown, run: Effect.Effect<A, E, R>) =>
  Effect.logInfo({ event: "rpc.request", tag, requestId: String(requestId) }).pipe(
    Effect.andThen(run),
    Effect.tapCause((cause) =>
      Effect.logError({
        event: "rpc.failed",
        tag,
        requestId: String(requestId),
        cause: Cause.pretty(cause),
      }),
    ),
  );

const handlers = ApplicationRpcs.toLayer(
  Effect.gen(function* () {
    const runtime = yield* AsterRuntime;
    const registry = yield* DurableContext;
    const queries = yield* QueryExecution;
    const conversations = yield* AgentConversations;
    const wireContext = (record: ContextText) =>
      Schema.decodeUnknownEffect(ContextText)(publicJson(record)).pipe(Effect.orDie);
    const contexts = Effect.suspend(() =>
      Effect.forEach(Object.values(registry.snapshot()), wireContext),
    );
    const liveEntries = Effect.fnUntraced(function* (pattern: RegExp) {
      const entries = yield* Effect.forEach(
        registry.directory().filter(({ path }) => pattern.test(path)),
        (entry) =>
          runtime.actors
            .select(`/user${entry.path}`)
            .resolve()
            .pipe(
              Effect.as(entry),
              Effect.catchTag("ActorNotFound", () => Effect.succeed(undefined)),
            ),
        { concurrency: 4 },
      );
      return entries.filter((entry) => entry !== undefined);
    });
    return {
      SubscribeInvalidations: () => invalidations(registry),
      ListContexts: () => contexts,
      GetContext: ({ path }) =>
        Effect.suspend(() => {
          const record = registry.get(path);
          return record
            ? wireContext(record)
            : Effect.fail(
                new ApplicationError({
                  kind: "not-found",
                  message: "Context not found",
                }),
              );
        }),
      QueryContext: (input) => queries.json(input),
      ListGoals: () =>
        liveEntries(/^\/goals\/[^/]+$/).pipe(
          Effect.flatMap((entries) =>
            Effect.forEach(
              entries,
              ({ path }) =>
                readGoalState(runtime.actors, path).pipe(
                  Effect.map((state) => ({
                    path,
                    revision: registry.get(path)!.revision,
                    ...goalBusinessState(state),
                  })),
                ),
              { concurrency: 4 },
            ),
          ),
        ),
      ListTasks: () =>
        liveEntries(/^\/tasks\/[^/]+$/).pipe(
          Effect.flatMap((entries) =>
            Effect.forEach(
              entries,
              ({ path, description }) =>
                readTaskState(runtime.actors, path).pipe(
                  Effect.map((state) => ({
                    path,
                    description,
                    revision: registry.get(path)!.revision,
                    status: state.status,
                    sourcePath: state.admission.source,
                    replyTo: state.admission.replyTo,
                    agent: state.admission.agent,
                    inputs: state.inputs.length,
                  })),
                ),
              { concurrency: 4 },
            ),
          ),
        ),
      ListSignals: () =>
        liveEntries(/^\/signals\/[^/]+$/).pipe(
          Effect.flatMap((entries) =>
            Effect.forEach(
              entries,
              ({ path }) =>
                readActor(
                  runtime.actors,
                  path,
                  (replyTo) => new GetSignal({ replyTo }),
                  SignalRecord,
                ).pipe(Effect.map(({ state, ...metadata }) => ({ ...metadata, ...state }))),
              { concurrency: 4 },
            ),
          ),
        ),
      GetGoalTimeline: ({ slug, ...page }) => goalTimeline(registry, conversations, slug, page),
      SendGoalMessage: (input, options) =>
        traced(
          "SendGoalMessage",
          options.requestId,
          Effect.gen(function* () {
            const text = input.text.trim();
            if (!text)
              return yield* new ApplicationError({
                kind: "invalid-input",
                message: "Message text is required",
              });
            const requestId = input.requestId ?? randomUUID();
            const reply = yield* ask<GoalsRootCommand, GoalCommandReply>(
              runtime,
              "/user/goals",
              (replyTo) => ({
                _tag: "Route",
                slug: input.slug,
                command: {
                  _tag: "SubmitInput",
                  requestId,
                  input: { _tag: "UserInput", text },
                  replyTo,
                },
              }),
            );
            if (reply._tag === "Rejected") return yield* reply.error;
          }),
        ),
      EndGoal: (input, options) =>
        traced(
          "EndGoal",
          options.requestId,
          Effect.gen(function* () {
            const requestId = input.requestId ?? randomUUID();
            const reply = yield* ask<GoalsRootCommand, GoalCommandReply>(
              runtime,
              "/user/goals",
              (replyTo) => ({
                _tag: "Route",
                slug: input.slug,
                command: { _tag: "End", requestId, replyTo },
              }),
            );
            if (reply._tag === "Rejected") return yield* reply.error;
          }),
        ),
      RetryGoalTurn: (input, options) =>
        traced(
          "RetryGoalTurn",
          options.requestId,
          Effect.gen(function* () {
            const reply = yield* ask<GoalsRootCommand, GoalCommandReply>(
              runtime,
              "/user/goals",
              (replyTo) => ({
                _tag: "Route",
                slug: input.slug,
                command: {
                  _tag: "RetryTurn",
                  requestId: input.requestId,
                  turnId: input.turnId,
                  replyTo,
                },
              }),
            );
            if (reply._tag === "Rejected") return yield* reply.error;
            return reply.receipt;
          }),
        ),
      InspectTask: ({ path }) => inspectTask(registry, conversations, path, runtime.actors),
      CheckTask: (input, options) =>
        traced(
          "CheckTask",
          options.requestId,
          Effect.gen(function* () {
            const reply = yield* ask<TasksRootCommand, TaskAdmissionReply>(
              runtime,
              "/user/tasks",
              (replyTo) => ({ _tag: "CheckTask", input, replyTo }),
            );
            if (reply._tag === "Rejected") return yield* reply.error;
            return reply.receipt;
          }),
        ),
      RetryTask: (input, options) =>
        traced(
          "RetryTask",
          options.requestId,
          Effect.gen(function* () {
            const reply = yield* ask<TasksRootCommand, TaskAdmissionReply>(
              runtime,
              "/user/tasks",
              (replyTo) => ({ _tag: "RetryTask", input, replyTo }),
            );
            if (reply._tag === "Rejected") return yield* reply.error;
            return reply.receipt;
          }),
        ),
      ListApprovals: () =>
        Effect.suspend(() =>
          approvalEntries(runtime.actors).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(PublicApprovalEntry))),
          ),
        ).pipe(Effect.orDie),
      RespondToApproval: ({ id, response }, options) =>
        traced(
          "RespondToApproval",
          options.requestId,
          Effect.gen(function* () {
            const reply = yield* ask<ApprovalCommand, ApprovalReply>(
              runtime,
              "/user/approvals",
              (replyTo) => ({ _tag: "Resolve", id, response, replyTo }),
            );
            if (reply._tag === "Rejected") return yield* reply.error;
          }),
        ),
      InspectProcessing: () => inspectReactions(runtime.actors),
      RecoverProcessing: (input, options) =>
        traced(
          "RecoverProcessing",
          options.requestId,
          Effect.gen(function* () {
            const reply = yield* ask<ReactionCommand, RecoveryReply>(
              runtime,
              "/user/system-one",
              (replyTo) => ({ _tag: "Recover", input, replyTo }),
            );
            if (reply._tag === "Rejected") return yield* reply.error;
            return reply.receipt;
          }),
        ),
      InspectRuntime: () =>
        runtime.inspect.pipe(
          Effect.map(({ actors, ...snapshot }) => ({
            ...snapshot,
            actors: actors.map(({ metadata, ...actor }) => ({
              ...actor,
              contextPath:
                typeof metadata.contextPath === "string" ? metadata.contextPath : undefined,
            })),
          })),
          Effect.flatMap(Schema.decodeUnknownEffect(RuntimeSnapshot)),
          Effect.mapError(
            () =>
              new ApplicationError({
                kind: "unavailable",
                message: "Runtime inspection unavailable",
              }),
          ),
        ),
    };
  }),
);

/** Applications provide the Protocol and the Runtime Layer's existing domain services. */
export const layer = RpcServer.layer(ApplicationRpcs).pipe(Layer.provide(handlers));
