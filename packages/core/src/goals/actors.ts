import { goalAgentGate } from "./gate.js";
import { deliverTask } from "../tasks/message.js";
import { randomUUID } from "node:crypto";
import { ReplyTo, type ActorContext } from "@aster/actor";
import { AgentError, AgentRunner } from "@aster/agent";
import { ApplicationError } from "@aster/api-contracts";
import { Deferred, Effect, Layer, Match, Option, Schema } from "effect";
import { GoalSettings } from "../config/settings.js";
import type { GoalDefinition } from "../config/schema.js";
import { ContextActor } from "../context/actor.js";
import { defineContext } from "../context/definition.js";
import { ContextRegistry } from "../context/registry.js";
import { ContextQueries } from "../context/queries.js";
import { MemoryRecall } from "../memory/contracts.js";
import { GoalSignals } from "../signals/goal-owner.js";
import { ExternalAgents } from "../tasks/model.js";
import { attachGoalTasks, cancelGoalTasks, followupTask } from "../tasks/commands.js";
import { AgentConversations } from "@aster/agent";
import { GoalState } from "./state.js";
import { goalWorkingState } from "./working-state.js";
import { goalAdmission } from "./admission.js";
import { goalInputs, type StoredGoalInput } from "./inputs.js";
import {
  GoalCommand,
  GoalControl,
  GoalRequestData,
  goalRequestFingerprint,
  GoalReadyReply,
  GoalCommandReply,
} from "./protocol.js";
import { conversationText, runGoalConversation } from "./conversation.js";
export { GoalCommand, GoalCommandReply, GoalReadyReply } from "./protocol.js";
export { GoalsRootActor, GoalsRootCommand } from "./root.js";

const GoalMailbox = Schema.Union([
  GoalCommand,
  GoalControl,
  Schema.TaggedStruct("RunNext", {}),
  Schema.TaggedStruct("UpdateProgress", {
    generation: Schema.String,
    progress: Schema.String,
    completed: Schema.Boolean,
    evidence: Schema.Array(Schema.String),
    replyTo: ReplyTo<GoalCommandReply>(),
  }),
  Schema.TaggedStruct("GateSettled", {
    generation: Schema.String,
    inputId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", {
        value: Schema.Struct({ relevant: Schema.Boolean, reason: Schema.String }),
      }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(AgentError) }),
    ]),
  }),
  Schema.TaggedStruct("ConversationSettled", {
    generation: Schema.String,
    inputId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.String }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(AgentError) }),
    ]),
  }),
  Schema.TaggedStruct("PeersEnded", {
    generation: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Void }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(Error) }),
    ]),
  }),
]);
export type GoalMailbox = typeof GoalMailbox.Type;
type Services =
  GoalSignals | GoalSettings | AgentConversations | AgentRunner | MemoryRecall | ExternalAgents;
type Owner = ActorContext<GoalMailbox, Services | ContextRegistry>;

export class GoalActor extends ContextActor.Service<GoalActor, Services>()("goals/Actor", {
  command: GoalMailbox,
  context: defineContext({
    state: GoalState,
    message: Schema.Unknown,
  }),
}) {
  static readonly layer = Layer.effect(
    GoalActor,
    Effect.gen(function* () {
      const runner = yield* AgentRunner;
      const registry = yield* ContextRegistry;
      const settings = yield* GoalSettings;
      const signals = yield* GoalSignals;
      const history = yield* AgentConversations;
      const memory = yield* MemoryRecall;
      const agents = yield* ExternalAgents;
      const queries = Option.getOrUndefined(yield* Effect.serviceOption(ContextQueries));
      let definition: GoalDefinition;
      let path = "";
      const working = goalWorkingState(registry, () => path);
      const { state, current, save } = working;
      const inputs = goalInputs(working, history);
      const accept = goalAdmission(registry, working, history);
      const incarnation = randomUUID();
      let activated = false;
      let running: { generation: string; cancellation: Deferred.Deferred<void> } | undefined;
      const waiters: { replyTo: ReplyTo<GoalReadyReply>; stage?: "restored" | "activated" }[] = [];
      const ready = Effect.fnUntraced(function* () {
        for (let index = waiters.length - 1; index >= 0; index--) {
          const waiter = waiters[index]!;
          if (!activated && waiter.stage !== "restored") continue;
          waiters.splice(index, 1);
          yield* waiter.replyTo.tell({ _tag: "Ready" });
        }
      });
      const patchInput = (id: string, patch: Partial<StoredGoalInput>) =>
        save({
          inputs: state().inputs.map((input) =>
            input.inputId === id ? { ...input, ...patch } : input,
          ),
        });
      const wake = (context: Owner) => context.self.tell({ _tag: "RunNext" });
      const endPeers = Effect.fnUntraced(function* (context: Owner) {
        yield* cancelGoalTasks(context, registry, path);
        yield* context.pipeToSelf(
          signals.deactivate(definition.slug).pipe(Effect.asVoid),
          (result) => ({ _tag: "PeersEnded", generation: incarnation, result }),
        );
      });
      const runConversation = Effect.fnUntraced(function* (
        context: Owner,
        input: StoredGoalInput,
        reconcile: boolean,
      ) {
        const attempt = running!;
        yield* context.pipeToSelf(
          runGoalConversation({
            goal: definition,
            input: yield* inputs.resolve(input),
            model: settings.reasoning!.model,
            registry,
            memory,
            queries,
            executors: Object.keys(agents),
            contextTokens: settings.reasoning?.contextTokens,
            reserveTokens: settings.reasoning?.reserveTokens,
            reconcile,
            update: (progress, completed, evidence) =>
              context.self
                .ask<GoalCommandReply>((replyTo) => ({
                  _tag: "UpdateProgress",
                  generation: attempt.generation,
                  progress,
                  completed,
                  evidence,
                  replyTo,
                }))
                .pipe(
                  Effect.mapError(
                    () =>
                      new ApplicationError({
                        kind: "unavailable",
                        message: "Goal update acknowledgement missing",
                      }),
                  ),
                  Effect.flatMap((reply) =>
                    reply._tag === "Accepted"
                      ? Effect.succeed(reply.receipt)
                      : Effect.fail(reply.error),
                  ),
                ),
            startTask: (input) => deliverTask(context, input),
            followupTask: (input) => followupTask(context, input),
            signal: (input) => signals.applySignal(input),
          }).pipe(
            Effect.provideService(AgentRunner, runner),
            Effect.map((result) => conversationText(result.messages)),
            Effect.raceFirst(
              Deferred.await(attempt.cancellation).pipe(Effect.andThen(Effect.interrupt)),
            ),
          ),
          (result) => ({
            _tag: "ConversationSettled",
            generation: attempt.generation,
            inputId: input.inputId,
            result,
          }),
        );
      });
      return GoalActor.of({
        started: (context) =>
          Effect.gen(function* () {
            const slug = context.path.split("/").at(-1)!;
            const configured = settings.definitions.find((goal) => goal.slug === slug);
            if (!configured) return yield* Effect.die(new Error(`Unknown Goal ${slug}`));
            definition = configured;
            path = `/goals/${slug}`;
            if (!registry.get(path)) {
              const initial: GoalState = {
                definition,
                status: "active",
                summary: "Ready to begin",
                inputs: [],
                receipts: [],
              };
              yield* registry
                .commit(
                  {
                    path,
                    description: definition.description,
                    messages: [],
                    state: {
                      ...initial,
                      inputs: [],
                    },
                  },
                  { expectedRevision: 0 },
                )
                .pipe(Effect.orDie);
            }
            yield* inputs.recover();
            if (slug !== "personal" && state().inputs.length === 0)
              yield* inputs
                .accept({ _tag: "GoalStarted", pursuit: "initial" }, "initial", {
                  rootRequestId: `goal:${slug}:initial`,
                  remainingAgentTurns: 4,
                })
                .pipe(Effect.orDie);
            yield* save({
              definition,
              inputs: state().inputs.map((input) =>
                state().status === "active" && input.status === "unknown"
                  ? { ...input, status: "running" }
                  : input,
              ),
            }).pipe(Effect.orDie);
            yield* attachGoalTasks(context, registry, path);
            const activation = context.metadata.goalActivation as
              Deferred.Deferred<void> | undefined;
            if (activation)
              yield* context.pipeToSelf(Deferred.await(activation), () => ({ _tag: "Activate" }));
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("AwaitReady", (command) =>
              Effect.gen(function* () {
                waiters.push(command);
                yield* ready();
              }),
            ),
            Match.tag("Activate", () =>
              Effect.gen(function* () {
                activated = true;
                yield* ready();
                yield* wake(context);
              }),
            ),
            Match.tag("PeersEnded", (command) =>
              Effect.gen(function* () {
                if (command.generation !== incarnation) return;
                if (command.result._tag === "Failure") yield* Effect.logError(command.result.error);
              }),
            ),
            Match.tag("SubmitInput", "End", "RetryTurn", (command) =>
              Effect.gen(function* () {
                const request = Schema.decodeUnknownSync(GoalRequestData)(command);
                const payloadFingerprint = goalRequestFingerprint(request);
                const previous = state().receipts.find(
                  (item) => item.requestId === request.requestId,
                );
                if (previous)
                  return yield* command.replyTo.tell(
                    previous.payloadFingerprint === payloadFingerprint
                      ? { _tag: "Accepted", receipt: previous.receipt }
                      : {
                          _tag: "Rejected",
                          error: new ApplicationError({
                            kind: "conflict",
                            message: "Goal request identity belongs to another payload",
                          }),
                        },
                  );
                const receipt = {
                  requestId: request.requestId,
                  revision: (current().revision ?? 0) + 1,
                };
                const record = { requestId: request.requestId, payloadFingerprint, receipt };
                const receipts = [...state().receipts, record];
                const result = yield* Match.value(request).pipe(
                  Match.tag("SubmitInput", () => accept(request, record)),
                  Match.tag("End", () =>
                    save({
                      status: "completed",
                      receipts,
                      inputs: state().inputs.map((input) =>
                        Match.value(input).pipe(
                          Match.when({ status: "running" }, (input) => ({
                            ...input,
                            status: "unknown" as const,
                            error: "Goal ended during conversation delivery",
                          })),
                          Match.when({ status: "pending" }, (input) => ({
                            ...input,
                            status: "ignored" as const,
                          })),
                          Match.orElse((input) => input),
                        ),
                      ),
                    }).pipe(Effect.orDie, Effect.as(receipt)),
                  ),
                  Match.tag("RetryTurn", (request) =>
                    Effect.gen(function* () {
                      const input = state().inputs.find(
                        (input) => input.inputId === request.turnId,
                      );
                      if (
                        state().status !== "active" ||
                        running ||
                        !input ||
                        input.status !== "failed" ||
                        state().inputs.some((item) => item.retryOf === input.inputId)
                      )
                        return yield* new ApplicationError({
                          kind: "conflict",
                          message:
                            "Only a failed, unretried input on an idle active Goal can be retried",
                        });
                      const original = yield* inputs.resolve(input);
                      yield* inputs.accept(
                        original.payload,
                        request.requestId,
                        input.causal,
                        { receipts },
                        undefined,
                        input.inputId,
                      );
                      return receipt;
                    }),
                  ),
                  Match.exhaustive,
                  Effect.result,
                );
                if (result._tag === "Failure")
                  return yield* command.replyTo.tell({ _tag: "Rejected", error: result.failure });
                yield* command.replyTo.tell({ _tag: "Accepted", receipt: result.success });
                if (request._tag === "End") {
                  const attempt = running;
                  running = undefined;
                  if (attempt) yield* Deferred.succeed(attempt.cancellation, undefined);
                  yield* endPeers(context);
                } else yield* wake(context);
              }),
            ),
            Match.tag("UpdateProgress", (command) =>
              Effect.gen(function* () {
                const invalid =
                  !running ||
                  running.generation !== command.generation ||
                  state().status !== "active" ||
                  !command.progress.trim() ||
                  command.progress.length > 6000 ||
                  command.evidence.some((path) => !registry.get(path)) ||
                  (command.completed &&
                    (!definition.completionCriteria || !command.evidence.length));
                if (invalid)
                  return yield* command.replyTo.tell({
                    _tag: "Rejected",
                    error: new ApplicationError({
                      kind: "conflict",
                      message: "Goal update is stale or lacks completion evidence",
                    }),
                  });
                yield* save({
                  summary: command.progress,
                  ...(command.completed
                    ? {
                        status: "completed",
                        inputs: state().inputs.map((input) =>
                          input.status === "pending"
                            ? {
                                ...input,
                                status: "ignored" as const,
                              }
                            : input,
                        ),
                      }
                    : {}),
                });
                yield* command.replyTo.tell({
                  _tag: "Accepted",
                  receipt: { requestId: command.generation, revision: current().revision! },
                });
                if (command.completed) yield* endPeers(context);
              }),
            ),
            Match.tag("RunNext", () =>
              Effect.gen(function* () {
                if (!activated || running || state().status !== "active") return;
                // An uncertain delivery blocks later inputs. Restart inspects it using the same Pi identity.
                const input =
                  state().inputs.find((input) => input.status === "running") ??
                  state().inputs.find((input) => input.status === "pending");
                if (!input || state().inputs.some((item) => item.status === "unknown")) return;
                if (input.causal.remainingAgentTurns <= 0) {
                  if (input.kind === "ExecutionFeedback")
                    yield* history
                      .append(path, `${input.inputId}:budget`, "goal.reply", {
                        inputId: input.inputId,
                        text: "The task has an update. I have reached the automatic follow-up limit; please review Task details or send me another instruction.",
                      })
                      .pipe(Effect.orDie);
                  yield* patchInput(input.inputId, {
                    status: "ignored",
                    error: "Automatic feedback budget exhausted",
                  });
                  yield* wake(context);
                  return;
                }
                running = { generation: randomUUID(), cancellation: yield* Deferred.make<void>() };
                yield* patchInput(input.inputId, { status: "running" });
                const resolved = yield* inputs.resolve(input);
                if (resolved.payload._tag === "GoalIntent" && input.relevant === undefined) {
                  const attempt = running;
                  yield* context.pipeToSelf(
                    goalAgentGate(
                      settings.reasoning!.model,
                      definition,
                      resolved.payload.intent,
                    ).pipe(
                      Effect.provideService(AgentRunner, runner),
                      Effect.raceFirst(
                        Deferred.await(attempt.cancellation).pipe(Effect.andThen(Effect.interrupt)),
                      ),
                    ),
                    (result) => ({
                      _tag: "GateSettled",
                      generation: attempt.generation,
                      inputId: input.inputId,
                      result,
                    }),
                  );
                } else yield* runConversation(context, input, input.status === "running");
              }),
            ),
            Match.tag("GateSettled", (command) =>
              Effect.gen(function* () {
                if (running?.generation !== command.generation) return;
                if (command.result._tag === "Failure") {
                  running = undefined;
                  yield* patchInput(command.inputId, {
                    status: "failed",
                    error: command.result.error.message,
                  });
                  yield* wake(context);
                  return;
                }
                const decision = command.result.value;
                yield* patchInput(command.inputId, {
                  relevant: decision.relevant,
                  ...(decision.relevant ? {} : { status: "ignored" }),
                });
                if (!decision.relevant) {
                  running = undefined;
                  yield* wake(context);
                  return;
                }
                yield* runConversation(
                  context,
                  state().inputs.find((input) => input.inputId === command.inputId)!,
                  true,
                );
              }),
            ),
            Match.tag("ConversationSettled", (command) =>
              Effect.gen(function* () {
                if (running?.generation !== command.generation) return;
                running = undefined;
                if (command.result._tag === "Failure") {
                  const error = command.result.error;
                  yield* history
                    .append(path, `${command.inputId}:reply:${error.outcome}`, "goal.reply", {
                      inputId: command.inputId,
                      text:
                        error.outcome === "failed"
                          ? "I couldn't complete this request. Please retry or give me another instruction."
                          : "The outcome of this request is uncertain. I need to reconcile it before continuing.",
                    })
                    .pipe(Effect.orDie);
                  yield* patchInput(command.inputId, {
                    status: error.outcome === "failed" ? "failed" : "unknown",
                    error: error.message,
                  });
                } else {
                  const input = state().inputs.find((input) => input.inputId === command.inputId)!;
                  const text =
                    command.result.value.trim() ||
                    (input.kind === "UserInput"
                      ? "I couldn't produce a reply. Please give me another instruction."
                      : input.kind === "ExecutionFeedback"
                        ? "The task has an update. Please review its result or pending request in Task details."
                        : "");
                  if (text)
                    yield* history
                      .append(path, `${command.inputId}:reply`, "goal.reply", {
                        inputId: command.inputId,
                        text,
                      })
                      .pipe(Effect.orDie);
                  yield* patchInput(command.inputId, { status: "completed" });
                }
                yield* wake(context);
              }),
            ),
            Match.exhaustive,
            Effect.orDie,
          ),
      });
    }),
  );
}
