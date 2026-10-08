import { ExternalAgents } from "../tasks/execution/contracts.js";
import { GoalMailbox, GoalCommand, GoalRequestData } from "./protocol.js";
import { GoalSnapshot, type StoredGoalInput } from "./state/snapshot.js";
import { GoalState } from "./state/model.js";
import { GoalAgent } from "./agent.js";
import { cancelGoalTasks } from "../tasks/delivery.js";
import type { SignalRootCommand } from "../signals/protocol.js";
import { ContextRegistry } from "../context/registry.js";
import { defineContext } from "../context/definition.js";
import { ContextActor, contextPath } from "../context/actor.js";
import { GoalSettings } from "../config/settings.js";
import { Context, Deferred, Effect, Layer, Match, Ref, Result, Schema } from "effect";
import { AgentConversations } from "@aster/agent/harness";
import type { ActorContext, ActorRef } from "@aster/actor";
import { randomUUID } from "node:crypto";
import { CurrentActors } from "../services/actors.js";

type Services = ExternalAgents | Layer.Services<typeof GoalAgent.layer> | AgentConversations;
type Owner = ActorContext<GoalMailbox, Services | ContextRegistry>;
type Attempt = { generation: string; cancellation: Deferred.Deferred<void> };

export class GoalActor extends ContextActor.Service<GoalActor, Services>()("goals/Actor", {
  command: GoalMailbox,
  context: defineContext({ state: GoalSnapshot, message: Schema.Unknown }),
}) {
  static readonly layer = Layer.effect(
    GoalActor,
    Effect.gen(function* () {
      const agent = yield* GoalAgent;
      const scope = yield* Effect.scope;
      const initialized = yield* Deferred.make<GoalState["Service"]>();
      const registry = yield* ContextRegistry;
      const settings = yield* GoalSettings;
      const incarnation = randomUUID();
      // Only lifecycle/mailbox handlers access these refs. Workers return through pipeToSelf.
      const running = yield* Ref.make<Attempt | undefined>(undefined);
      const gating = yield* Ref.make<(Attempt & { inputId: string }) | undefined>(undefined);
      const wake = (context: Owner) => context.self.tell({ _tag: "RunNext" });
      const endPeers = Effect.fnUntraced(function* (context: Owner) {
        const gate = yield* Ref.getAndSet(gating, undefined);
        if (gate) yield* Deferred.succeed(gate.cancellation, undefined);
        yield* cancelGoalTasks(context, registry, contextPath(context));
        yield* context.pipeToSelf(
          context
            .select("/user/signals")
            .resolve()
            .pipe(
              Effect.flatMap((root) =>
                (root as ActorRef<SignalRootCommand>).ask<void>((replyTo) => ({
                  _tag: "PauseByOwner",
                  owner: contextPath(context),
                  replyTo,
                })),
              ),
            ),
          (result) => ({ _tag: "PeersEnded", generation: incarnation, result }),
        );
      });

      // Read-only screening has its own slot. Pending inputs survive interruption and can be screened again.
      const screenNext = Effect.fnUntraced(function* (context: Owner, model: GoalState["Service"]) {
        if (yield* Ref.get(gating)) return;
        const state = yield* model.read;
        const input = state.inputs.find(
          (input) =>
            input.status === "pending" &&
            input.kind === "GoalIntent" &&
            input.relevant === undefined &&
            input.remainingAgentTurns > 0,
        );
        if (!input) return;
        const { payload } = yield* model.resolve(input);
        if (payload._tag !== "GoalIntent")
          return yield* Effect.die(
            new Error("Context input reference does not match its Pi payload"),
          );
        const attempt = {
          generation: randomUUID(),
          cancellation: yield* Deferred.make<void>(),
          inputId: input.inputId,
        };
        yield* Ref.set(gating, attempt);
        yield* context.pipeToSelf(
          agent
            .screen(state.definition, payload.intent)
            .pipe(
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
      });
      const runConversation = Effect.fnUntraced(function* (
        context: Owner,
        model: GoalState["Service"],
        input: StoredGoalInput,
        attempt: Attempt,
      ) {
        const resolved = yield* model.resolve(input);
        const goal = (yield* model.read).definition;
        yield* context.pipeToSelf(
          agent
            .converse({
              goal,
              input: resolved,
            })
            .pipe(
              Effect.provideService(CurrentActors, context),
              Effect.provideService(GoalState, model),
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
      const handleRequest = Effect.fnUntraced(function* (
        command: GoalCommand,
        context: Owner,
        model: GoalState["Service"],
      ) {
        const request = Schema.decodeUnknownSync(GoalRequestData)(command);
        const busy = (yield* Ref.get(running)) !== undefined;
        const result = yield* model.accept(request, busy).pipe(Effect.result);
        if (Result.isFailure(result))
          return yield* command.replyTo.tell({ _tag: "Rejected", error: result.failure });
        yield* command.replyTo.tell({ _tag: "Accepted", receipt: result.success.receipt });
        if (result.success.replayed) return;
        if (request._tag === "End") {
          const attempt = yield* Ref.getAndSet(running, undefined);
          if (attempt) yield* Deferred.succeed(attempt.cancellation, undefined);
          yield* endPeers(context);
        } else yield* wake(context);
      });
      const runNext = Effect.fnUntraced(function* (context: Owner, model: GoalState["Service"]) {
        const state = yield* model.read;
        if (state.status !== "active") return;
        // An uncertain delivery blocks later inputs. Restart inspects it using the same Pi identity.
        if (state.inputs.some((input) => input.status === "unknown")) return;
        yield* screenNext(context, model);
        if (yield* Ref.get(running)) return;
        const pending = state.inputs.filter((input) => input.status === "pending");
        // Recover the original Pi submission first. Otherwise users precede ready background inputs.
        const input =
          state.inputs.find((input) => input.status === "running") ??
          pending.find((input) => input.kind === "UserInput") ??
          pending.find(
            (input) =>
              input.kind !== "GoalIntent" ||
              input.relevant === true ||
              input.remainingAgentTurns <= 0,
          );
        if (!input) return;
        if (input.remainingAgentTurns <= 0) {
          yield* model.exhaust(input);
          yield* wake(context);
          return;
        }
        const attempt = { generation: randomUUID(), cancellation: yield* Deferred.make<void>() };
        yield* Ref.set(running, attempt);
        yield* model.start(input.inputId);
        yield* runConversation(context, model, input, attempt);
      });
      const settleGate = Effect.fnUntraced(function* (
        command: Extract<GoalMailbox, { _tag: "GateSettled" }>,
        context: Owner,
        model: GoalState["Service"],
      ) {
        const attempt = yield* Ref.get(gating);
        if (attempt?.generation !== command.generation || attempt.inputId !== command.inputId)
          return;
        yield* Ref.set(gating, undefined);
        const state = yield* model.read;
        const input = state.inputs.find((input) => input.inputId === command.inputId);
        if (state.status !== "active" || input?.status !== "pending") return;
        yield* model.recordGate(
          command.inputId,
          command.result._tag === "Success"
            ? Result.succeed(command.result.value)
            : Result.fail(command.result.error),
        );
        yield* wake(context);
      });
      const settleConversation = Effect.fnUntraced(function* (
        command: Extract<GoalMailbox, { _tag: "ConversationSettled" }>,
        context: Owner,
        model: GoalState["Service"],
      ) {
        if ((yield* Ref.get(running))?.generation !== command.generation) return;
        yield* Ref.set(running, undefined);
        yield* model.settle(
          command.inputId,
          command.result._tag === "Success"
            ? Result.succeed(command.result.value)
            : Result.fail(command.result.error),
        );
        yield* wake(context);
      });
      return GoalActor.of({
        started: (context) =>
          Effect.gen(function* () {
            const slug = context.path.split("/").at(-1)!;
            const definition = settings.definitions.find((goal) => goal.slug === slug);
            if (!definition) return yield* Effect.die(new Error(`Unknown Goal ${slug}`));
            const services = yield* Layer.buildWithScope(
              GoalState.layer(contextPath(context), definition),
              scope,
            );
            yield* Deferred.succeed(initialized, Context.get(services, GoalState));
            yield* wake(context);
          }),
        receive: (command, context) =>
          Effect.gen(function* () {
            const model = yield* Deferred.await(initialized);
            yield* Match.value(command).pipe(
              Match.tag("AttachTask", (command) =>
                model.attachTask(command.taskPath).pipe(
                  Effect.matchEffect({
                    onSuccess: () => command.replyTo.tell({ _tag: "Attached" }),
                    onFailure: (error) => command.replyTo.tell({ _tag: "Rejected", error }),
                  }),
                ),
              ),
              Match.tag("PeersEnded", (command) =>
                Effect.gen(function* () {
                  if (command.generation !== incarnation) return;
                  if (command.result._tag === "Failure")
                    yield* Effect.logError(command.result.error);
                }),
              ),
              Match.tag("SubmitInput", "End", "RetryTurn", (command) =>
                handleRequest(command, context, model),
              ),
              Match.tag("RunNext", () => runNext(context, model)),
              Match.tag("GateSettled", (command) => settleGate(command, context, model)),
              Match.tag("ConversationSettled", (command) =>
                settleConversation(command, context, model),
              ),
              Match.exhaustive,
              Effect.orDie,
            );
          }),
      });
    }),
  ).pipe(Layer.provide(GoalAgent.layer));
}
