import { ReplyTo, type ActorContext, type ActorRef } from "@aster/actor";
import { ApplicationError, CommandReceipt, TaskMessage } from "@aster/api-contracts";
import { Clock, Context, Cron, Effect, Layer, Match, Schema } from "effect";
import { isDeepStrictEqual } from "node:util";
import { ContextActor } from "../context/actor.js";
import { defineContext } from "../context/definition.js";
import { ContextRegistry } from "../context/registry.js";
import { SignalDefinition, validateSignalTime } from "../config/schema.js";
import { deliverTask } from "../tasks/message.js";
import { SignalState } from "./state.js";
import { GoalSignalInput, applyGoalSignal } from "./goal-command.js";
import { SignalReactionInput, acceptSignalReaction } from "./reaction.js";
import { scheduledCausalChain, signalEnabled } from "./policy.js";

export class SignalDefinitions extends Context.Service<
  SignalDefinitions,
  readonly SignalDefinition[]
>()("signals/Definitions") {}
export const SignalCommandReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type SignalCommandReply = typeof SignalCommandReply.Type;
const Ready = Schema.TaggedStruct("Ready", { replyTo: ReplyTo<void>() });
const Activate = Schema.TaggedStruct("Activate", {});
const React = Schema.TaggedStruct("React", {
  input: SignalReactionInput,
  replyTo: ReplyTo<SignalCommandReply>(),
});
const ApplyGoalCommand = Schema.TaggedStruct("ApplyGoalCommand", {
  input: GoalSignalInput,
  replyTo: ReplyTo<SignalCommandReply>(),
});
const Deactivate = Schema.TaggedStruct("Deactivate", {
  goal: Schema.String,
  replyTo: ReplyTo<void>(),
});
const Command = Schema.Union([
  Ready,
  Activate,
  React,
  ApplyGoalCommand,
  Deactivate,
  Schema.TaggedStruct("Tick", { revision: Schema.Int, due: Schema.Number }),
  Schema.TaggedStruct("Dispatch", {}),
  Schema.TaggedStruct("Delivered", {
    id: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: CommandReceipt }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(ApplicationError) }),
    ]),
  }),
]);
type Command = typeof Command.Type;
export const nextSignalTime = (definition: SignalDefinition, now: number): number | undefined =>
  Match.value(definition.trigger).pipe(
    Match.tag("Context", () => undefined),
    Match.tag("Schedule", ({ schedule }) =>
      Match.value(schedule).pipe(
        Match.when({ type: "once" }, ({ at }) => Date.parse(at)),
        Match.when({ type: "cron" }, ({ expression, timeZone }) =>
          Cron.next(Cron.parseUnsafe(expression, timeZone), now).getTime(),
        ),
        Match.exhaustive,
      ),
    ),
    Match.exhaustive,
  );

export class SignalActor extends ContextActor.Service<SignalActor, SignalDefinitions>()(
  "signals/SignalActor",
  {
    command: Command,
    context: defineContext({ state: SignalState, message: Schema.Never }),
  },
) {
  static readonly layer = Layer.effect(
    SignalActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const definitions = yield* SignalDefinitions;
      let path = "",
        activated = false,
        armed = "",
        retryScheduled = false;
      const inFlight = new Set<string>();
      const state = () => registry.get(path)?.state as SignalState | undefined;
      const save = Effect.fn("Signal.save")(function* (patch: Partial<SignalState>) {
        const current = registry.get(path)!;
        yield* registry
          .commit(
            { ...current, state: { ...current.state, ...patch } },
            { expectedRevision: current.revision },
          )
          .pipe(Effect.orDie);
      });
      const enabled = () =>
        activated && !!state() && signalEnabled(state()!, (slug) => registry.get(`/goals/${slug}`));
      const arm = Effect.fn("Signal.arm")(function* (actor: ActorContext<Command>) {
        const current = state();
        if (!current || !enabled() || current.trigger._tag !== "Schedule" || current.timerDone) {
          armed = "";
          return;
        }
        const due = current.nextDue ?? nextSignalTime(current, yield* Clock.currentTimeMillis)!;
        if (current.nextDue === undefined) yield* save({ nextDue: due });
        const key = `${current.revision}:${due}`;
        if (armed === key) return;
        armed = key;
        yield* actor.pipeToSelf(
          Effect.sleep(Math.min(86400000, Math.max(0, due - (yield* Clock.currentTimeMillis)))),
          () => ({ _tag: "Tick", revision: current.revision, due }),
        );
      });
      const dispatch = Effect.fn("Signal.dispatch")(function* (actor: ActorContext<Command>) {
        if (!enabled()) return;
        for (const item of state()!.occurrences) {
          if (item.delivered || item.error || inFlight.has(item.message.requestId)) continue;
          inFlight.add(item.message.requestId);
          yield* actor.pipeToSelf(deliverTask(actor, item.message), (result) => ({
            _tag: "Delivered",
            id: item.message.requestId,
            result,
          }));
        }
      });
      return SignalActor.of({
        started: (actor) =>
          Effect.gen(function* () {
            path = `/signals/${actor.path.split("/").at(-1)!}`;
            const definition = definitions.find((item) => `/signals/${item.slug}` === path);
            const current = registry.get(path);
            if (definition) {
              if (definition.trigger._tag === "Schedule") validateSignalTime(definition.trigger);
              if (current && (current.state as SignalState).goal)
                return yield* Effect.die(
                  new Error("Configured Signal collides with Goal ownership"),
                );
              const old = current && Schema.decodeUnknownSync(SignalState)(current.state);
              const changed =
                !old ||
                !isDeepStrictEqual(Schema.decodeUnknownSync(SignalDefinition)(old), definition);
              if (changed)
                yield* registry
                  .commit(
                    {
                      path,
                      description: `Signal: ${definition.slug}`,
                      state: {
                        ...old,
                        ...definition,
                        active: true,
                        revision: (old?.revision ?? 0) + 1,
                        occurrences: old?.occurrences ?? [],
                        nextDue: nextSignalTime(definition, yield* Clock.currentTimeMillis),
                        timerDone: false,
                      },
                      messages: [],
                    },
                    { expectedRevision: current?.revision ?? 0 },
                  )
                  .pipe(Effect.orDie);
            } else if (current && !(current.state as SignalState).goal)
              yield* save({ active: false });
          }),
        receive: (command, actor) =>
          Match.value(command).pipe(
            Match.tag("Ready", ({ replyTo }) => replyTo.tell(undefined)),
            Match.tag("Activate", () =>
              Effect.gen(function* () {
                activated = true;
                yield* arm(actor);
                yield* dispatch(actor);
              }),
            ),
            Match.tag("Deactivate", ({ goal, replyTo }) =>
              Effect.gen(function* () {
                if (state()?.goal === goal)
                  yield* save({ active: false, revision: state()!.revision + 1 });
                yield* replyTo.tell(undefined);
              }),
            ),
            Match.tag("ApplyGoalCommand", ({ input, replyTo }) =>
              Effect.gen(function* () {
                const now = yield* Clock.currentTimeMillis;
                const result = yield* applyGoalSignal({
                  registry,
                  path,
                  raw: input,
                  configured: definitions,
                  nextDue: (definition) => nextSignalTime(definition, now),
                }).pipe(Effect.result);
                if (result._tag === "Failure")
                  return yield* replyTo.tell({ _tag: "Rejected", error: result.failure });
                activated = true;
                yield* replyTo.tell({ _tag: "Accepted", receipt: result.success });
                yield* arm(actor);
                yield* dispatch(actor);
              }),
            ),
            Match.tag("React", ({ input, replyTo }) =>
              Effect.gen(function* () {
                const result = yield* acceptSignalReaction(registry, path, input).pipe(
                  Effect.result,
                );
                if (result._tag === "Failure")
                  return yield* replyTo.tell({ _tag: "Rejected", error: result.failure });
                yield* replyTo.tell({ _tag: "Accepted", receipt: result.success });
                yield* dispatch(actor);
              }),
            ),
            Match.tag("Tick", (command) =>
              Effect.gen(function* () {
                const current = state();
                if (
                  !current ||
                  !enabled() ||
                  command.revision !== current.revision ||
                  current.nextDue !== command.due ||
                  current.timerDone
                )
                  return;
                armed = "";
                const now = yield* Clock.currentTimeMillis;
                if (now < command.due) return yield* arm(actor);
                const requestId = `${path}:timer:${current.revision}:${command.due}`;
                const message: TaskMessage = {
                  requestId,
                  source: path,
                  task: current.task,
                  createdAt: new Date(command.due).toISOString(),
                  causal: scheduledCausalChain(current, requestId),
                };
                yield* save({
                  occurrences: [...current.occurrences, { message, delivered: false }],
                  timerDone:
                    current.trigger._tag === "Schedule" && current.trigger.schedule.type === "once",
                  nextDue: nextSignalTime(current, now),
                });
                yield* dispatch(actor);
                yield* arm(actor);
              }),
            ),
            Match.tag("Dispatch", () =>
              Effect.gen(function* () {
                retryScheduled = false;
                yield* dispatch(actor);
              }),
            ),
            Match.tag("Delivered", ({ id, result }) =>
              Effect.gen(function* () {
                inFlight.delete(id);
                const retry = result._tag === "Failure" && result.error.kind === "unavailable";
                yield* save({
                  occurrences: state()!.occurrences.map((item) =>
                    item.message.requestId !== id
                      ? item
                      : {
                          ...item,
                          delivered: result._tag === "Success",
                          ...(!retry && result._tag === "Failure"
                            ? { error: result.error.message }
                            : {}),
                        },
                  ),
                });
                if (retry && !retryScheduled) {
                  retryScheduled = true;
                  yield* actor.pipeToSelf(Effect.sleep("3 seconds"), () => ({ _tag: "Dispatch" }));
                }
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}
export const SignalRootCommand = Schema.Union([
  Ready,
  Activate,
  React,
  ApplyGoalCommand,
  Deactivate,
]);
export type SignalRootCommand = typeof SignalRootCommand.Type;
export class SignalRootActor extends ContextActor.Service<SignalRootActor, SignalDefinitions>()(
  "signals/RootActor",
  {
    command: SignalRootCommand,
    context: defineContext({ state: Schema.Struct({}), message: Schema.Never }),
  },
) {
  static readonly layer = Layer.effect(
    SignalRootActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry,
        definitions = yield* SignalDefinitions;
      return SignalRootActor.of({
        started: (actor) =>
          Effect.gen(function* () {
            yield* registry
              .commit(
                {
                  path: "/signals",
                  description: "Context and scheduled Tasks",
                  state: {},
                  messages: [],
                },
                { expectedRevision: registry.get("/signals")?.revision ?? 0 },
              )
              .pipe(Effect.orDie);
            const slugs = new Set([
              ...definitions.map((d) => d.slug),
              ...Object.keys(registry.snapshot())
                .filter((p) => /^\/signals\/[^/]+$/.test(p))
                .map((p) => p.split("/").at(-1)!),
            ]);
            for (const slug of slugs) yield* actor.spawn(slug, SignalActor);
          }),
        receive: (command, actor) =>
          Match.value(command).pipe(
            Match.tag("Ready", ({ replyTo }) =>
              Effect.gen(function* () {
                for (const child of yield* actor.children())
                  yield* (child as ActorRef<Command>)
                    .ask<void>((replyTo) => ({ _tag: "Ready", replyTo }))
                    .pipe(Effect.orDie);
                yield* replyTo.tell(undefined);
              }),
            ),
            Match.tag("Activate", () =>
              Effect.gen(function* () {
                for (const child of yield* actor.children())
                  yield* (child as ActorRef<Command>).tell({ _tag: "Activate" });
              }),
            ),
            Match.tag("Deactivate", ({ goal, replyTo }) =>
              Effect.gen(function* () {
                for (const child of yield* actor.children())
                  yield* (child as ActorRef<Command>)
                    .ask<void>((replyTo) => ({ _tag: "Deactivate", goal, replyTo }))
                    .pipe(Effect.orDie);
                yield* replyTo.tell(undefined);
              }),
            ),
            Match.tag("React", "ApplyGoalCommand", (command) =>
              Effect.gen(function* () {
                const decoded =
                  command._tag === "React"
                    ? yield* Schema.decodeUnknownEffect(SignalReactionInput)(command.input).pipe(
                        Effect.result,
                      )
                    : yield* Schema.decodeUnknownEffect(GoalSignalInput)(command.input).pipe(
                        Effect.result,
                      );
                if (decoded._tag === "Failure")
                  return yield* command.replyTo.tell({
                    _tag: "Rejected",
                    error: new ApplicationError({
                      kind: "invalid-input",
                      message: "Invalid Signal command",
                    }),
                  });
                const slug = decoded.success.target.slice("/signals/".length);
                let child = (yield* actor.child(slug)) as ActorRef<Command> | undefined;
                if (!child && command._tag === "ApplyGoalCommand")
                  child = yield* actor.spawn(slug, SignalActor).pipe(Effect.orDie);
                if (!child)
                  return yield* command.replyTo.tell({
                    _tag: "Rejected",
                    error: new ApplicationError({
                      kind: "not-found",
                      message: "Signal unavailable",
                    }),
                  });
                yield* (child as ActorRef<Command>).tell(command);
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}
