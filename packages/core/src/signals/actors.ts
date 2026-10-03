import { SignalReactionInput, acceptSignalReaction } from "./reaction.js";
import {
  CausalChain,
  ApplicationError,
  SignalDeliveryInput,
  SignalDeliveryReceipt,
} from "@aster/api-contracts";
import { applyPersonalSignal } from "./command.js";
import { applyGoalSignal, GoalSignalInput } from "./goal-command.js";
import { scheduledCausalChain, signalEnabled, sourceSignalEligible } from "./policy.js";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { ReplyTo, type ActorContext, type ActorRef } from "@aster/actor";
import { ContextActor, childActorName, spawnContextChild } from "../context/actor.js";
import { defineContext, ContextRecord } from "../context/model.js";
import { ContextRegistry } from "../context/registry.js";
import { Clock, Cron, Context, Effect, Layer, Schedule, Schema } from "effect";
import { SignalDefinition, validateSignalTime } from "../config/schema.js";
import { TaskPreparation, ExternalAgents } from "../tasks/model.js";
import type { GoalCommand } from "../goals/actors.js";
import { SignalRunActor, type RunCommand } from "../tasks/run.js";
import { SignalState as DurableSignalState } from "./state.js";
import { signalNotifications } from "../notifications/signal.js";

export class SignalDefinitions extends Context.Service<
  SignalDefinitions,
  ReadonlyArray<SignalDefinition>
>()("signals/Definitions") {}

interface Occurrence {
  causal?: CausalChain;
  id: string;
  text: string;
  delivered: boolean;
  source: ContextRecord;
}
interface SignalState extends SignalDefinition {
  causal?: CausalChain;
  goal?: string;
  owner?: string;
  active?: boolean;
  deleted?: boolean;
  revision?: number;
  seenSources?: readonly string[];
  nextDue?: number;
  timerDone?: boolean;
  occurrences?: readonly Occurrence[];
}
export const SignalCommandReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: SignalDeliveryReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type SignalCommandReply = typeof SignalCommandReply.Type;
export const SignalConfigureReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { ref: ReplyTo<unknown>() }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type SignalConfigureReply = typeof SignalConfigureReply.Type;
const ApplyPersonalCommand = Schema.TaggedStruct("ApplyPersonalCommand", {
  input: SignalDeliveryInput,
  replyTo: ReplyTo<SignalCommandReply>(),
});
const ApplyGoalCommand = Schema.TaggedStruct("ApplyGoalCommand", {
  input: GoalSignalInput,
  subscriber: ReplyTo<GoalCommand>(),
  replyTo: ReplyTo<SignalCommandReply>(),
});
const ReactCommand = Schema.TaggedStruct("React", {
  input: SignalReactionInput,
  replyTo: ReplyTo<SignalCommandReply>(),
});
const ReadyCommand = Schema.TaggedStruct("Ready", { replyTo: ReplyTo<void>() });
const SignalCommand = Schema.Union([
  ReadyCommand,
  Schema.TaggedStruct("ReadyAfterConfiguration", { replyTo: ReplyTo<void>() }),
  ReactCommand,
  ApplyPersonalCommand,
  ApplyGoalCommand,
  Schema.TaggedStruct("Trigger", { sourceContext: ContextRecord }),
  Schema.TaggedStruct("Tick", { revision: Schema.Number, due: Schema.Number }),
  Schema.TaggedStruct("DeliverOccurrences", {}),
  Schema.TaggedStruct("DeliveryFinished", { id: Schema.String, accepted: Schema.Boolean }),
  Schema.TaggedStruct("Configure", {
    causal: Schema.optional(CausalChain),
    definition: SignalDefinition,
    goal: Schema.optional(Schema.String),
    subscriber: Schema.optional(ReplyTo<GoalCommand>()),
    active: Schema.Boolean,
    deleted: Schema.optional(Schema.Boolean),
    replyTo: Schema.optional(ReplyTo<SignalConfigureReply>()),
  }),
  Schema.TaggedStruct("Recover", {}),
]);
type SignalCommand = typeof SignalCommand.Type;

export class SignalActor extends ContextActor.Service<
  SignalActor,
  SignalDefinitions | TaskPreparation | ExternalAgents
>()("signals/SignalActor", {
  command: SignalCommand,
  context: defineContext({
    identity: "Condition and schedule monitoring",
    state: Schema.Record(Schema.String, Schema.Unknown),
    message: Schema.Never,
  }),
}) {
  static readonly layer = Layer.effect(
    SignalActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry,
        definitions = yield* SignalDefinitions;
      const agents = yield* ExternalAgents;
      let subscriber: ActorRef<GoalCommand> | undefined,
        path = "";
      let armed = "";
      let retryScheduled = false;
      const inFlight = new Set<string>();
      const state = () => registry.get(path)?.state as SignalState | undefined;
      const save = Effect.fn("Signal.save")(function* (patch: object) {
        const current = registry.get(path)!;
        const next = { ...current.state, ...patch } as Record<string, unknown>;
        if (next.goal === undefined) delete next.goal;
        next.businessOutbox = signalNotifications({
          path,
          revision: (current.revision ?? 0) + 1,
          at: new Date(yield* Clock.currentTimeMillis).toISOString(),
          previous: Schema.decodeUnknownSync(DurableSignalState)(current.state),
          next: Schema.decodeUnknownSync(DurableSignalState)(next),
        });
        yield* registry
          .commit({ ...current, state: next }, { expectedRevision: current.revision ?? 0 })
          .pipe(Effect.asVoid, Effect.orDie);
      });
      const enabled = () =>
        !!state() && signalEnabled(state()!, (slug) => registry.get(`/goals/${slug}`));
      const nextTime = (s: SignalState, now: number) =>
        s.schedule?.type === "once"
          ? Math.max(Date.parse(s.schedule.at), s.notBefore ? Date.parse(s.notBefore) : 0)
          : s.schedule?.type === "cron"
            ? Cron.next(
                Cron.parseUnsafe(s.schedule.expression, s.schedule.timeZone),
                Math.max(now, s.notBefore ? Date.parse(s.notBefore) - 1 : now),
              ).getTime()
            : undefined;
      const arm = (context: ActorContext<SignalCommand, any>) =>
        Effect.gen(function* () {
          const s = state()!;
          if (!enabled() || !s.schedule || s.timerDone) {
            armed = "";
            return;
          }
          const now = yield* Clock.currentTimeMillis;
          const due = s.nextDue ?? nextTime(s, now)!;
          if (s.nextDue === undefined) yield* save({ nextDue: due });
          const key = `${s.revision}:${due}`;
          if (armed === key) return;
          armed = key;
          yield* context.pipeToSelf(
            Effect.schedule(
              Effect.void,
              Schedule.duration(Math.min(86400000, Math.max(0, due - now))),
            ),
            () => ({ _tag: "Tick", revision: s.revision ?? 1, due }),
          );
        });
      const dispatch = (context: ActorContext<SignalCommand, any>) =>
        Effect.gen(function* () {
          if (!enabled()) return;
          const pending = state()?.occurrences?.filter((o) => !o.delivered) ?? [];
          for (const occurrence of pending) {
            if (inFlight.has(occurrence.id)) continue;
            let delivery: Effect.Effect<boolean, import("@aster/actor").AskTimeoutError>;
            if (state()!.goal) {
              if (!subscriber) continue;
              delivery = subscriber
                .ask<{ accepted: boolean }>((replyTo) => ({
                  _tag: "Occurrence",
                  id: occurrence.id,
                  signalPath: path,
                  causal: occurrence.causal,
                  text: occurrence.text,
                  replyTo,
                }))
                .pipe(Effect.map((reply) => reply.accepted));
            } else {
              const id = createHash("sha256").update(occurrence.id).digest("hex").slice(0, 32);
              const runPath = `${path}/runs/${id}`;
              if (registry.get(runPath)) {
                delivery = Effect.succeed(true);
              } else {
                const run =
                  ((yield* context.child(childActorName(`runs/${id}`))) as
                    ActorRef<RunCommand> | undefined) ??
                  (yield* spawnContextChild(context, `runs/${id}`, SignalRunActor).pipe(
                    Effect.orDie,
                  ));
                delivery = run
                  .ask<void>((replyTo) => ({
                    _tag: "Initialize",
                    path: runPath,
                    definition: Schema.decodeUnknownSync(SignalDefinition)(state()!),
                    sourceContext: occurrence.source,
                    causal: occurrence.causal,
                    replyTo,
                  }))
                  .pipe(Effect.as(true));
              }
            }
            // Persisted occurrences are the outbox. Waiting for durable receipt runs
            // outside the mailbox, so Configure and Tick can still make progress.
            inFlight.add(occurrence.id);
            yield* context.pipeToSelf(delivery, (result) => ({
              _tag: "DeliveryFinished",
              id: occurrence.id,
              accepted: result._tag === "Success" && result.value,
            }));
          }
          if (!retryScheduled && state()?.occurrences?.some((o) => !o.delivered)) {
            retryScheduled = true;
            yield* context.pipeToSelf(Effect.sleep("3 seconds"), () => ({
              _tag: "DeliverOccurrences",
            }));
          }
        });
      return SignalActor.of({
        started: (context) =>
          Effect.gen(function* () {
            const slug = context.path.split("/").at(-1)!;
            path = `/signals/${slug}`;
            if (state())
              yield* Schema.decodeUnknownEffect(DurableSignalState)(state()).pipe(Effect.orDie);
            const definition = definitions.find((d) => d.slug === slug);
            if (definition && state()?.owner === "/personal")
              return yield* Effect.die(
                new Error("Configured Signal collides with a Personal-owned Signal"),
              );
            if (definition) {
              validateSignalTime(definition);
              if (!state())
                yield* registry
                  .commit(
                    {
                      path,
                      description: `Signal：${slug}`,
                      state: {
                        ...definition,
                        active: true,
                        revision: 1,
                        occurrences: [],
                        nextDue: nextTime(definition, yield* Clock.currentTimeMillis),
                      },
                      messages: [],
                    },
                    { expectedRevision: 0 },
                  )
                  .pipe(Effect.asVoid, Effect.orDie);
              yield* context.self.tell({ _tag: "Configure", definition, active: true });
            } else if (state()) {
              const goal = state()!.goal;
              if (goal) {
                const resolved = yield* context
                  .select(`/user/goals/${goal}`)
                  .resolve()
                  .pipe(Effect.option);
                if (resolved._tag === "Some") subscriber = resolved.value as ActorRef<GoalCommand>;
              }
              yield* context.self.tell({ _tag: "Recover" });
            }
          }),
        receive: (command, context) =>
          Effect.gen(function* () {
            switch (command._tag) {
              case "Ready":
                // A root can queue Ready before started queues Configure. Requeue the
                // barrier behind startup commands before acknowledging the catalogue.
                return yield* context.self.tell({
                  _tag: "ReadyAfterConfiguration",
                  replyTo: command.replyTo,
                });
              case "ReadyAfterConfiguration":
                return yield* command.replyTo.tell(undefined);
              case "React": {
                const result = yield* acceptSignalReaction(registry, path, command.input).pipe(
                  Effect.result,
                );
                if (result._tag === "Failure")
                  yield* command.replyTo.tell({ _tag: "Rejected", error: result.failure });
                else {
                  yield* command.replyTo.tell({ _tag: "Accepted", receipt: result.success });
                  yield* dispatch(context);
                }
                return;
              }
              case "ApplyGoalCommand": {
                const now = yield* Clock.currentTimeMillis;
                const result = yield* applyGoalSignal({
                  registry,
                  path,
                  raw: command.input,
                  configured: definitions,
                  agents: Object.keys(agents),
                  nextDue: (definition) => nextTime(definition, now),
                }).pipe(Effect.result);
                if (result._tag === "Failure")
                  yield* command.replyTo.tell({ _tag: "Rejected", error: result.failure });
                else {
                  subscriber = command.subscriber;
                  yield* command.replyTo.tell({ _tag: "Accepted", receipt: result.success });
                  yield* context.self.tell({ _tag: "Recover" });
                }
                return;
              }
              case "ApplyPersonalCommand": {
                const now = yield* Clock.currentTimeMillis;
                const result = yield* applyPersonalSignal({
                  registry,
                  path,
                  raw: command.input,
                  configured: definitions,
                  agents: Object.keys(agents),
                  nextDue: (definition) => nextTime(definition, now),
                }).pipe(Effect.result);
                if (result._tag === "Failure")
                  yield* command.replyTo.tell({ _tag: "Rejected", error: result.failure });
                else {
                  yield* command.replyTo.tell({ _tag: "Accepted", receipt: result.success });
                  yield* context.self.tell({ _tag: "Recover" });
                }
                return;
              }
              case "Configure": {
                // Personal definitions may only change through the versioned command protocol.
                if (
                  state()?.owner === "/personal" ||
                  (state()?.goal !== undefined && state()?.goal !== command.goal)
                ) {
                  if (command.replyTo)
                    yield* command.replyTo.tell({
                      _tag: "Rejected",
                      error: new ApplicationError({
                        kind: "conflict",
                        message: "Signal belongs to another owner",
                      }),
                    });
                  return;
                }
                if (command.goal !== undefined && command.definition.action !== undefined) {
                  if (command.replyTo)
                    yield* command.replyTo.tell({
                      _tag: "Rejected",
                      error: new ApplicationError({
                        kind: "invalid-input",
                        message:
                          "Goal Signals deliver evidence to the Goal; publication actions require a Task-producing Signal",
                      }),
                    });
                  return;
                }
                validateSignalTime(command.definition);
                subscriber = command.subscriber;
                const snapshot = registry.get(path);
                const previous = snapshot?.state as SignalState | undefined;
                const content = (s: SignalState) =>
                  JSON.stringify({
                    slug: s.slug,
                    goal: s.goal,
                    when: s.when,
                    task: s.task,
                    action: s.action,
                    ...(s.taskId === undefined ? {} : { taskId: s.taskId }),
                    agent: s.agent,
                    mode: s.mode,
                    schedule: s.schedule,
                    notBefore: s.notBefore,
                    active: s.active,
                    deleted: !!s.deleted,
                  });
                const proposed = {
                  ...Schema.decodeUnknownSync(SignalDefinition)(command.definition),
                  ...(command.goal === undefined ? {} : { goal: command.goal }),
                  active: command.active,
                  deleted: !!command.deleted,
                };
                const changed = !previous || content(previous) !== content(proposed);
                const now = yield* Clock.currentTimeMillis;
                // Reattachment must not invalidate frozen commands by advancing Context revision.
                if (
                  changed ||
                  (command.causal && !isDeepStrictEqual(command.causal, previous?.causal))
                )
                  yield* registry
                    .commit(
                      {
                        path,
                        description: `Signal：${command.definition.slug}`,
                        state: {
                          ...previous,
                          ...proposed,
                          action: proposed.action,
                          schedule: proposed.schedule,
                          notBefore: proposed.notBefore,
                          taskId: proposed.taskId,
                          ...(command.causal ? { causal: command.causal } : {}),
                          revision: (previous?.revision ?? 0) + (changed ? 1 : 0),
                          ...(changed
                            ? { nextDue: nextTime(proposed, now), timerDone: false }
                            : {}),
                        },
                        messages: [],
                      },
                      { expectedRevision: snapshot?.revision ?? 0 },
                    )
                    .pipe(Effect.asVoid, Effect.orDie);
                if (command.replyTo)
                  yield* command.replyTo.tell({
                    _tag: "Accepted",
                    ref: context.self as ActorRef<unknown>,
                  });
                yield* context.self.tell({ _tag: "Recover" });
                return;
              }
              case "Recover": {
                if (!state()) return;
                for (const record of Object.values(registry.snapshot())) {
                  if (!record.path.startsWith(`${path}/runs/`)) continue;
                  const relative = `runs/${record.path.split("/").at(-1)!}`;
                  const ref =
                    ((yield* context.child(childActorName(relative))) as
                      ActorRef<RunCommand> | undefined) ??
                    (yield* spawnContextChild(context, relative, SignalRunActor).pipe(
                      Effect.orDie,
                    ));
                  yield* ref.tell({ _tag: "Resume", path: record.path, subscriber });
                }
                yield* arm(context);
                yield* dispatch(context);
                return;
              }
              case "DeliverOccurrences":
                retryScheduled = false;
                yield* dispatch(context);
                return;
              case "DeliveryFinished":
                inFlight.delete(command.id);
                if (!command.accepted) return;
                yield* save({
                  occurrences: state()!.occurrences!.map((o) =>
                    o.id === command.id ? { ...o, delivered: true } : o,
                  ),
                });
                return;
              case "Tick": {
                const s = state();
                if (
                  !s ||
                  !enabled() ||
                  command.revision !== s.revision ||
                  command.due !== s.nextDue ||
                  s.timerDone
                )
                  return;
                armed = "";
                const now = yield* Clock.currentTimeMillis;
                if (now < command.due) {
                  yield* arm(context);
                  return;
                }
                const id = `${path}:${s.revision}:time:${command.due}`;
                const causal = scheduledCausalChain(s, id);
                const source = {
                  path,
                  description: registry.get(path)!.description,
                  state: {
                    slug: s.slug,
                    goal: s.goal,
                    when: s.when,
                    task: s.task,
                    action: s.action,
                    taskId: s.taskId,
                    agent: s.agent,
                    mode: s.mode,
                    ...(s.schedule === undefined ? {} : { schedule: s.schedule }),
                    ...(s.notBefore === undefined ? {} : { notBefore: s.notBefore }),
                    ...(s.goal === undefined ? {} : { goal: s.goal }),
                    ...(s.revision === undefined ? {} : { revision: s.revision }),
                  },
                  messages: [
                    {
                      type: "Timer",
                      scheduledAt: new Date(command.due).toISOString(),
                      observedAt: new Date(now).toISOString(),
                    },
                  ],
                };
                yield* save({
                  timerDone: s.schedule?.type === "once",
                  nextDue: s.schedule?.type === "cron" ? nextTime(s, now) : undefined,
                  occurrences: [
                    ...(s.occurrences ?? []),
                    {
                      id,
                      text: `Scheduled check: ${s.when}\nRelated task: ${s.taskId ?? "None"}\nScheduled time: ${new Date(command.due).toISOString()}\nRead the current evidence before deciding whether work is needed.`,
                      source,
                      ...(causal ? { causal } : {}),
                      delivered: false,
                    },
                  ],
                });
                yield* dispatch(context);
                yield* arm(context);
                return;
              }
              case "Trigger": {
                const s = state();
                const now = yield* Clock.currentTimeMillis;
                if (!s || !sourceSignalEligible(s, now, (slug) => registry.get(`/goals/${slug}`)))
                  return;
                const { through: _through, ...sourceState } = command.sourceContext.state as Record<
                  string,
                  unknown
                >;
                const sourceKey = createHash("sha256")
                  .update(
                    JSON.stringify({
                      path: command.sourceContext.path,
                      state: sourceState,
                      messages: command.sourceContext.messages,
                    }),
                  )
                  .digest("hex");
                if (s.seenSources?.includes(sourceKey)) return;
                const id = `${path}:source:${sourceKey}`;
                yield* save({
                  seenSources: [...(s.seenSources ?? []), sourceKey],
                  occurrences: [
                    ...(s.occurrences ?? []),
                    {
                      id,
                      text: `Condition: ${s.when}\nRelated task: ${s.taskId ?? "None"}\nSource: ${command.sourceContext.path}\nRead the source Context; the matched snapshot is stored in the occurrences at ${path}.`,
                      source: command.sourceContext,
                      ...(s.causal ? { causal: s.causal } : {}),
                      delivered: false,
                    },
                  ],
                });
                yield* dispatch(context);
                return;
              }
            }
          }),
      });
    }),
  );
}

export const SignalRootCommand = Schema.Union([
  ReadyCommand,
  ReactCommand,
  ApplyPersonalCommand,
  ApplyGoalCommand,
  Schema.TaggedStruct("Trigger", {
    slug: Schema.String,
    sourceContext: ContextRecord,
  }),
  Schema.TaggedStruct("Upsert", {
    causal: Schema.optional(CausalChain),
    definition: SignalDefinition,
    goal: Schema.String,
    subscriber: ReplyTo<GoalCommand>(),
    active: Schema.Boolean,
    deleted: Schema.optional(Schema.Boolean),
    replyTo: ReplyTo<SignalConfigureReply>(),
  }),
]);
export type SignalRootCommand = typeof SignalRootCommand.Type;
export class SignalRootActor extends ContextActor.Service<
  SignalRootActor,
  SignalDefinitions | TaskPreparation | ExternalAgents
>()("signals/RootActor", {
  command: SignalRootCommand,
  context: defineContext({ identity: "Signals", state: Schema.Struct({}), message: Schema.Never }),
}) {
  static readonly layer = Layer.effect(
    SignalRootActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry,
        definitions = yield* SignalDefinitions;
      return SignalRootActor.of({
        started: (context) =>
          Effect.gen(function* () {
            yield* registry
              .commit(
                {
                  path: "/signals",
                  description: "Condition and schedule monitoring",
                  state: {},
                  messages: [],
                },
                { expectedRevision: registry.get("/signals")?.revision ?? 0 },
              )
              .pipe(Effect.asVoid, Effect.orDie);
            const slugs = new Set([
              ...definitions.map((d) => d.slug),
              ...Object.keys(registry.snapshot())
                .filter((p) => /^\/signals\/[^/]+$/.test(p))
                .map((p) => p.split("/").at(-1)!),
            ]);
            for (const slug of slugs) {
              const child =
                ((yield* context.child(slug)) as ActorRef<SignalCommand> | undefined) ??
                (yield* context.spawn(slug, SignalActor));
              const record = registry.get(`/signals/${slug}`);
              if (
                record &&
                !(record.state as { goal?: string }).goal &&
                (record.state as { owner?: string }).owner !== "/personal" &&
                !definitions.some((d) => d.slug === slug)
              )
                yield* child.tell({
                  _tag: "Configure",
                  definition: record.state as SignalDefinition,
                  active: false,
                });
            }
          }),
        receive: (command, context) =>
          Effect.gen(function* () {
            if (command._tag === "Ready") {
              for (const child of yield* context.children())
                yield* (child as ActorRef<SignalCommand>)
                  .ask<void>((replyTo) => ({ _tag: "Ready", replyTo }))
                  .pipe(Effect.orDie);
              yield* command.replyTo.tell(undefined);
            } else if (command._tag === "React") {
              const decoded = yield* Schema.decodeUnknownEffect(SignalReactionInput)(
                command.input,
              ).pipe(Effect.result);
              if (decoded._tag === "Failure")
                return yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "invalid-input",
                    message: "Invalid Signal reaction",
                  }),
                });
              const child = yield* context.child(decoded.success.target.slice("/signals/".length));
              if (child)
                yield* (child as ActorRef<SignalCommand>).tell({
                  ...command,
                  input: decoded.success,
                });
              else
                yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "not-found",
                    message: "Signal Actor unavailable",
                  }),
                });
            } else if (command._tag === "ApplyGoalCommand") {
              const decoded = yield* Schema.decodeUnknownEffect(GoalSignalInput)(
                command.input,
              ).pipe(Effect.result);
              if (decoded._tag === "Failure")
                return yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "invalid-input",
                    message: "Invalid Goal Signal command",
                  }),
                });
              const slug = decoded.success.target.slice("/signals/".length);
              const child =
                (yield* context.child(slug)) ??
                (yield* context.spawn(slug, SignalActor).pipe(Effect.orDie));
              yield* (child as ActorRef<SignalCommand>).tell({
                ...command,
                input: decoded.success,
              });
            } else if (command._tag === "ApplyPersonalCommand") {
              const decoded = yield* Schema.decodeUnknownEffect(SignalDeliveryInput)(
                command.input,
              ).pipe(Effect.result);
              if (decoded._tag === "Failure")
                return yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "invalid-input",
                    message: "Invalid Personal Signal command",
                  }),
                });
              const slug = decoded.success.target.slice("/signals/".length);
              const child =
                (yield* context.child(slug)) ??
                (yield* context.spawn(slug, SignalActor).pipe(Effect.orDie));
              yield* (child as ActorRef<SignalCommand>).tell({
                ...command,
                input: decoded.success,
              });
            } else if (command._tag === "Upsert") {
              const existing = yield* context.child(command.definition.slug);
              const child =
                (existing as ActorRef<SignalCommand> | undefined) ??
                (yield* context.spawn(command.definition.slug, SignalActor).pipe(Effect.orDie));
              yield* child.tell({ ...command, _tag: "Configure" });
            } else {
              const child = yield* context.child(command.slug);
              if (child)
                yield* (child as ActorRef<SignalCommand>).tell({
                  _tag: "Trigger",
                  sourceContext: command.sourceContext,
                });
            }
          }),
      });
    }),
  );
}
