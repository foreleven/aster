import { signalEnabled, sourceSignalEligible } from "./policy.js";
import { createHash } from "node:crypto";
import { ReplyTo, type ActorContext, type ActorRef } from "@aster/actor";
import { ContextActor, childActorName, spawnContextChild } from "../context/actor.js";
import { defineContext, ContextRecord } from "../context/model.js";
import { ContextRegistry } from "../context/registry.js";
import { Clock, Cron, Context, Effect, Layer, Schedule, Schema } from "effect";
import { SignalDefinition, validateSignalTime } from "../config/schema.js";
import { TaskPreparation, ExternalAgents } from "../tasks/model.js";
import type { GoalCommand } from "../goals/actors.js";
import { SignalRunActor, type RunCommand } from "../tasks/run.js";

export class SignalDefinitions extends Context.Service<
  SignalDefinitions,
  ReadonlyArray<SignalDefinition>
>()("signals/Definitions") {}

interface Occurrence {
  id: string;
  text: string;
  delivered: boolean;
  source: ContextRecord;
}
interface SignalState extends SignalDefinition {
  goal?: string;
  active?: boolean;
  deleted?: boolean;
  revision?: number;
  seenSources?: readonly string[];
  nextDue?: number;
  timerDone?: boolean;
  occurrences?: readonly Occurrence[];
}
const SignalCommand = Schema.Union([
  Schema.TaggedStruct("Trigger", { sourceContext: ContextRecord }),
  Schema.TaggedStruct("Tick", { revision: Schema.Number, due: Schema.Number }),
  Schema.TaggedStruct("DeliverOccurrences", {}),
  Schema.TaggedStruct("DeliveryFinished", { id: Schema.String, accepted: Schema.Boolean }),
  Schema.TaggedStruct("Configure", {
    definition: SignalDefinition,
    goal: Schema.optional(Schema.String),
    subscriber: Schema.optional(ReplyTo<GoalCommand>()),
    active: Schema.Boolean,
    deleted: Schema.optional(Schema.Boolean),
    replyTo: Schema.optional(ReplyTo<ActorRef<unknown>>()),
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
      let subscriber: ActorRef<GoalCommand> | undefined,
        path = "";
      let armed = "";
      let retryScheduled = false;
      const inFlight = new Set<string>();
      const state = () => registry.get(path)?.state as SignalState | undefined;
      const save = (patch: object) =>
        registry.set({ ...registry.get(path)!, state: { ...state(), ...patch } });
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
            const definition = definitions.find((d) => d.slug === slug);
            if (definition) {
              validateSignalTime(definition);
              if (!state())
                yield* registry.set({
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
                });
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
              case "Configure": {
                validateSignalTime(command.definition);
                subscriber = command.subscriber;
                const previous = state();
                const content = (s: SignalState) =>
                  JSON.stringify({
                    slug: s.slug,
                    when: s.when,
                    task: s.task,
                    taskId: s.taskId,
                    agent: s.agent,
                    mode: s.mode,
                    schedule: s.schedule,
                    notBefore: s.notBefore,
                    active: s.active,
                    deleted: !!s.deleted,
                  });
                const proposed = {
                  ...Schema.decodeUnknownSync(SignalDefinition)(command.definition),
                  goal: command.goal,
                  active: command.active,
                  deleted: !!command.deleted,
                };
                const changed = !previous || content(previous) !== content(proposed);
                const now = yield* Clock.currentTimeMillis;
                yield* registry.set({
                  path,
                  description: `Signal：${command.definition.slug}`,
                  state: {
                    ...previous,
                    ...proposed,
                    revision: (previous?.revision ?? 0) + (changed ? 1 : 0),
                    ...(changed ? { nextDue: nextTime(proposed, now), timerDone: false } : {}),
                  },
                  messages: [],
                });
                if (command.replyTo) yield* command.replyTo.tell(context.self as ActorRef<unknown>);
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
                const source = {
                  path,
                  description: registry.get(path)!.description,
                  state: {
                    slug: s.slug,
                    when: s.when,
                    task: s.task,
                    taskId: s.taskId,
                    agent: s.agent,
                    mode: s.mode,
                    schedule: s.schedule,
                    notBefore: s.notBefore,
                    goal: s.goal,
                    revision: s.revision,
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
  Schema.TaggedStruct("Trigger", {
    slug: Schema.String,
    sourceContext: ContextRecord,
  }),
  Schema.TaggedStruct("Upsert", {
    definition: SignalDefinition,
    goal: Schema.String,
    subscriber: ReplyTo<GoalCommand>(),
    active: Schema.Boolean,
    deleted: Schema.optional(Schema.Boolean),
    replyTo: ReplyTo<ActorRef<unknown>>(),
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
            yield* registry.set({
              path: "/signals",
              description: "Condition and schedule monitoring",
              state: {},
              messages: [],
            });
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
            if (command._tag === "Upsert") {
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
