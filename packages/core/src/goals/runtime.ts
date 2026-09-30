import { GoalToolError } from "./tasks.js";
import { Context, Effect, Schema } from "effect";
import { choice } from "../decisions/system-one.js";
import type { ActorRef, AskTimeoutError } from "@aster/actor";
import type { ContextRecord } from "../context/model.js";
import type { ContextRegistry } from "../context/registry.js";
import type { GoalSettings } from "../config/settings.js";
import type { GoalReasoner } from "./reasoner.js";
import type { GoalCommand } from "./actors.js";
import {
  SignalDefinition,
  validateSignalTime,
  type CoreConfig,
  type GoalDefinition,
} from "../config/schema.js";
import type { SignalRootCommand } from "../signals/actors.js";
import type { SystemOneClient } from "../decisions/system-one.js";
import type { GoalHistory } from "./history.js";
import type { SignalToolRequest } from "./tasks.js";

/** Goal coordination stays in the caller Fiber; only external reasoning uses Promise adapters. */
export class GoalRuntime extends Context.Service<
  GoalRuntime,
  {
    readonly definitions: readonly GoalDefinition[];
    readonly reasoner: GoalReasoner;
    readonly history?: GoalHistory;
    readonly contextTokens?: number;
    readonly reserveTokens?: number;
    readonly signals: (goal: string) => readonly SignalDefinition[];
    readonly reconcile: (
      goal: string,
      subscriber: ActorRef<GoalCommand>,
    ) => Effect.Effect<readonly ActorRef<unknown>[], GoalToolError | AskTimeoutError>;
    readonly editSignal?: (
      goal: string,
      request: SignalToolRequest,
      subscriber: ActorRef<GoalCommand>,
    ) => Effect.Effect<unknown, GoalToolError | AskTimeoutError>;
    readonly deactivate: (goal: string) => Effect.Effect<void, GoalToolError | AskTimeoutError>;
  }
>()("goals/Runtime") {}

export const makeGoalRuntime = (
  config: GoalSettings["Service"],
  registry: ContextRegistry["Service"],
  root: Pick<ActorRef<SignalRootCommand>, "ask">,
  reasoner: GoalReasoner,
  history?: GoalHistory,
): GoalRuntime["Service"] => {
  const subscribers = new Map<string, ActorRef<GoalCommand>>();
  const records = (goal: string) =>
    Object.values(registry.snapshot())
      .map((r) => ({ ...r, state: r.state as Record<string, unknown> }))
      .filter((r) => /^\/signals\/[^/]+$/.test(r.path) && r.state.goal === goal);
  const signals = (goal: string) =>
    records(goal)
      .filter((r) => r.state.active !== false && !r.state.deleted)
      .map((r) => r.state as SignalDefinition);
  const upsert = (
    definition: SignalDefinition,
    goal: string,
    active: boolean,
    subscriber: ActorRef<GoalCommand>,
    deleted = false,
  ) =>
    Effect.gen(function* () {
      const existing = registry.get(`/signals/${definition.slug}`);
      if (existing && (existing.state as { goal?: string }).goal !== goal)
        return yield* new GoalToolError({ message: "Signal belongs to another owner" });
      return yield* root.ask<ActorRef<unknown>>((replyTo) => ({
        _tag: "Upsert",
        definition,
        goal,
        active,
        deleted,
        subscriber,
        replyTo,
      }));
    });

  return {
    definitions: config.definitions,
    reasoner,
    history,
    contextTokens: config.reasoning?.contextTokens,
    reserveTokens: config.reasoning?.reserveTokens,
    signals,
    deactivate: (goal) =>
      Effect.gen(function* () {
        const subscriber = subscribers.get(goal);
        if (subscriber)
          for (const definition of signals(goal))
            yield* upsert(definition, goal, false, subscriber);
      }),
    reconcile: (goal, subscriber) =>
      Effect.gen(function* () {
        subscribers.set(goal, subscriber);
        const refs = [];
        for (const record of records(goal))
          refs.push(
            yield* upsert(
              record.state as SignalDefinition,
              goal,
              (registry.get(`/goals/${goal}`)?.state as { status?: string } | undefined)?.status !==
                "completed" && record.state.active !== false,
              subscriber,
              !!record.state.deleted,
            ),
          );
        return refs;
      }),
    editSignal: (goal, request, subscriber) =>
      Effect.gen(function* () {
        const all = records(goal);
        if (request.operation === "signal_list")
          return all.filter((r) => !r.state.deleted).map((r) => r.state);
        if (!request.id || !/^[a-z0-9][a-z0-9-]*$/.test(request.id))
          return yield* new GoalToolError({ message: "Invalid Signal ID" });
        const slug = request.id.startsWith(`${goal}--`) ? request.id : `${goal}--${request.id}`;
        const existing = all.find((r) => r.state.slug === slug);
        if (request.operation === "signal_get") {
          if (!existing) return yield* new GoalToolError({ message: "Signal not found" });
          return existing.state;
        }
        if (request.operation === "signal_create" && existing) return existing.state;
        if (
          request.operation !== "signal_create" &&
          (!existing || existing.state.deleted || request.revision !== existing.state.revision)
        )
          return yield* new GoalToolError({
            message: "Signal missing, deleted or revision changed",
          });
        const deleting = request.operation === "signal_delete";
        const patch = request.operation === "signal_delete" ? {} : request.definition;
        const raw: Record<string, unknown> = {
          ...existing?.state,
          ...patch,
          slug,
          when: patch.when ?? existing?.state.when ?? "Check the latest Goal progress on schedule",
          task:
            patch.task ??
            existing?.state.task ??
            "Evaluate changes, record conclusions, or advance tasks",
          agent: "doubao-delegate",
          mode: "confirm",
        };
        if (raw.schedule === null) delete raw.schedule;
        if (raw.notBefore === null) delete raw.notBefore;
        if (raw.taskId === null) delete raw.taskId;
        if (
          patch.taskId &&
          !(registry.get(`/goals/${goal}`)?.state as { tasks?: { id: string }[] })?.tasks?.some(
            (task) => task.id === patch.taskId,
          )
        )
          return yield* new GoalToolError({
            message: "Signal taskId must reference a task in this Goal",
          });
        let definition: SignalDefinition;
        try {
          definition = Schema.decodeUnknownSync(SignalDefinition)(raw);
          validateSignalTime(definition);
        } catch (cause) {
          return yield* new GoalToolError({ message: String(cause) });
        }
        if (!definition.when.trim() || !definition.task.trim())
          return yield* new GoalToolError({ message: "Signal condition must not be empty" });
        yield* upsert(definition, goal, !deleting, subscriber, deleting);
        return registry.get(`/signals/${slug}`)!.state;
      }),
  };
};

export const relevantGoals = (
  client: SystemOneClient,
  record: ContextRecord,
  goals: CoreConfig["goals"],
) =>
  Effect.gen(function* () {
    if (!goals.length) return [];
    const result = yield* client.systemOne({
      state: JSON.stringify({
        path: record.path,
        description: record.description,
        state: record.state,
      }),
      questions: Object.fromEntries(
        goals.map((goal, index) => [
          `goal_${index}`,
          choice(
            `Does this Context contain information relevant to this Goal: ${goal.description}?`,
            {
              yes: "Relevant evidence may change progress, blockers, or useful Signals.",
              no: "Unrelated to this Goal.",
            },
          ),
        ]),
      ),
    });
    return goals.filter((_, index) => {
      const answer = result.answers[`goal_${index}`];
      return answer?.type === "choice" && answer.choice === "yes";
    });
  });
