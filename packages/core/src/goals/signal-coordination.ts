import { ApplicationError, CommandReceipt, CausalChain } from "@aster/api-contracts";
import { GoalToolError } from "./tasks.js";
import { Context, Effect, Layer, Schema } from "effect";
import type { ActorRef, AskTimeoutError } from "@aster/actor";
import { ContextRegistry } from "../context/registry.js";
import type { GoalCommand } from "./actors.js";
import { SignalDefinition } from "../config/schema.js";
import type { GoalSignalInput } from "../signals/goal-command.js";
import type {
  SignalCommandReply,
  SignalConfigureReply,
  SignalRootCommand,
} from "../signals/actors.js";
import { SignalCommands } from "../signals/commands.js";

/** Goal coordination stays in the caller Fiber; only external reasoning uses Promise adapters. */
export class GoalSignals extends Context.Service<
  GoalSignals,
  {
    readonly signals: (goal: string) => readonly SignalDefinition[];
    readonly reconcile: (
      goal: string,
      subscriber: ActorRef<GoalCommand>,
    ) => Effect.Effect<readonly ActorRef<unknown>[], GoalToolError | AskTimeoutError>;
    readonly applySignal?: (
      input: GoalSignalInput,
      subscriber: ActorRef<GoalCommand>,
    ) => Effect.Effect<CommandReceipt, ApplicationError>;
    readonly deactivate: (
      goal: string,
      subscriber: ActorRef<GoalCommand>,
    ) => Effect.Effect<void, GoalToolError | AskTimeoutError>;
  }
>()("goals/Signals") {
  static readonly layer = Layer.effect(
    GoalSignals,
    Effect.gen(function* () {
      return makeGoalSignalCommands(yield* ContextRegistry, yield* SignalCommands);
    }),
  );
}

export const makeGoalSignalCommands = (
  registry: ContextRegistry["Service"],
  root: Pick<ActorRef<SignalRootCommand>, "ask">,
): GoalSignals["Service"] => {
  const records = (goal: string) =>
    Object.values(registry.snapshot())
      .map((r) => ({ ...r, state: r.state as Record<string, unknown> }))
      .filter((r) => /^\/signals\/[^/]+$/.test(r.path) && r.state.goal === goal);
  const signals = (goal: string) =>
    records(goal)
      .filter((r) => r.state.active !== false && !r.state.deleted)
      .map((r) => Schema.decodeUnknownSync(SignalDefinition)(r.state));
  const upsert = (
    definition: SignalDefinition,
    goal: string,
    active: boolean,
    subscriber: ActorRef<GoalCommand>,
    deleted = false,
    causal?: CausalChain,
  ) =>
    Effect.gen(function* () {
      const existing = registry.get(`/signals/${definition.slug}`);
      if (existing && (existing.state as { goal?: string }).goal !== goal)
        return yield* new GoalToolError({ message: "Signal belongs to another owner" });
      const result = yield* root.ask<SignalConfigureReply>((replyTo) => ({
        _tag: "Upsert",
        causal,
        definition,
        goal,
        active,
        deleted,
        subscriber,
        replyTo,
      }));
      if (result._tag === "Rejected")
        return yield* new GoalToolError({ message: result.error.message });
      return result.ref;
    });

  return {
    signals,
    deactivate: (goal, subscriber) =>
      Effect.gen(function* () {
        for (const definition of signals(goal)) yield* upsert(definition, goal, false, subscriber);
      }),
    reconcile: (goal, subscriber) =>
      Effect.gen(function* () {
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
    applySignal: (input, subscriber) =>
      root
        .ask<SignalCommandReply>((replyTo) => ({
          _tag: "ApplyGoalCommand",
          input,
          subscriber,
          replyTo,
        }))
        .pipe(
          Effect.catchTag("AskTimeoutError", () =>
            Effect.fail(
              new ApplicationError({
                kind: "unavailable",
                message: "Signal acknowledgement was not received",
              }),
            ),
          ),
          Effect.flatMap((result) =>
            result._tag === "Accepted" ? Effect.succeed(result.receipt) : Effect.fail(result.error),
          ),
        ),
  };
};
