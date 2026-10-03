import { runActorPath } from "./address.js";
import { ApplicationError, TaskDeliveryInput, ResumeRunDeliveryInput } from "@aster/api-contracts";
import { Effect, Layer, Schema } from "effect";
import { ContextActor } from "../context/actor.js";
import { ContextRegistry } from "../context/registry.js";
import { defineContext } from "../context/model.js";
import { ExternalAgents, TaskPreparation } from "./model.js";
import { RunState } from "./run-state.js";
import { personalTaskPath } from "./admission.js";
import { SignalRunActor, StartPersonalTask, ResumePersonalRun } from "./run.js";
import type { ActorRef } from "@aster/actor";
import type { RunCommand } from "./run.js";

const Command = Schema.Union([StartPersonalTask, ResumePersonalRun]);
export type RunRootCommand = typeof Command.Type;
/** Owns only independent Personal Runs; Goal and Signal children keep their original owners. */
export class RunRootActor extends ContextActor.Service<
  RunRootActor,
  TaskPreparation | ExternalAgents
>()("tasks/Root", {
  command: Command,
  context: defineContext({
    identity: "One-time tasks",
    state: Schema.Struct({}),
    message: Schema.Never,
  }),
}) {
  static readonly layer = Layer.effect(
    RunRootActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      return RunRootActor.of({
        started: (actor) =>
          Effect.gen(function* () {
            if (!registry.get("/runs"))
              yield* registry
                .commit(
                  { path: "/runs", description: "One-time tasks", state: {}, messages: [] },
                  { expectedRevision: 0 },
                )
                .pipe(Effect.orDie);
            for (const record of Object.values(registry.snapshot())) {
              if (!/^\/runs\/personal--[a-f0-9]{64}$/.test(record.path)) continue;
              const state = Schema.decodeUnknownSync(RunState)(record.state);
              if (!state.admission || state.admission.input.target !== record.path) continue;
              yield* actor
                .spawn(record.path.slice("/runs/".length), SignalRunActor)
                .pipe(Effect.orDie);
            }
          }),
        receive: (command, actor) =>
          Effect.gen(function* () {
            if (command._tag === "ResumePersonalRun") {
              const input = yield* Schema.decodeUnknownEffect(ResumeRunDeliveryInput)(
                command.input,
              ).pipe(Effect.result);
              if (input._tag === "Failure")
                return yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "invalid-input",
                    message: "Invalid Run resumption",
                  }),
                });
              const path = input.success.target;
              if (!registry.get(path))
                return yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({ kind: "not-found", message: "Run not found" }),
                });
              const runtimePath = runActorPath(path);
              const target = yield* actor.select(runtimePath).resolve().pipe(Effect.option);
              if (target._tag === "None")
                return yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "unavailable",
                    message: "Run owner is unavailable; no replacement owner was created",
                  }),
                });
              return yield* target.value.tell({ ...command, input: input.success });
            }
            const decoded = yield* Schema.decodeUnknownEffect(TaskDeliveryInput)(
              command.input,
            ).pipe(Effect.result);
            if (
              decoded._tag === "Failure" ||
              decoded.success.target !== personalTaskPath(decoded.success.requestId)
            )
              return yield* command.replyTo.tell({
                _tag: "Rejected",
                error: new ApplicationError({
                  kind: "invalid-input",
                  message: "Invalid Task command identity",
                }),
              });
            const name = decoded.success.target.slice("/runs/".length);
            const child =
              ((yield* actor.child(name)) as ActorRef<RunCommand> | undefined) ??
              (yield* actor.spawn(name, SignalRunActor).pipe(Effect.orDie));
            yield* child.tell({ ...command, input: decoded.success });
          }),
      });
    }),
  );
}
