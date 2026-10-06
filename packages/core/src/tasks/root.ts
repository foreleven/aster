import type { TaskServices } from "./actor.js";
import { taskActorPath } from "./address.js";
import {
  ApplicationError,
  TaskDeliveryInput,
  TaskPath,
  ResumeTaskDeliveryInput,
  FollowupTaskInput,
} from "@aster/api-contracts";
import { Effect, Layer, Schema } from "effect";
import { ContextActor } from "../context/actor.js";
import { ContextRegistry } from "../context/registry.js";
import { defineContext } from "../context/definition.js";
import { TaskState } from "./state.js";
import { taskPathFor } from "./admission.js";
import { TaskActor } from "./actor.js";
import { StartTask, ResumeTask, FollowupTask, TaskReady } from "./protocol.js";
import type { ActorRef } from "@aster/actor";
import type { TaskCommand } from "./protocol.js";

const Command = Schema.Union([StartTask, ResumeTask, FollowupTask, TaskReady]);
export type TasksRootCommand = typeof Command.Type;
/** Owns asynchronous Tasks sent to delegate Actors; conversations do not own execution. */
export class TasksRootActor extends ContextActor.Service<TasksRootActor, TaskServices>()(
  "tasks/Root",
  {
    command: Command,
    context: defineContext({
      state: Schema.Struct({}),
      message: Schema.Never,
    }),
  },
) {
  static readonly layer = Layer.effect(
    TasksRootActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      return TasksRootActor.of({
        started: (actor) =>
          Effect.gen(function* () {
            if (!registry.get("/tasks"))
              yield* registry
                .commit(
                  { path: "/tasks", description: "Persistent Tasks", state: {}, messages: [] },
                  { expectedRevision: 0 },
                )
                .pipe(Effect.orDie);
            for (const record of Object.values(registry.snapshot())) {
              if (!Schema.is(TaskPath)(record.path)) continue;
              const state = Schema.decodeUnknownSync(TaskState)(record.state);
              if (taskPathFor(state.admission.source, state.inputs[0]!.requestId) !== record.path)
                continue;
              yield* actor.spawn(record.path.slice("/tasks/".length), TaskActor).pipe(Effect.orDie);
            }
          }),
        receive: (command, actor) =>
          Effect.gen(function* () {
            if (command._tag === "Ready") {
              for (const child of yield* actor.children())
                yield* (child as ActorRef<TaskCommand>)
                  .ask<void>((replyTo) => ({ _tag: "Ready", replyTo }))
                  .pipe(Effect.orDie);
              return yield* command.replyTo.tell(undefined);
            }
            if (command._tag === "ResumeTask" || command._tag === "FollowupTask") {
              const input = yield* Schema.decodeUnknownEffect(
                command._tag === "ResumeTask" ? ResumeTaskDeliveryInput : FollowupTaskInput,
              )(command.input).pipe(Effect.result);
              if (input._tag === "Failure")
                return yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "invalid-input",
                    message: "Invalid Task resumption",
                  }),
                });
              const path = input.success.target;
              if (!registry.get(path))
                return yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({ kind: "not-found", message: "Task not found" }),
                });
              const runtimePath = taskActorPath(path);
              const target = yield* actor.select(runtimePath).resolve().pipe(Effect.option);
              if (target._tag === "None")
                return yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "unavailable",
                    message: "Task owner is unavailable; no replacement owner was created",
                  }),
                });
              return yield* target.value.tell(command);
            }
            const decoded = yield* Schema.decodeUnknownEffect(TaskDeliveryInput)(
              command.input,
            ).pipe(Effect.result);
            if (
              decoded._tag === "Failure" ||
              decoded.success.target !==
                taskPathFor(decoded.success.source, decoded.success.requestId)
            )
              return yield* command.replyTo.tell({
                _tag: "Rejected",
                error: new ApplicationError({
                  kind: "invalid-input",
                  message: "Invalid Task command identity",
                }),
              });
            const name = decoded.success.target.slice("/tasks/".length);
            const child =
              ((yield* actor.child(name)) as ActorRef<TaskCommand> | undefined) ??
              (yield* actor.spawn(name, TaskActor).pipe(Effect.orDie));
            yield* child.tell({ ...command, input: decoded.success });
          }),
      });
    }),
  );
}
