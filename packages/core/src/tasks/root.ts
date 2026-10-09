import { ContextSession } from "../context/session.js";
import { Effect, Match, Schema } from "effect";
import { ContextActor } from "../context/actor.js";
import { ContextRegistry } from "../context/registry.js";
import { ApplicationError } from "../operations.js";
import { TaskActor } from "./actor.js";
import { FollowupTaskInput, TaskDeliveryInput, TaskPath, TaskRecoveryInput } from "./contracts.js";
import { taskActorPath } from "./delivery.js";
import { taskPathFor } from "./state/admission.js";
import { TaskSnapshot } from "./state/snapshot.js";
import { queryTasks, TasksQueries } from "./queries.js";

import { CommandProcessor, type ActorRef } from "@aster/actor";
import { CheckTask, Input, RetryTask, StartTask, type TaskCommand } from "./protocol.js";

export type TasksRootCommand = StartTask | CheckTask | RetryTask | Input;
/** Owns asynchronous Tasks sent to delegate Actors; conversations do not own execution. */
export const TasksRootActor = ContextActor.define("tasks/Root", {
  commands: [StartTask, CheckTask, RetryTask, Input, ...TasksQueries],
})(
  Effect.gen(function* () {
    const processor = yield* CommandProcessor.make({ concurrency: 2 });
    const registry = yield* ContextRegistry;
    yield* ContextSession.make({
      path: "/tasks",
      state: Schema.Struct({}),
      message: Schema.Never,
      initial: { state: {}, description: "Persistent Tasks" },
    }).pipe(Effect.orDie);
    return {
      started: (actor) =>
        Effect.gen(function* () {
          for (const record of Object.values(registry.snapshot())) {
            if (!Schema.is(TaskPath)(record.path)) continue;
            const state = Schema.decodeUnknownSync(TaskSnapshot)(record.state);
            if (taskPathFor(state.admission.source, state.inputs[0]!.requestId) !== record.path)
              continue;
            const name = record.path.slice("/tasks/".length);
            const child = (yield* actor.child(name)) ?? (yield* actor.spawn(name, TaskActor));
            yield* actor.watch(child);
          }
        }),
      receiveSignal: (signal) =>
        Effect.logError({
          event: "task.actor.terminated",
          actorPath: signal.ref.path,
          cause: signal.cause,
        }),
      receive: (command, actor) =>
        Match.value(command).pipe(
          Match.tag("list", "read", (request) =>
            processor.submit(request, actor, queryTasks(request, actor)),
          ),
          Match.tag("Input", "CheckTask", "RetryTask", (command) =>
            Effect.gen(function* () {
              const input = yield* Schema.decodeUnknownEffect(
                command._tag === "Input" ? FollowupTaskInput : TaskRecoveryInput,
              )(command.input).pipe(Effect.result);
              if (input._tag === "Failure")
                return yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "invalid-input",
                    message: "Invalid Task input",
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
            }),
          ),
          Match.tag("StartTask", (command) =>
            Effect.gen(function* () {
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
              yield* actor.watch(child);
              yield* child.tell({ ...command, input: decoded.success });
            }),
          ),
          Match.exhaustive,
        ),
    };
  }),
);
