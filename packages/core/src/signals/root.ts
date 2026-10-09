import { ContextSession } from "../context/session.js";
import { CommandProcessor, type ActorContext, type ActorRef } from "@aster/actor";
import { Effect, Match, Schema } from "effect";
import { ContextActor } from "../context/actor.js";
import { ContextRegistry } from "../context/registry.js";
import { ApplicationError } from "../operations.js";
import { SignalActor } from "./actor.js";
import {
  SignalChangeInput,
  type SignalCommand,
  SignalDefinitions,
  SignalReactionInput,
  type SignalRootCommand,
  SignalRootCommands,
  SignalRootInternal,
} from "./protocol.js";
import { listSignals, querySignals, SignalsQueries } from "./queries.js";
import { SignalSnapshot } from "./state/snapshot.js";

const register = Effect.fnUntraced(function* (
  actor: ActorContext<SignalRootCommand>,
  slug: string,
) {
  const child =
    (yield* actor.child(slug)) ??
    (yield* actor.spawn(slug, SignalActor, {
      metadata: { signalActivation: actor.metadata.signalActivation },
    }));
  yield* actor.watch(child);
  return child as ActorRef<SignalCommand>;
});
export const SignalRootActor = ContextActor.define("signals/RootActor", {
  commands: [...SignalRootCommands, ...SignalsQueries],
  internal: SignalRootInternal,
})(
  Effect.gen(function* () {
    const processor = yield* CommandProcessor.make({ concurrency: 2 });
    const registry = yield* ContextRegistry;
    const definitions = yield* SignalDefinitions;
    yield* ContextSession.make({
      path: "/signals",
      state: Schema.Struct({}),
      message: Schema.Never,
      initial: { state: {}, description: "Context and scheduled Tasks" },
    }).pipe(Effect.orDie);
    return {
      started: (actor) =>
        Effect.gen(function* () {
          const slugs = new Set([
            ...definitions.map((definition) => definition.slug),
            ...Object.keys(registry.snapshot())
              .filter((path) => /^\/signals\/[^/]+$/.test(path))
              .map((path) => path.slice("/signals/".length)),
          ]);
          for (const slug of slugs) yield* register(actor, slug);
        }),
      receiveSignal: (signal) =>
        Effect.logError({
          event: "signal.actor.terminated",
          actorPath: signal.ref.path,
          cause: signal.cause,
        }),
      receive: (command, actor) =>
        Match.value(command).pipe(
          Match.tag("list", "read", (request) =>
            processor.submit(request, actor, querySignals(request, actor)),
          ),
          Match.tag("ListByOwner", (request) =>
            processor.submit(
              request,
              actor,
              listSignals(actor).pipe(
                Effect.map((records) =>
                  records.filter(
                    (record) =>
                      Schema.decodeUnknownSync(SignalSnapshot)(record.state).owner ===
                      request.owner,
                  ),
                ),
                Effect.mapError(
                  (error) => new ApplicationError({ kind: "unavailable", message: error.message }),
                ),
              ),
            ),
          ),
          Match.tag("PauseByOwner", ({ owner, replyTo }) =>
            Effect.gen(function* () {
              const children = yield* actor.children();
              yield* actor.pipeToSelf(
                Effect.forEach(
                  children,
                  (child) =>
                    (child as ActorRef<SignalCommand>)
                      .ask<void>((replyTo) => ({ _tag: "PauseByOwner", owner, replyTo }))
                      .pipe(Effect.orDie),
                  { concurrency: "unbounded", discard: true },
                ),
                () => ({ _tag: "OwnerPaused", replyTo }),
              );
            }),
          ),
          Match.tag("OwnerPaused", ({ replyTo }) => replyTo.tell(undefined)),
          Match.tag("React", "Change", (command) =>
            Effect.gen(function* () {
              const decoded = yield* command._tag === "React"
                ? Schema.decodeUnknownEffect(SignalReactionInput)(command.input).pipe(Effect.result)
                : Schema.decodeUnknownEffect(SignalChangeInput)(command.input).pipe(Effect.result);
              if (decoded._tag === "Failure")
                return yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "invalid-input",
                    message: "Invalid Signal command",
                  }),
                });
              const slug = decoded.success.target.slice("/signals/".length);
              let child = (yield* actor.child(slug)) as ActorRef<SignalCommand> | undefined;
              if (
                !child &&
                command._tag === "Change" &&
                command.input.change.operation === "create" &&
                !registry.get(decoded.success.target)
              )
                child = yield* register(actor, slug).pipe(Effect.orDie);
              if (!child)
                return yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "not-found",
                    message: "Signal unavailable",
                  }),
                });
              yield* child.tell(command);
            }),
          ),
          Match.exhaustive,
        ),
    };
  }),
);
