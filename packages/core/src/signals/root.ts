import { AgentConversations } from "@aster/agent";
import { type ActorContext, type ActorRef } from "@aster/actor";
import { ApplicationError } from "../operations.js";
import { Effect, Layer, Match, Schema } from "effect";
import { ContextActor } from "../context/actor.js";
import { defineContext } from "../context/definition.js";
import { ContextRegistry } from "../context/registry.js";
import { SignalActor } from "./actor.js";
import {
  SignalChangeInput,
  SignalReactionInput,
  SignalCommand,
  SignalDefinitions,
  SignalRootCommand,
} from "./protocol.js";
import { SignalSnapshot } from "./state/snapshot.js";

type Services = SignalDefinitions | AgentConversations;
const register = Effect.fnUntraced(function* (
  actor: ActorContext<SignalRootCommand, Services | ContextRegistry>,
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
export class SignalRootActor extends ContextActor.Service<SignalRootActor, Services>()(
  "signals/RootActor",
  {
    command: SignalRootCommand,
    context: defineContext({ state: Schema.Struct({}), message: Schema.Never }),
  },
) {
  static readonly layer = Layer.effect(
    SignalRootActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const definitions = yield* SignalDefinitions;
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
            Match.tag("ListByOwner", ({ owner, replyTo }) =>
              replyTo.tell({
                _tag: "Success",
                value: Object.values(registry.reader.snapshot()).filter(
                  (record) =>
                    /^\/signals\/[^/]+$/.test(record.path) &&
                    Schema.decodeUnknownSync(SignalSnapshot)(record.state).owner === owner,
                ),
              }),
            ),
            Match.tag("PauseByOwner", ({ owner, replyTo }) =>
              Effect.gen(function* () {
                const children = (yield* actor.children()).filter((child) => {
                  const record = registry.get(`/signals/${child.path.split("/").at(-1)}`);
                  return (
                    record && Schema.decodeUnknownSync(SignalSnapshot)(record.state).owner === owner
                  );
                });
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
                  ? Schema.decodeUnknownEffect(SignalReactionInput)(command.input).pipe(
                      Effect.result,
                    )
                  : Schema.decodeUnknownEffect(SignalChangeInput)(command.input).pipe(
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
      });
    }),
  );
}
