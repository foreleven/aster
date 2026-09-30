import type { ActorRef } from "@aster/actor";
import { Context, Deferred, Effect, Layer } from "effect";
import type { SignalRootCommand } from "./actors.js";

/** Domain command port. It has no invented Actor identity or lifecycle. */
export class SignalCommands extends Context.Service<
  SignalCommands,
  {
    readonly ask: ActorRef<SignalRootCommand>["ask"];
    readonly bind: (root: ActorRef<SignalRootCommand>) => Effect.Effect<boolean>;
  }
>()("signals/Commands") {
  static readonly layer = Layer.effect(
    SignalCommands,
    Effect.gen(function* () {
      const ready = yield* Deferred.make<ActorRef<SignalRootCommand>>();
      return {
        ask: (command, timeout) =>
          Deferred.await(ready).pipe(Effect.flatMap((root) => root.ask(command, timeout))),
        bind: (root) => Deferred.succeed(ready, root),
      };
    }),
  );
}
