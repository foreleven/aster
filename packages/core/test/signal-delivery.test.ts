import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, type ActorRef } from "@aster/actor";
import { Deferred, Effect, Layer } from "effect";
import {
  ContextRegistry,
  ExternalAgents,
  SignalDefinitions,
  SignalRootActor,
  makeContextRegistry,
} from "../src/index.js";
import { fakeAgent, preparationLayer } from "./fixtures.js";

test("Signal configuration can progress while a Run has not acknowledged durable initialization", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const definition = {
          slug: "watch",
          when: "changed",
          task: "read",
          agent: "test",
          mode: "auto" as const,
        };
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, {
              ...registry,
              set: (record, options) =>
                record.path.includes("/runs/") &&
                (record.state as { status?: string }).status === "preparing"
                  ? Deferred.succeed(entered, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                      Effect.andThen(registry.set(record, options)),
                    )
                  : registry.set(record, options),
            }),
            Layer.succeed(SignalDefinitions, [definition]),
            preparationLayer,
            Layer.succeed(ExternalAgents, { test: fakeAgent() }),
          ),
        );
        const root = yield* system.spawn("signals", SignalRootActor);
        yield* root.tell({
          _tag: "Trigger",
          slug: "watch",
          sourceContext: {
            path: "/source",
            description: "source",
            state: {},
            messages: [],
          },
        });
        yield* Deferred.await(entered);
        const signal = yield* system.select("/user/signals/watch").resolve();
        const reply = yield* signal.ask<ActorRef<unknown>>(
          (replyTo) => ({
            _tag: "Configure",
            definition,
            active: false,
            replyTo,
          }),
          "200 millis",
        );
        assert.equal(reply, signal);
        const state = registry.get("/signals/watch")!.state as {
          active: boolean;
          occurrences: { delivered: boolean }[];
        };
        assert.equal(state.active, false);
        const occurrences = state.occurrences;
        assert.equal(occurrences.length, 1);
        assert.equal(occurrences[0]!.delivered, false);
        yield* Deferred.succeed(release, undefined);
      }),
    ),
  );
});
