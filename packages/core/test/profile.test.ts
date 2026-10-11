import { ActorSystem, Command } from "@aster/actor";
import { Effect, Layer } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ContextQueries } from "../src/queries/registry.js";
import { contextSpawnOptions } from "../src/context/actor.js";
import { DurableContext } from "../src/context/store.js";
import {
  defaultUserProfile,
  ReadUserProfile,
  UpdateUserProfile,
  UserProfileActor,
} from "../src/index.js";
import { makeTestContext } from "../src/testing/context.js";

test("user profile is a durable Context with a typed query and explicit updates", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeTestContext();
        const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(DurableContext, registry.backend),
            Layer.succeed(ContextQueries, queries),
          ),
        );
        const actor = yield* system.spawn(
          "profile",
          UserProfileActor,
          contextSpawnOptions("/profile"),
        );
        yield* actor.awaitStarted;

        assert.deepEqual(
          yield* queries.query({ path: "/profile", command: "profile", args: {} }),
          defaultUserProfile(),
        );
        assert.deepEqual(
          yield* actor.ask<Command.Reply<typeof ReadUserProfile>>(
            (replyTo) => new ReadUserProfile({ replyTo }),
          ),
          { _tag: "Success", value: defaultUserProfile() },
        );

        const update = yield* actor.ask<Command.Reply<typeof UpdateUserProfile>>(
          (replyTo) =>
            new UpdateUserProfile({
              update: {
                displayName: "Feng",
                timeZone: "America/Los_Angeles",
                responseStyle: "concise",
              },
              replyTo,
            }),
        );
        assert.deepEqual(update, {
          _tag: "Success",
          value: {
            displayName: "Feng",
            timeZone: "America/Los_Angeles",
            preferences: { responseStyle: "concise" },
          },
        });
        assert.equal(registry.get("/profile")?.revision, 2);
        assert.match(registry.get("/profile")?.text ?? "", /Feng/);
        assert.equal(registry.journal().length, 0);
        const queried = yield* queries.query({ path: "/profile", command: "profile", args: {} });
        assert.deepEqual(queried, update._tag === "Success" ? update.value : undefined);
      }),
    ),
  );
});
