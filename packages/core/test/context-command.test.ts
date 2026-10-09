import { ActorSystem, ActorTestKit, Command } from "@aster/actor";
import { Clock, Deferred, Effect, Fiber, Layer, Queue, Schema } from "effect";
import { TestClock } from "effect/testing";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ContextActor, contextPath, contextSpawnOptions } from "../src/context/actor.js";
import { defineContext } from "../src/context/definition.js";
import { ContextCommand } from "../src/context/queries/protocol.js";
import { ContextQueries } from "../src/context/queries/routes.js";
import { ContextRegistry } from "../src/context/registry.js";
import { makeContextRegistry } from "../src/testing/context.js";

class Search extends ContextCommand.Class<Search>()("search", {
  payload: { query: Schema.NonEmptyString, limit: Schema.optional(Schema.Number) },
  description: "Search retained messages.",
}) {}
class Local extends Command.Class<Local>()("Local", { payload: {} }) {}
const Internal = Schema.TaggedStruct("Poll", {});
const context = defineContext({ state: Schema.Struct({}), message: Schema.Never });

test("Context commands derive discovery and typed requests from one class list and unregister on stop", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
        const Owner = ContextActor.define("test/context-command/Owner", {
          commands: [Search, Local],
          internal: Internal,
          context,
        })(
          Effect.succeed({
            query: (command, actor) =>
              Effect.succeed({
                command: command._tag,
                queriedAt: "2026-10-09T00:00:00Z",
                path: contextPath(actor),
                data: command.query,
              }),
            receive: () => Effect.void,
          }),
        );
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ContextQueries, queries),
          ),
        );
        const ref = yield* system.spawn("owner", Owner, contextSpawnOptions("/mail/test"));
        yield* ref.awaitStarted;
        const description = yield* queries.describe("/mail/test");
        assert.deepEqual(
          description.commands.map((entry) => entry.command),
          ["search"],
        );
        assert.doesNotMatch(
          JSON.stringify(description.commands[0].arguments),
          /replyTo|_tag|input/,
        );
        const remote = yield* queries.query({
          path: "/mail/test",
          command: "search",
          args: { query: "remote" },
        });
        assert.equal(remote.data, "remote");
        const local = yield* ref.ask<Command.Reply<typeof Search>>(
          (replyTo) => new Search({ query: "local", limit: undefined, replyTo }),
        );
        assert.equal(local._tag, "Success");
        if (local._tag === "Success") assert.equal(local.value.data, "local");
        const invalidArgs: readonly Record<string, string | boolean>[] = [
          { query: "" },
          { query: "valid", unexpected: true },
        ];
        for (const args of invalidArgs) {
          assert.equal(
            (yield* Effect.flip(queries.query({ path: "/mail/test", command: "search", args })))
              .kind,
            "invalid-input",
          );
        }
        yield* system.stop(ref);
        assert.equal((yield* queries.list()).total, 0);
        assert.equal((yield* Effect.flip(queries.describe("/mail/test"))).kind, "unavailable");
        const replacement = yield* system.spawn("owner", Owner, contextSpawnOptions("/mail/test"));
        yield* replacement.awaitStarted;
        assert.equal((yield* queries.list()).total, 1);
      }),
    ),
  );
});

test("Context query cancellation interrupts owned work and owner shutdown fails pending requests", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
        const entered = yield* Queue.unbounded<void>();
        const interrupted = yield* Deferred.make<void>();
        const Owner = ContextActor.define("test/context-command/Slow", {
          commands: [Search],
          context,
        })(
          Effect.succeed({
            query: () =>
              Queue.offer(entered, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
              ),
            receive: () => Effect.void,
          }),
        );
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ContextQueries, queries),
          ),
        );
        const ref = yield* system.spawn("slow", Owner, contextSpawnOptions("/slow"));
        yield* ref.awaitStarted;
        const work = yield* queries
          .query({ path: "/slow", command: "search", args: { query: "wait" } })
          .pipe(Effect.forkScoped);
        yield* Queue.take(entered);
        yield* Fiber.interrupt(work);
        yield* Deferred.await(interrupted);
        const pending = yield* queries
          .query({ path: "/slow", command: "search", args: { query: "stop" } })
          .pipe(Effect.forkScoped);
        yield* Queue.take(entered);
        yield* system.stop(ref);
        assert.equal((yield* Effect.flip(Fiber.join(pending))).kind, "unavailable");
      }),
    ),
  );
});

test("query defects enter supervision and restart rebuilds the owner's catalogue", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* Effect.gen(function* () {
          const registry = yield* makeContextRegistry();
          const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
          let generation = 0;
          const Owner = ContextActor.define("test/context-command/Restart", {
            commands: [Search],
            context,
          })(
            Effect.sync(() => {
              const current = ++generation;
              return {
                receive: () => Effect.void,
                query: (command, actor) =>
                  command.query === "defect"
                    ? Effect.die(new Error("query defect"))
                    : Effect.succeed({
                        path: contextPath(actor),
                        command: command._tag,
                        queriedAt: "2026-10-09T00:00:00Z",
                        data: current,
                      }),
              };
            }),
          );
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(ContextQueries, queries),
            ),
          );
          const ref = yield* system.spawn("restart", Owner, contextSpawnOptions("/restart"));
          yield* ref.awaitStarted;
          const failure = yield* Effect.flip(
            queries.query({ path: "/restart", command: "search", args: { query: "defect" } }),
          );
          assert.equal(failure.kind, "unavailable");
          yield* clock.adjust(1_000);
          // A direct typed request queues behind reinitialization and provides a mailbox fence.
          const reply = yield* ref.ask<Command.Reply<typeof Search>>(
            (replyTo) => new Search({ query: "after", replyTo }),
          );
          assert.equal(reply._tag, "Success");
          if (reply._tag === "Success") assert.equal(reply.value.data, 2);
          assert.equal((yield* queries.describe("/restart")).commands.length, 1);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});

test("concurrent commands may share one explicit reply Actor without losing responses", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
        const entered = yield* Queue.unbounded<void>();
        const release = yield* Deferred.make<void>();
        const Owner = ContextActor.define("test/context-command/SharedReply", {
          commands: [Search],
          context,
        })(
          Effect.succeed({
            receive: () => Effect.void,
            query: (command, actor) =>
              Effect.gen(function* () {
                yield* Queue.offer(entered, undefined);
                yield* Deferred.await(release);
                return {
                  path: contextPath(actor),
                  command: command._tag,
                  queriedAt: "2026-10-09T00:00:00Z",
                  data: command.query,
                };
              }),
          }),
        );
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ContextQueries, queries),
          ),
        );
        const ref = yield* system.spawn("shared", Owner);
        yield* ref.awaitStarted;
        const replies = yield* ActorTestKit.probe<Command.Reply<typeof Search>>();
        yield* ref.tell(new Search({ query: "first", replyTo: replies.ref }));
        yield* ref.tell(new Search({ query: "second", replyTo: replies.ref }));
        yield* Queue.take(entered);
        yield* Queue.take(entered);
        yield* Deferred.succeed(release, undefined);
        const responses = [yield* replies.take(), yield* replies.take()];
        assert.deepEqual(
          responses
            .map((reply) => (reply._tag === "Success" ? reply.value.data : "failure"))
            .sort(),
          ["first", "second"],
        );
      }),
    ),
  );
});
