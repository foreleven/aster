import { DurableContext } from "@aster/core";
import { ActorSystem, ActorTestKit, Command, CommandProcessor } from "@aster/actor";
import { Clock, Deferred, Effect, Fiber, Layer, Match, Queue, Schema } from "effect";
import { TestClock } from "effect/testing";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ContextActor, contextPath, contextSpawnOptions } from "../src/context/actor.js";
import { ContextQueryError } from "../src/context/contracts.js";
import { ContextCommand } from "../src/context/queries/protocol.js";
import { ContextQueries } from "../src/context/queries/routes.js";
import { ContextRegistry } from "../src/context/registry.js";
import { makeContextRegistry } from "../src/testing/context.js";

const SearchResult = Schema.Struct({
  path: Schema.String,
  command: Schema.String,
  queriedAt: Schema.String,
  data: Schema.Unknown,
});
class Search extends ContextCommand.Class<Search>()("search", {
  payload: { query: Schema.NonEmptyString, limit: Schema.optional(Schema.Number) },
  description: "Search retained messages.",
  success: SearchResult,
  error: ContextQueryError,
}) {}
class Local extends Command.Class<Local>()("Local", { payload: {} }) {}
const Internal = Schema.TaggedStruct("Poll", {});

test("Context commands own response codecs, text presentation and void replies", async () => {
  class Count extends ContextCommand.Class<Count>()("count", {
    description: "Count retained items.",
    payload: { fail: Schema.optional(Schema.Boolean) },
    success: Schema.NumberFromString,
    error: Schema.Void,
  }) {
    static override text = (count: number): string => `${count} retained items`;
  }
  class Empty extends ContextCommand.Class<Empty>()("empty", {
    description: "Read an empty result.",
    payload: { wait: Schema.optional(Schema.Boolean) },
    success: Schema.Void,
  }) {
    static override text = (): string => "No retained items";
  }
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
        const entered = yield* Deferred.make<void>();
        const Owner = ContextActor.define("test/context-command/OwnResponse", {
          commands: [Count, Empty],
        })(
          Effect.gen(function* () {
            const processor = yield* CommandProcessor.make({ concurrency: 1 });
            return {
              receive: (command, actor) =>
                Match.value(command).pipe(
                  Match.tag("count", ({ fail, replyTo }) =>
                    Command.reply(replyTo, fail ? Effect.fail(undefined) : Effect.succeed(42)),
                  ),
                  Match.tag("empty", (request) =>
                    processor.submit(
                      request,
                      actor,
                      request.wait
                        ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
                        : Effect.void,
                    ),
                  ),
                  Match.exhaustive,
                ),
            };
          }),
        );
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ContextQueries, queries),
          ),
        );
        const owner = yield* system.spawn(
          "own-response",
          Owner,
          contextSpawnOptions("/own-response"),
        );
        yield* owner.awaitStarted;
        const input = { path: "/own-response", command: "count", args: {} };
        const direct = yield* owner.ask<Command.Reply<typeof Count>>(
          (replyTo) => new Count({ replyTo }),
        );
        assert.deepEqual(direct, { _tag: "Success", value: 42 });
        assert.equal(yield* queries.query(input), 42);
        assert.equal(yield* queries.text(input), "42 retained items");
        assert.equal(yield* queries.json(input), "42");
        assert.equal(
          yield* queries.query({ ...input, args: { fail: true } }).pipe(Effect.flip),
          undefined,
        );
        assert.equal(
          yield* queries.json({ ...input, args: { fail: true } }).pipe(Effect.flip),
          null,
        );
        const empty = { ...input, command: "empty" };
        assert.deepEqual(
          yield* owner.ask<Command.Reply<typeof Empty>>((replyTo) => new Empty({ replyTo })),
          {
            _tag: "Success",
            value: undefined,
          },
        );
        assert.equal(yield* queries.query(empty), undefined);
        assert.equal(yield* queries.text(empty), "No retained items");
        assert.equal(yield* queries.json(empty), null);
        const waiting = yield* queries
          .query({ ...empty, args: { wait: true } })
          .pipe(Effect.flip, Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* system.stop(owner);
        assert.equal(
          Schema.decodeUnknownSync(ContextQueryError)(yield* Fiber.join(waiting)).kind,
          "unavailable",
        );
      }),
    ),
  );
});

test("Context commands derive discovery and typed requests from one class list and unregister on stop", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
        const Owner = ContextActor.define("test/context-command/Owner", {
          commands: [Search, Local],
          internal: Internal,
        })(
          Effect.succeed({
            receive: (command, actor) =>
              command._tag === "search"
                ? Command.reply(
                    command.replyTo,
                    Effect.succeed({
                      command: command._tag,
                      queriedAt: "2026-10-09T00:00:00Z",
                      path: contextPath(actor),
                      data: command.query,
                    }),
                  )
                : Effect.void,
          }),
        );
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.merge(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(DurableContext, registry.backend),
            ),
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
        assert.deepEqual(remote, {
          path: "/mail/test",
          command: "search",
          queriedAt: "2026-10-09T00:00:00Z",
          data: "remote",
        });
        assert.equal(Schema.decodeUnknownSync(SearchResult)(remote).data, "remote");
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
            Schema.decodeUnknownSync(ContextQueryError)(
              yield* Effect.flip(queries.query({ path: "/mail/test", command: "search", args })),
            ).kind,
            "invalid-input",
          );
        }
        yield* system.stop(ref);
        assert.equal((yield* queries.list()).total, 0);
        assert.equal(
          Schema.decodeUnknownSync(ContextQueryError)(
            yield* Effect.flip(queries.describe("/mail/test")),
          ).kind,
          "unavailable",
        );
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
        })(
          Effect.gen(function* () {
            const processor = yield* CommandProcessor.make({ concurrency: 1 });
            return {
              receive: (command, actor) =>
                processor.submit(
                  command,
                  actor,
                  Queue.offer(entered, undefined).pipe(
                    Effect.andThen(Effect.never),
                    Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
                  ),
                ),
            };
          }),
        );
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.merge(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(DurableContext, registry.backend),
            ),
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
        assert.equal(
          Schema.decodeUnknownSync(ContextQueryError)(yield* Effect.flip(Fiber.join(pending))).kind,
          "unavailable",
        );
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
          })(
            Effect.gen(function* () {
              const current = ++generation;
              const processor = yield* CommandProcessor.make({ concurrency: 1 });
              return {
                receive: (command, actor) =>
                  processor.submit(
                    command,
                    actor,
                    command.query === "defect"
                      ? Effect.die(new Error("query defect"))
                      : Effect.succeed({
                          path: contextPath(actor),
                          command: command._tag,
                          queriedAt: "2026-10-09T00:00:00Z",
                          data: current,
                        }),
                  ),
              };
            }),
          );
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.merge(
                Layer.succeed(ContextRegistry, registry),
                Layer.succeed(DurableContext, registry.backend),
              ),
              Layer.succeed(ContextQueries, queries),
            ),
          );
          const ref = yield* system.spawn("restart", Owner, contextSpawnOptions("/restart"));
          yield* ref.awaitStarted;
          const failure = yield* Effect.flip(
            queries.query({ path: "/restart", command: "search", args: { query: "defect" } }),
          );
          assert.equal(Schema.decodeUnknownSync(ContextQueryError)(failure).kind, "unavailable");
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
        })(
          Effect.gen(function* () {
            const processor = yield* CommandProcessor.make({ concurrency: 2 });
            return {
              receive: (command, actor) =>
                processor.submit(
                  command,
                  actor,
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
                ),
            };
          }),
        );
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.merge(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(DurableContext, registry.backend),
            ),
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

test("owner stop replies to active and queued commands sent with a shared reply Actor", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        class Fence extends Command.Class<Fence>()("Fence", { payload: {}, reply: Schema.Void }) {}
        const registry = yield* makeContextRegistry();
        const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
        const entered = yield* Deferred.make<void>();
        const Owner = ContextActor.define("test/context-command/QueuedStop", {
          commands: [Search, Fence],
        })(
          Effect.gen(function* () {
            const processor = yield* CommandProcessor.make({ concurrency: 1 });
            return {
              receive: (command, actor) =>
                command._tag === "Fence"
                  ? command.replyTo.tell(undefined)
                  : processor.submit(
                      command,
                      actor,
                      Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
                    ),
            };
          }),
        );
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.merge(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(DurableContext, registry.backend),
            ),
            Layer.succeed(ContextQueries, queries),
          ),
        );
        const ref = yield* system.spawn("queued-stop", Owner);
        yield* ref.awaitStarted;
        const replies = yield* ActorTestKit.probe<Command.Reply<typeof Search>>();
        yield* ref.tell(new Search({ query: "active", replyTo: replies.ref }));
        yield* Deferred.await(entered);
        yield* ref.tell(new Search({ query: "queued", replyTo: replies.ref }));
        yield* ref.ask<void>((replyTo) => new Fence({ replyTo }));
        yield* system.stop(ref);
        for (const reply of [yield* replies.take(), yield* replies.take()]) {
          assert.equal(reply._tag, "Failure");
          if (reply._tag === "Failure") assert.equal(reply.error.kind, "unavailable");
        }
      }),
    ),
  );
});
