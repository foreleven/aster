import assert from "node:assert/strict";
import { test } from "node:test";
import {
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Match,
  Queue,
  Schema,
  Scope,
} from "effect";
import { TestClock } from "effect/testing";
import { Actor, ActorSystem, Command, CommandProcessor } from "../src/index.js";

class WorkError extends Schema.TaggedError<WorkError>()("WorkError", { id: Schema.String }) {}
class Work extends Command.Class<Work>()("Work", {
  payload: { id: Schema.String },
  success: Schema.String,
  error: WorkError,
}) {}
class Fence extends Command.Class<Fence>()("Fence", { payload: {}, reply: Schema.Void }) {}
class Prefix extends Context.Service<Prefix, string>()("test/processor/Prefix") {}

test("ask preserves Duration inputs while omitted timeouts retain the default", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* Effect.gen(function* () {
          const entered = yield* Queue.unbounded<void>();
          const Silent = Actor.define("test/processor/AskTimeout", { commands: [Fence] })(
            Effect.succeed({ receive: () => Queue.offer(entered, undefined).pipe(Effect.asVoid) }),
          );
          const system = yield* ActorSystem.make();
          const actor = yield* system.spawn("silent", Silent);
          const scope = yield* Effect.scope;
          class InheritedDuration {
            get milliseconds() {
              return 5;
            }
          }
          const cases: ReadonlyArray<readonly [Parameters<typeof actor.ask>[1], number]> = [
            [{}, 0],
            [new InheritedDuration(), 5],
            [{ timeout: {} }, 0],
            [undefined, 30_000],
            [{ scope }, 30_000],
            [{ timeout: undefined }, 30_000],
          ];
          for (const [options, timeout] of cases) {
            const request = yield* actor
              .ask<void>((replyTo) => new Fence({ replyTo }), options)
              .pipe(Effect.flip, Effect.forkScoped);
            yield* Queue.take(entered);
            if (timeout > 0) {
              yield* clock.adjust(timeout - 1);
              assert.equal(request.pollUnsafe(), undefined);
            }
            yield* clock.adjust(1);
            assert.equal(request.pollUnsafe()?._tag, "Success");
            assert.equal((yield* Fiber.join(request))._tag, "AskTimeoutError");
            assert.notEqual(scope.state._tag, "Closed");
          }
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});

test("queued commands preserve FIFO, skip cancelled requests and keep the mailbox responsive", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Queue.unbounded<string>();
        const released = yield* Queue.unbounded<string>();
        const release = yield* Deferred.make<void>();
        const scopes = new Map<string, Scope.Scope>();
        const Owner = Actor.define("test/processor/Fifo", { commands: [Work, Fence] })(
          Effect.gen(function* () {
            const processor = yield* CommandProcessor.make({ concurrency: 1 });
            return {
              receive: (command, actor) =>
                Match.value(command).pipe(
                  Match.tag("Fence", ({ replyTo }) => replyTo.tell(undefined)),
                  Match.tag("Work", (request) =>
                    processor.submit(
                      request,
                      actor,
                      Effect.gen(function* () {
                        scopes.set(request.id, request.replyTo.scope!);
                        yield* Effect.acquireRelease(Queue.offer(entered, request.id), () =>
                          Queue.offer(released, request.id),
                        );
                        if (request.id === "first") yield* Deferred.await(release);
                        if (request.id === "failure")
                          return yield* new WorkError({ id: request.id });
                        const prefix = yield* Prefix;
                        return prefix + request.id;
                      }),
                    ),
                  ),
                  Match.exhaustive,
                ),
            };
          }),
        );
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(Prefix, "ok:")),
        );
        const ref = yield* system.spawn("fifo", Owner);
        const parent = yield* Effect.scope;
        const ask = (id: string) =>
          ref.ask<Command.Reply<typeof Work>>((replyTo) => new Work({ id, replyTo }));
        const first = yield* ask("first").pipe(Effect.forkScoped);
        assert.equal(yield* Queue.take(entered), "first");
        const failure = yield* ask("failure").pipe(Effect.forkScoped);
        const cancelled = yield* ask("cancelled").pipe(Effect.forkScoped);
        const last = yield* ask("last").pipe(Effect.forkScoped);
        // This fence proves all three requests were enqueued while the worker was blocked.
        yield* ref.ask<void>((replyTo) => new Fence({ replyTo }));
        assert.equal(yield* Queue.size(entered), 0);
        yield* Fiber.interrupt(cancelled);
        yield* Deferred.succeed(release, undefined);
        assert.deepEqual(yield* Fiber.join(first), { _tag: "Success", value: "ok:first" });
        const rejected = yield* Fiber.join(failure);
        assert.equal(rejected._tag, "Failure");
        if (rejected._tag === "Failure") assert.equal(rejected.error.id, "failure");
        assert.deepEqual(yield* Fiber.join(last), { _tag: "Success", value: "ok:last" });
        assert.deepEqual(
          [yield* Queue.take(entered), yield* Queue.take(entered)],
          ["failure", "last"],
        );
        assert.deepEqual(
          [yield* Queue.take(released), yield* Queue.take(released), yield* Queue.take(released)],
          ["first", "failure", "last"],
        );
        assert.equal(scopes.has("cancelled"), false);
        assert.equal(scopes.get("first")!.state._tag, "Closed");
        assert.notEqual(parent.state._tag, "Closed");
        // Requests remain supported when the caller has no ambient Scope.
        assert.deepEqual(yield* ask("unscoped").pipe(Effect.provideContext(Context.empty())), {
          _tag: "Success",
          value: "ok:unscoped",
        });
        assert.equal(scopes.get("unscoped")!.state._tag, "Closed");
      }),
    ),
  );
});

test("timeouts and explicit parent Scope closure release workers without cancelling sibling requests", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* Effect.gen(function* () {
          const parent = yield* Scope.make();
          const entered = yield* Queue.unbounded<string>();
          const released = yield* Queue.unbounded<string>();
          const Owner = Actor.define("test/processor/Scopes", { commands: [Work, Fence] })(
            Effect.gen(function* () {
              const processor = yield* CommandProcessor.make({ concurrency: 2 });
              return {
                receive: (command, actor) =>
                  Match.value(command).pipe(
                    Match.tag("Fence", ({ replyTo }) => replyTo.tell(undefined)),
                    Match.tag("Work", (request) =>
                      processor.submit(
                        request,
                        actor,
                        Queue.offer(entered, request.id).pipe(
                          Effect.andThen(Effect.never),
                          Effect.ensuring(Queue.offer(released, request.id)),
                        ),
                      ),
                    ),
                    Match.exhaustive,
                  ),
              };
            }),
          );
          const system = yield* ActorSystem.make();
          const ref = yield* system.spawn("scopes", Owner);
          const ask = (id: string, timeout: number) =>
            ref.ask<Command.Reply<typeof Work>>((replyTo) => new Work({ id, replyTo }), {
              scope: parent,
              timeout,
            });
          const first = yield* ask("first", 1000).pipe(Effect.forkScoped);
          const second = yield* ask("second", 60_000).pipe(Effect.forkScoped);
          assert.deepEqual(
            [yield* Queue.take(entered), yield* Queue.take(entered)],
            ["first", "second"],
          );
          const third = yield* ask("third", 60_000).pipe(Effect.forkScoped);
          yield* ref.ask<void>((replyTo) => new Fence({ replyTo }));
          assert.equal(yield* Queue.size(entered), 0);
          yield* clock.adjust(1000);
          assert.equal((yield* Effect.flip(Fiber.join(first)))._tag, "AskTimeoutError");
          assert.equal(yield* Queue.take(released), "first");
          assert.equal(yield* Queue.take(entered), "third");
          assert.notEqual(parent.state._tag, "Closed");
          assert.equal(second.pollUnsafe(), undefined);
          yield* Scope.close(parent, Exit.void);
          assert.ok(Exit.isFailure(yield* Fiber.await(second)));
          assert.ok(Exit.isFailure(yield* Fiber.await(third)));
          assert.deepEqual([yield* Queue.take(released), yield* Queue.take(released)].sort(), [
            "second",
            "third",
          ]);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});

test("closed ask Scopes prevent admission and command factory defects close only the request Scope", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const Owner = Actor.define("test/processor/Admission", { commands: [Fence] })(
          Effect.succeed({ receive: ({ replyTo }) => replyTo.tell(undefined) }),
        );
        const system = yield* ActorSystem.make();
        const ref = yield* system.spawn("admission", Owner);
        const closed = yield* Scope.make();
        yield* Scope.close(closed, Exit.void);
        let built = false;
        const cancelled = yield* Effect.exit(
          ref.ask<void>(
            (replyTo) => {
              built = true;
              return new Fence({ replyTo });
            },
            { scope: closed },
          ),
        );
        assert.ok(Exit.isFailure(cancelled));
        assert.equal(built, false);
        const parent = yield* Effect.scope;
        let requestScope: Scope.Scope | undefined;
        const defective = yield* Effect.exit(
          ref.ask<void>(
            (replyTo) => {
              requestScope = replyTo.scope;
              throw new Error("invalid command factory");
            },
            { scope: parent },
          ),
        );
        assert.ok(Exit.isFailure(defective));
        assert.equal(requestScope!.state._tag, "Closed");
        assert.notEqual(parent.state._tag, "Closed");
        yield* ref.ask<void>((replyTo) => new Fence({ replyTo }));
      }),
    ),
  );
});
