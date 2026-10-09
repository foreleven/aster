import { Cause, Context, Data, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { serialize } from "node:v8";
import {
  Actor,
  ActorPersistence,
  ActorStartupError,
  ActorSystem,
  InMemoryActorPersistence,
  PersistentActor,
  ReplyTo,
} from "../src/index.js";

class StartupFailure extends Data.TaggedError("StartupFailure")<Record<never, never>> {}
class Hooks extends Context.Service<
  Hooks,
  {
    readonly acquire: Effect.Effect<void, StartupFailure>;
    readonly start: Effect.Effect<void, StartupFailure>;
  }
>()("test/StartupHooks") {}
const Worker = Actor.define("test/StartupWorker", {
  commands: [Schema.TaggedStruct("Read", { replyTo: ReplyTo<string>() })],
})(
  Effect.gen(function* () {
    const hooks = yield* Hooks;
    yield* hooks.acquire;
    return {
      started: () => hooks.start,
      receive: (command) => command.replyTo.tell("handled"),
    };
  }),
);
const run = <A, E>(effect: Effect.Effect<A, E, import("effect").Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(effect).pipe(Effect.timeout("5 seconds")));
const systemWith = (hooks: Hooks["Service"]) =>
  ActorSystem.make().pipe(ActorSystem.provide(Layer.succeed(Hooks, hooks)));

test("startup waits for Layer and started; mailbox queues and waiter cancellation is local", async () => {
  await run(
    Effect.gen(function* () {
      const acquiring = yield* Deferred.make<void>();
      const acquired = yield* Deferred.make<void>();
      const starting = yield* Deferred.make<void>();
      const started = yield* Deferred.make<void>();
      const system = yield* systemWith({
        acquire: Deferred.succeed(acquiring, undefined).pipe(
          Effect.andThen(Deferred.await(acquired)),
        ),
        start: Deferred.succeed(starting, undefined).pipe(Effect.andThen(Deferred.await(started))),
      });
      const ref = yield* system.spawn("worker", Worker);
      yield* Deferred.await(acquiring);
      const cancelled = yield* ref.awaitStarted.pipe(Effect.forkScoped);
      const waiting = yield* ref.awaitStarted.pipe(Effect.forkScoped);
      const reply = yield* ref
        .ask((replyTo) => ({ _tag: "Read", replyTo }))
        .pipe(Effect.forkScoped);
      yield* Fiber.interrupt(cancelled);
      assert.equal(waiting.pollUnsafe(), undefined);
      assert.equal(reply.pollUnsafe(), undefined);
      yield* Deferred.succeed(acquired, undefined);
      yield* Deferred.await(starting);
      assert.equal(waiting.pollUnsafe(), undefined);
      assert.equal(reply.pollUnsafe(), undefined);
      yield* Deferred.succeed(started, undefined);
      yield* Fiber.join(waiting);
      yield* ref.awaitStarted;
      assert.equal(yield* Fiber.join(reply), "handled");
    }),
  );
});

for (const phase of ["acquire", "start"] as const) {
  for (const defect of [false, true]) {
    test(`${phase} ${defect ? "defect" : "typed failure"} reaches every startup waiter`, async () => {
      await run(
        Effect.gen(function* () {
          const cause = new StartupFailure();
          const system = yield* systemWith({
            acquire: Effect.void,
            start: Effect.void,
            [phase]: defect ? Effect.die(cause) : Effect.fail(cause),
          });
          const ref = yield* system.spawn("worker", Worker, { supervision: () => "stop" });
          const exits = yield* Effect.all(
            [Effect.exit(ref.awaitStarted), Effect.exit(ref.awaitStarted)],
            {
              concurrency: "unbounded",
            },
          );
          for (const exit of exits) {
            assert.ok(Exit.isFailure(exit));
            const failure = Cause.squash(exit.cause);
            if (defect) assert.equal(failure, cause);
            else {
              assert.ok(failure instanceof ActorStartupError);
              assert.equal(failure.path, ref.path);
              assert.equal(failure.cause, cause);
            }
          }
          yield* system.stop(ref);
          assert.deepEqual(yield* Effect.exit(ref.awaitStarted), exits[0]);
        }),
      );
    });
  }
}

test("stopping during startup interrupts current and future waiters without mixing replacement refs", async () => {
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const system = yield* systemWith({
        acquire: Effect.void,
        start: Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
      });
      const ref = yield* system.spawn("worker", Worker);
      yield* Deferred.await(entered);
      const waiting = yield* ref.awaitStarted.pipe(Effect.forkScoped);
      const stopping = yield* system.stop(ref).pipe(Effect.forkScoped);
      const exit = yield* Fiber.await(waiting);
      assert.ok(Exit.hasInterrupts(exit));
      assert.ok(Exit.hasInterrupts(yield* Effect.exit(ref.awaitStarted)));
      // Graceful stop still drains the started hook; cancelling a waiter never cancels it.
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(stopping);
      const replacement = yield* system.spawn("worker", Worker);
      yield* replacement.awaitStarted;
      assert.notEqual(replacement.incarnation, ref.incarnation);
      assert.ok(Exit.hasInterrupts(yield* Effect.exit(ref.awaitStarted)));
    }),
  );
});

test("Scope shutdown interrupts a pending startup and future waiters", async () => {
  const ref = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const system = yield* systemWith({
          acquire: Effect.void,
          start: Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        });
        const ref = yield* system.spawn("worker", Worker);
        yield* Deferred.await(entered);
        return ref;
      }),
    ),
  );
  assert.ok(Exit.hasInterrupts(await Effect.runPromiseExit(ref.awaitStarted)));
});

const Counter = PersistentActor.define("test/StartupCounter", {
  commands: [Schema.TaggedStruct("Read", { replyTo: ReplyTo<number>() })],
  event: Schema.Number,
  state: Schema.Number,
})(
  Effect.succeed({
    initialState: 0,
    applyEvent: (state, event) => state + event,
    receive: (command, context) => command.replyTo.tell(context.state),
  }),
);

test("startup waits for persistent recovery before releasing callers", async () => {
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const store = yield* ActorPersistence.pipe(Effect.provide(InMemoryActorPersistence.layer));
      yield* store.append("/user/counter", 0, [serialize(7).toString("base64")]);
      const system = yield* ActorSystem.make().pipe(
        ActorSystem.provide(
          Layer.succeed(ActorPersistence, {
            ...store,
            load: (id) =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(store.load(id)),
              ),
          }),
        ),
      );
      const counter = yield* system.spawn("counter", Counter);
      yield* Deferred.await(entered);
      const waiting = yield* counter.awaitStarted.pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      assert.equal(waiting.pollUnsafe(), undefined);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(waiting);
      assert.equal(yield* counter.ask((replyTo) => ({ _tag: "Read", replyTo })), 7);
    }),
  );
});
