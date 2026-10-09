import { Context, Deferred, Effect, Fiber, Layer, Schema } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Actor, ActorSystem, ReplyTo } from "../src/index.js";

class Resource extends Context.Service<Resource, { readonly id: number }>()(
  "test/SharedResource",
) {}
class Dependent extends Context.Service<Dependent, { readonly resource: Resource["Service"] }>()(
  "test/SharedDependent",
) {}
const Reader = Actor.define("test/SharedReader", {
  commands: [Schema.TaggedStruct("Read", { replyTo: ReplyTo<boolean>() })],
})(
  Effect.gen(function* () {
    const resource = yield* Resource;
    const dependent = yield* Dependent;
    return { receive: ({ replyTo }) => replyTo.tell(resource === dependent.resource) };
  }),
);

test("provided Layers share one memo map across acquisition steps", async () => {
  let acquired = 0;
  let released = 0;
  const resource = Layer.effect(
    Resource,
    Effect.acquireRelease(
      Effect.sync(() => ({ id: ++acquired })),
      () =>
        Effect.sync(() => {
          released++;
        }),
    ),
  );
  const dependent = Layer.effect(
    Dependent,
    Effect.gen(function* () {
      return { resource: yield* Resource };
    }),
  ).pipe(Layer.provide(resource));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(resource),
          ActorSystem.provide(dependent),
        );
        const actor = yield* system.spawn("reader", Reader);
        assert.equal(yield* actor.ask((replyTo) => ({ _tag: "Read", replyTo })), true);
        assert.equal(acquired, 1);
        assert.equal(released, 0);
      }),
    ),
  );
  assert.equal(released, 1);
});

const Idle = Actor.define("test/IdleBoundary", {
  commands: [Schema.TaggedStruct("Noop", {})],
})(Effect.succeed({ receive: () => Effect.void }));

test("inspection reads the live registry each time the same Effect runs", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        const inspect = system.inspect();
        assert.deepEqual(yield* inspect, []);
        const actor = yield* system.spawn("later", Idle);
        assert.deepEqual(
          (yield* inspect).map((value) => value.path),
          ["/user/later"],
        );
        yield* system.stop(actor);
        assert.deepEqual(yield* inspect, []);
      }),
    ),
  );
});

// The owner of terminate may be cancelled before a blocked handler can finish.
// Outer-scope cleanup must still make progress and release shared resources.
test("interrupting graceful termination still closes the system", { timeout: 3000 }, async () => {
  let released = false;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const Blocked = Actor.define("test/BlockedShutdown", {
          commands: [Schema.TaggedStruct("Block", {})],
        })(
          Effect.succeed({
            receive: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          }),
        );
        const resource = Layer.effect(
          Resource,
          Effect.acquireRelease(Effect.succeed({ id: 1 }), () =>
            Effect.sync(() => {
              released = true;
            }),
          ),
        );
        const system = yield* ActorSystem.make().pipe(ActorSystem.provide(resource));
        const actor = yield* system.spawn("blocked", Blocked);
        yield* actor.tell({ _tag: "Block" });
        yield* Deferred.await(entered);
        const termination = yield* system.terminate().pipe(Effect.forkScoped);
        while ((yield* system.inspect())[0]?.status !== "stopping") yield* Effect.yieldNow;
        yield* Fiber.interrupt(termination);
        assert.equal(released, true);
        assert.deepEqual(yield* system.inspect(), []);
        yield* system.terminate();
      }),
    ),
  );
});

test(
  "a failing service finalizer completes all termination waiters",
  { timeout: 3000 },
  async () => {
    const failure = new Error("resource close failed");
    const exit = await Effect.runPromise(
      Effect.exit(
        Effect.scoped(
          Effect.gen(function* () {
            const resource = Layer.effect(
              Resource,
              Effect.acquireRelease(Effect.succeed({ id: 1 }), () => Effect.die(failure)),
            );
            const system = yield* ActorSystem.make().pipe(ActorSystem.provide(resource));
            const first = yield* Effect.exit(system.terminate());
            const second = yield* Effect.exit(system.terminate());
            assert.equal(first._tag, "Failure");
            assert.equal(second._tag, "Failure");
          }),
        ),
      ),
    );
    assert.equal(exit._tag, "Failure");
  },
);

test("parent resources outlive descendant behavior cleanup", async () => {
  const closed: string[] = [];
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const childReady = yield* Deferred.make<void>();
        const Child = Actor.define("test/OrderChild", {
          commands: [Schema.TaggedStruct("Noop", {})],
        })(
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                closed.push("child");
              }),
            );
            return {
              started: () => Deferred.succeed(childReady, undefined).pipe(Effect.asVoid),
              receive: () => Effect.void,
            };
          }),
        );
        const Parent = Actor.define("test/OrderParent", {
          commands: [Schema.TaggedStruct("Noop", {})],
        })(
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                closed.push("parent");
              }),
            );
            return {
              started: (context) => context.spawn("child", Child).pipe(Effect.asVoid),
              receive: () => Effect.void,
            };
          }),
        );
        const system = yield* ActorSystem.make();
        const parent = yield* system.spawn("parent", Parent);
        yield* Deferred.await(childReady);
        yield* system.stop(parent);
        assert.deepEqual(closed, ["child", "parent"]);
      }),
    ),
  );
});

test("selection follows replacements but stale references cannot stop them", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        for (const name of ["a*b", "a?b", "a#b", "a:b"]) {
          const invalid = yield* Effect.result(system.spawn(name, Idle));
          assert.equal(invalid._tag, "Failure");
        }
        const selection = system.select("/user/worker");
        const first = yield* system.spawn("worker", Idle);
        assert.equal(yield* selection.resolve(), first);
        yield* system.stop(first);
        const replacement = yield* system.spawn("worker", Idle);
        yield* system.stop(first);
        assert.equal(yield* selection.resolve(), replacement);
      }),
    ),
  );
});
