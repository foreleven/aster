import { Deferred, Effect, Schema } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Actor, ActorSystem } from "../src/index.js";

test("runtime inspection observes current command and queued work without exposing payloads", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const Worker = Actor.define("test/Inspection", {
          commands: [Schema.TaggedStruct("Work", { secret: Schema.String })],
        })(
          Effect.succeed({
            receive: () =>
              Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
          }),
        );
        const system = yield* ActorSystem.make();
        const worker = yield* system.spawn("worker", Worker, {
          metadata: { contextPath: "/work", secret: "not-public" },
        });
        yield* worker.tell({ _tag: "Work", secret: "credential" });
        yield* Deferred.await(entered);
        yield* worker.tell({ _tag: "Work", secret: "credential" });
        const snapshot = yield* system.inspect();
        assert.equal(snapshot[0]?.processing, true);
        assert.equal(snapshot[0]?.mailboxSize, 1);
        assert.equal(snapshot[0]?.currentCommand, "Work");
        assert.deepEqual(snapshot[0]?.metadata, {});
        const projected = yield* system.inspect({ metadata: ["contextPath"] });
        assert.deepEqual(projected[0]?.metadata, { contextPath: "/work" });
        assert.ok(!JSON.stringify(projected).includes("not-public"));
        assert.ok(!JSON.stringify(snapshot).includes("credential"));
        assert.ok(!JSON.stringify(snapshot).includes("not-public"));
        yield* Deferred.succeed(release, undefined);
        yield* system.terminate();
        assert.deepEqual(yield* system.inspect(), []);
      }),
    ),
  );
});
