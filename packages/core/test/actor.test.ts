import { ActorSystem, ReplyTo, type ActorRef } from "@aster/actor";
import { Effect, Fiber, Layer, Match, Schema, Stream } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import {
  ContextActor,
  ContextRegistry,
  contextPath,
  contextSpawnOptions,
  defineContext,
  spawnContextChild,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

const Command = Schema.TaggedUnion({
  Set: { value: Schema.Number },
  Read: {
    replyTo: ReplyTo<{ path: string; value: number }>(),
  },
  Fail: {},
  Stop: {},
});
const Counter = ContextActor.define("test/ContextCounter", {
  commands: Object.values(Command.cases),
  context: defineContext({
    state: Schema.Struct({ value: Schema.Number }),
    message: Schema.String,
  }),
})(
  Effect.gen(function* () {
    const registry = yield* ContextRegistry;
    return {
      started: (actor) => {
        const path = contextPath(actor);
        return registry
          .commit(
            registry.get(path) ?? {
              path,
              description: "Counter",
              state: { value: 0 },
              messages: [],
            },
            { expectedRevision: registry.get(path)?.revision ?? 0 },
          )
          .pipe(Effect.asVoid);
      },
      receive: (command, actor) => {
        const path = contextPath(actor);
        const record = registry.get(path)!;
        return Match.value(command).pipe(
          Match.tag("Fail", (_command) => Effect.die(new Error("restart"))),
          Match.tag("Stop", (_command) => actor.stopSelf()),
          Match.tag("Read", (command) =>
            command.replyTo.tell({ path, value: (record.state as { value: number }).value }),
          ),
          Match.tag("Set", (command) =>
            registry
              .commit(
                {
                  ...record,
                  state: { value: command.value },
                  messages: [...record.messages, "updated"],
                },
                { expectedRevision: record.revision ?? 0 },
              )
              .pipe(Effect.asVoid, Effect.orDie),
          ),
          Match.exhaustive,
        );
      },
    };
  }),
);

const Parent = ContextActor.define("test/ContextParent", {
  commands: [
    Schema.TaggedStruct("Children", {
      replyTo: ReplyTo<readonly ActorRef<typeof Command.Type>[]>(),
    }),
  ],
  context: defineContext({
    state: Schema.Struct({}),
    message: Schema.Never,
  }),
})(
  Effect.succeed({
    receive: (command, actor) =>
      Match.value(command).pipe(
        Match.tag("Children", (command) =>
          Effect.gen(function* () {
            const virtual = yield* spawnContextChild(actor, "me/one", Counter).pipe(Effect.orDie);
            const direct = yield* actor.spawn("direct", Counter).pipe(Effect.orDie);
            yield* command.replyTo.tell([virtual, direct]);
          }),
        ),
        Match.exhaustive,
      ),
  }),
);

test("Context definition registers before started and commands, and survives restart and stop", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(ContextRegistry, registry)),
        );
        const actor = yield* system.spawn("counter", Counter);
        yield* actor.tell({ _tag: "Set", value: 7 });
        yield* actor.tell({ _tag: "Fail" });
        assert.deepEqual(yield* actor.ask((replyTo) => ({ _tag: "Read", replyTo })), {
          path: "/counter",
          value: 7,
        });
        const stopped = yield* Stream.runHead(
          Stream.filter(
            system.events,
            (event) => event._tag === "ActorStopped" && event.path === actor.path,
          ),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* actor.tell({ _tag: "Stop" });
        yield* Fiber.join(stopped).pipe(Effect.timeout("2 seconds"));
        assert.deepEqual(registry.get("/counter")?.messages, ["updated"]);
        const replacement = yield* system.spawn("counter", Counter);
        assert.deepEqual(yield* replacement.ask((replyTo) => ({ _tag: "Read", replyTo })), {
          path: "/counter",
          value: 7,
        });
      }),
    ),
  );
});

test("explicit public paths propagate to direct and virtual children independently of actor paths", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(ContextRegistry, registry)),
        );
        const parent = yield* system.spawn(
          "physical",
          Parent,
          contextSpawnOptions("/accounts/work"),
        );
        const children = yield* parent.ask<readonly ActorRef<typeof Command.Type>[]>((replyTo) => ({
          _tag: "Children",
          replyTo,
        }));
        const paths: string[] = [];
        for (const child of children)
          paths.push(
            (yield* child.ask<{ path: string; value: number }>((replyTo) => ({
              _tag: "Read",
              replyTo,
            }))).path,
          );
        assert.deepEqual(paths, ["/accounts/work/me/one", "/accounts/work/direct"]);
        yield* registry.commit(
          { path: "/accounts/work", description: "Parent", state: {}, messages: [] },
          { expectedRevision: 0 },
        );
        assert.equal(registry.get("/physical"), undefined);
      }),
    ),
  );
});

const dependencyChecks = Effect.gen(function* () {
  const system = yield* ActorSystem.make();
  // @ts-expect-error ContextActor adds ContextRegistry to its required services automatically.
  yield* system.spawn("missing-registry", Counter);
});
void dependencyChecks;
