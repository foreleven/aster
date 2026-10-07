import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { Clock, Deferred, Effect, Fiber, Layer, Logger, Schema } from "effect";
import { TestClock } from "effect/testing";
import {
  ContextRegistry,
  ExternalAgents,
  GoalsRootActor,
  GoalSnapshot,
  type GoalCommandReply,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { testConversations } from "./conversation-fixtures.js";
import { goalWorkflowLayer } from "./workflow-fixtures.js";

const TerminationLog = Schema.Struct({
  event: Schema.Literal("goal.actor.terminated"),
  actorPath: Schema.String,
  cause: Schema.Struct({ message: Schema.String }),
});

for (const fails of [false, true]) {
  test(`Goal ${fails ? "startup failure is supervised and watched" : "slow startup queues its own inputs"} without blocking siblings`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const slowRead = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const noticed = yield* Deferred.make<typeof TerminationLog.Type>();
          const goalActivation = yield* Deferred.make<void>();
          const clock = yield* TestClock.make();
          const history = testConversations();
          const registry = yield* makeContextRegistry();
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(Clock.Clock, clock),
              Logger.layer([
                Logger.make<unknown, void>(({ message }) => {
                  if (!Array.isArray(message)) return;
                  for (const entry of message)
                    if (Schema.is(TerminationLog)(entry))
                      Deferred.doneUnsafe(noticed, Effect.succeed(entry));
                }),
              ]),
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(ExternalAgents, {}),
              goalWorkflowLayer({
                definitions: ["slow", "fast"].map((slug) => ({ slug, description: slug })),
                history: {
                  ...history,
                  read: (path) =>
                    Effect.gen(function* () {
                      if (path === "/goals/slow") {
                        yield* Deferred.succeed(slowRead, undefined);
                        yield* Deferred.await(release);
                        if (fails) return yield* Effect.die(new Error("Goal recovery failed"));
                      }
                      return yield* history.read(path);
                    }),
                },
                reasoner: { plan: () => Effect.die("Execution gate must remain closed") },
              }),
            ),
          );
          const root = yield* system.spawn("goals", GoalsRootActor, {
            metadata: { goalActivation },
          });
          yield* Deferred.await(slowRead);
          yield* root.awaitStarted;
          const send = (slug: string, requestId: string) =>
            root.ask<GoalCommandReply>((replyTo) => ({
              _tag: "Route",
              slug,
              command: {
                _tag: "SubmitInput",
                requestId,
                input: { _tag: "UserInput", text: "Hello" },
                replyTo,
              },
            }));
          const slow = yield* send("slow", "queued").pipe(Effect.forkScoped);
          assert.equal((yield* send("fast", "first"))._tag, "Accepted");
          assert.equal(slow.pollUnsafe(), undefined);
          assert.equal(yield* Deferred.isDone(noticed), false);
          yield* Deferred.succeed(release, undefined);
          if (fails) {
            yield* clock.adjust("10 seconds");
            const log = yield* Deferred.await(noticed);
            assert.equal(log.actorPath, "/user/goals/slow");
            assert.match(log.cause.message, /Goal recovery failed/);
            yield* Fiber.interrupt(slow);
            assert.equal((yield* send("slow", "after-stop"))._tag, "Rejected");
          } else {
            assert.equal((yield* Fiber.join(slow))._tag, "Accepted");
          }
          assert.equal((yield* send("fast", "second"))._tag, "Accepted");
          const state = Schema.decodeUnknownSync(GoalSnapshot)(registry.get("/goals/fast")!.state);
          assert.ok(state.inputs.every((input) => input.status === "pending"));
          assert.equal(state.receipts.length, 2);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
}
