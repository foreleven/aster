import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { AgentConversations } from "@aster/agent";
import { Clock, Deferred, Effect, Fiber, Layer, Logger, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import {
  ContextRegistry,
  SignalDefinitions,
  SignalRootActor,
  type SignalCommandReply,
  type StoredContext,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { testConversations } from "./conversation-fixtures.js";
import { taskFixture } from "./task-fixtures.js";
import { readSignalHistory } from "../src/signals/state/store.js";
import type { SignalChangeInput } from "../src/signals/protocol.js";

const definition = {
  slug: "slow",
  trigger: { _tag: "Context" as const, when: "Evidence changes" },
  task: { _tag: "Goal" as const, target: "/goals/personal", text: "Review" },
};
const TerminationLog = Schema.Struct({
  event: Schema.Literal("signal.actor.terminated"),
  actorPath: Schema.String,
  cause: Schema.Struct({ message: Schema.String }),
});
for (const fails of [false, true])
  test(`Signal ${fails ? "terminal child failure is watched" : "slow recovery queues input"} without blocking siblings`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const noticed = yield* Deferred.make<typeof TerminationLog.Type>();
          const clock = yield* TestClock.make();
          const messages = testConversations();
          const registry = yield* makeContextRegistry();
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(Clock.Clock, clock),
              Layer.succeed(SignalDefinitions, [definition, { ...definition, slug: "other" }]),
              Layer.succeed(AgentConversations, {
                ...messages,
                read: (path) =>
                  Effect.gen(function* () {
                    if (path === "/signals/slow") {
                      yield* Deferred.succeed(entered, undefined);
                      yield* Deferred.await(release);
                      if (fails) return yield* Effect.die(new Error("Signal recovery failed"));
                    }
                    return yield* messages.read(path);
                  }),
              }),
              Logger.layer([
                Logger.make<unknown, void>(({ message }) => {
                  if (!Array.isArray(message)) return;
                  for (const entry of message)
                    if (Schema.is(TerminationLog)(entry))
                      Deferred.doneUnsafe(noticed, Effect.succeed(entry));
                }),
              ]),
            ),
          );
          const root = yield* system.spawn("signals", SignalRootActor);
          yield* Deferred.await(entered);
          yield* root.awaitStarted;
          const react = (slug: string) =>
            root.ask<SignalCommandReply>((replyTo) => ({
              _tag: "React",
              replyTo,
              input: {
                source: "/system-one",
                requestId: slug,
                causationId: "source",
                target: `/signals/${slug}`,
                version: 1,
                sourceContext: {
                  revision: 0,
                  path: "/source",
                  description: "Evidence",
                  state: {},
                  messages: [],
                },
              },
            }));
          const waiting = yield* react("slow").pipe(Effect.forkScoped);
          assert.equal((yield* react("other"))._tag, "Accepted");
          assert.equal(waiting.pollUnsafe(), undefined);
          yield* Deferred.succeed(release, undefined);
          if (fails) {
            yield* clock.adjust("10 seconds");
            assert.match((yield* Deferred.await(noticed)).cause.message, /Signal recovery failed/);
            yield* Fiber.interrupt(waiting);
          } else assert.equal((yield* Fiber.join(waiting))._tag, "Accepted");
          assert.equal((yield* react("other"))._tag, "Accepted");
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });

for (const operation of ["pause", "delete"] as const)
  test(`restored ${operation} reconciles a lost receipt without sending again`, async () => {
    const records = new Map<string, StoredContext>();
    const messages = testConversations();
    const path = "/signals/personal--watch";
    const input: SignalChangeInput = {
      requestId: "create",
      source: "/goals/personal",
      target: path,
      remainingAgentTurns: 3,
      change: { operation: "create", definition },
    };
    // Simulate a lost sender acknowledgement after the receiver committed admission.
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* taskFixture({ records, conversations: messages });
          const activation = yield* Deferred.make<void>();
          const root = yield* env.system.spawn("signals", SignalRootActor, {
            metadata: { signalActivation: activation },
          });
          assert.equal(
            (yield* root.ask<SignalCommandReply>((replyTo) => ({
              _tag: "Change",
              input,
              replyTo,
            })))._tag,
            "Accepted",
          );
          assert.equal(
            (yield* root.ask<SignalCommandReply>((replyTo) => ({
              _tag: "React",
              replyTo,
              input: {
                requestId: "reaction",
                source: "/system-one",
                causationId: "source",
                target: path,
                version: 1,
                sourceContext: {
                  revision: 0,
                  path: "/source",
                  description: "Evidence",
                  state: {},
                  messages: [],
                },
              },
            })))._tag,
            "Accepted",
          );
        }),
      ),
    );
    // Seed the two records that survived interruption, without running a second writer beside an Actor.
    const history = await Effect.runPromise(readSignalHistory(messages, path));
    const message = history.deliveries[0]!.message;
    await Effect.runPromise(
      messages.append(path, "event:3", "signal.event", {
        _tag: "DeliveryChanged",
        requestId: message.requestId,
        status: "sending",
      }),
    );
    await Effect.runPromise(
      messages.append(path, "event:4", "signal.event", {
        _tag: "Changed",
        snapshot: {
          ...history.snapshot!,
          status: operation === "pause" ? "paused" : "deleted",
          version: 2,
        },
      }),
    );
    const goal = records.get("/goals/personal")!;
    records.set(goal.snapshot.path, {
      ...goal,
      snapshot: {
        ...goal.snapshot,
        state: {
          ...goal.snapshot.state,
          receipts: [
            {
              requestId: message.requestId,
              receipt: { requestId: message.requestId, revision: 2 },
            },
          ],
        },
      },
    });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* taskFixture({ records, conversations: messages });
          const delivered = yield* Stream.runHead(
            env.system.events.pipe(
              Stream.filter(
                (event) =>
                  event._tag === "CommandProcessed" &&
                  event.path === `/user${path}` &&
                  event.commandTag === "Delivered",
              ),
            ),
          ).pipe(Effect.forkScoped({ startImmediately: true }));
          yield* env.system.spawn("signals", SignalRootActor);
          yield* Fiber.join(delivered);
          assert.equal(
            (yield* readSignalHistory(messages, path)).deliveries[0]!.status,
            "delivered",
          );
          assert.equal(env.feedback.length, 0);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
