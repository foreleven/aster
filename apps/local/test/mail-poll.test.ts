import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { ContextRegistry, contextSpawnOptions } from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import {
  LarkConfig,
  LarkEmailChannelActor,
  LarkMailCli,
  LarkCliError,
  type EmailData,
} from "@aster/integrations";
import { Clock, Deferred, Effect, Layer, Queue, Stream } from "effect";
import { TestClock } from "effect/testing";

const midnight = Date.parse("2026-10-03T00:00:00+08:00");
const email: EmailData = {
  messageId: "one",
  mailbox: "me",
  from: "Alice",
  subject: "Review",
  bodyPlainText: "Review",
  attachments: [],
};

test("mail retries failed windows, waits for durable publication, and deduplicates restart replay", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(midnight + 30 * 60_000);
        yield* Effect.gen(function* () {
          const windows = yield* Queue.unbounded<readonly [number, number]>();
          const fetched = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const saved = yield* Deferred.make<void>();
          const registry = yield* makeContextRegistry();
          const originalCommit = registry.commit;
          const controlledRegistry: typeof registry = {
            ...registry,
            commit: (update, options) =>
              Effect.gen(function* () {
                if (update.path === "/lark/mail/me/one") yield* Deferred.await(release);
                const result = yield* originalCommit(update, options);
                if (update.path === "/lark/mail/me/one") yield* Deferred.succeed(saved, undefined);
                return result;
              }),
          };
          let attempts = 0;
          let fetches = 0;
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, controlledRegistry),
              Layer.succeed(LarkConfig, {
                description: "Mail",
                mail: { mailbox: "me", description: "Mail", pollIntervalMs: 30_000 },
              }),
              Layer.succeed(LarkMailCli, {
                getMailboxProfile: () => Effect.never,
                listIds: (_mailbox, start, through) =>
                  Effect.gen(function* () {
                    yield* Queue.offer(windows, [start, through]);
                    if (++attempts === 1)
                      return yield* new LarkCliError({ cause: "EOF", message: "EOF" });
                    return ["one"];
                  }),
                getMessages: () =>
                  Effect.gen(function* () {
                    fetches++;
                    yield* Deferred.succeed(fetched, undefined);
                    return [email];
                  }),
              }),
            ),
          );
          const published = yield* Queue.unbounded<void>();
          yield* Stream.runForEach(system.events, (event) =>
            event._tag === "CommandProcessed" &&
            event.path === "/user/mail" &&
            event.commandTag === "Published"
              ? Queue.offer(published, undefined).pipe(Effect.asVoid)
              : Effect.void,
          ).pipe(Effect.forkScoped);
          yield* Effect.yieldNow;
          const actor = yield* system.spawn(
            "mail",
            LarkEmailChannelActor,
            contextSpawnOptions("/lark/mail"),
          );
          assert.deepEqual(yield* Queue.take(windows), [midnight, midnight + 30 * 60_000]);
          yield* clock.adjust(30_000);
          assert.deepEqual(yield* Queue.take(windows), [midnight, midnight + 30 * 60_000 + 30_000]);
          yield* Deferred.await(fetched);
          yield* actor.tell({ _tag: "Poll" });
          yield* clock.adjust(10_000);
          assert.equal(registry.get("/lark/mail/me/one"), undefined);
          assert.equal(yield* Queue.size(windows), 0);
          yield* Deferred.succeed(release, undefined);
          yield* Deferred.await(saved);
          yield* Queue.take(published);
          yield* clock.adjust(30_000);
          assert.deepEqual(yield* Queue.take(windows), [
            midnight + 29 * 60_000 + 30_000,
            midnight + 31 * 60_000 + 10_000,
          ]);
          assert.equal(fetches, 1);
          yield* system.stop(actor);
          yield* system.spawn("mail", LarkEmailChannelActor, contextSpawnOptions("/lark/mail"));
          assert.equal((yield* Queue.take(windows))[0], midnight);
          yield* clock.adjust(30_000);
          assert.equal(fetches, 1);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ).pipe(Effect.timeout("10 seconds")),
  );
});

for (const failure of ["transport", "incomplete"] as const)
  test(`mail does not advance when message fetch is ${failure}`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const clock = yield* TestClock.make();
          yield* clock.adjust(midnight + 30 * 60_000);
          yield* Effect.gen(function* () {
            const windows = yield* Queue.unbounded<number>();
            const registry = yield* makeContextRegistry();
            const system = yield* ActorSystem.make().pipe(
              ActorSystem.provide(
                Layer.succeed(ContextRegistry, registry),
                Layer.succeed(LarkConfig, {
                  description: "Mail",
                  mail: { mailbox: "me", description: "Mail", pollIntervalMs: 30_000 },
                }),
                Layer.succeed(LarkMailCli, {
                  getMailboxProfile: () => Effect.never,
                  listIds: (_mailbox, start) =>
                    Queue.offer(windows, start).pipe(Effect.as(["one"])),
                  getMessages: () =>
                    failure === "incomplete"
                      ? Effect.succeed([])
                      : Effect.fail(new LarkCliError({ cause: "EOF", message: "EOF" })),
                }),
              ),
            );
            yield* system.spawn("mail", LarkEmailChannelActor, contextSpawnOptions("/lark/mail"));
            assert.equal(yield* Queue.take(windows), midnight);
            yield* clock.adjust(30_000);
            assert.equal(yield* Queue.take(windows), midnight);
            assert.equal(registry.get("/lark/mail/me/one"), undefined);
          }).pipe(Effect.provideService(Clock.Clock, clock));
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });

test("empty mail windows catch up immediately, then wait for the polling interval", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        const now = midnight + 150 * 60_000;
        yield* clock.adjust(now);
        yield* Effect.gen(function* () {
          const windows = yield* Queue.unbounded<readonly [number, number]>();
          const registry = yield* makeContextRegistry();
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(LarkConfig, {
                description: "Mail",
                mail: { mailbox: "me", description: "Mail", pollIntervalMs: 30_000 },
              }),
              Layer.succeed(LarkMailCli, {
                getMailboxProfile: () => Effect.never,
                listIds: (_mailbox, start, through) =>
                  Queue.offer(windows, [start, through]).pipe(Effect.as([])),
                getMessages: () => Effect.die("Empty windows must not fetch message bodies"),
              }),
            ),
          );
          const published = yield* Queue.unbounded<void>();
          yield* Stream.runForEach(system.events, (event) =>
            event._tag === "CommandProcessed" &&
            event.path === "/user/mail" &&
            event.commandTag === "Published"
              ? Queue.offer(published, undefined).pipe(Effect.asVoid)
              : Effect.void,
          ).pipe(Effect.forkScoped);
          yield* Effect.yieldNow;
          yield* system.spawn("mail", LarkEmailChannelActor, contextSpawnOptions("/lark/mail"));
          assert.deepEqual(yield* Queue.take(windows), [midnight, midnight + 60 * 60_000]);
          assert.deepEqual(yield* Queue.take(windows), [
            midnight + 59 * 60_000,
            midnight + 119 * 60_000,
          ]);
          assert.deepEqual(yield* Queue.take(windows), [midnight + 118 * 60_000, now]);
          yield* Queue.take(published);
          yield* Queue.take(published);
          yield* Queue.take(published);
          yield* clock.adjust(29_999);
          assert.equal(yield* Queue.size(windows), 0);
          yield* clock.adjust(1);
          assert.deepEqual(yield* Queue.take(windows), [now - 60_000, now + 30_000]);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("stopping a mailbox interrupts its active retrieval", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        let interrupted = false;
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(LarkConfig, {
              description: "Mail",
              mail: { mailbox: "me", description: "Mail", pollIntervalMs: 30_000 },
            }),
            Layer.succeed(LarkMailCli, {
              getMailboxProfile: () => Effect.never,
              listIds: () =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.ensuring(
                    Effect.sync(() => {
                      interrupted = true;
                    }),
                  ),
                ),
              getMessages: () => Effect.die("Retrieval did not complete"),
            }),
          ),
        );
        const actor = yield* system.spawn(
          "mail",
          LarkEmailChannelActor,
          contextSpawnOptions("/lark/mail"),
        );
        yield* Deferred.await(entered);
        yield* system.stop(actor);
        assert.equal(interrupted, true);
        assert.equal(registry.get("/lark/mail/me/one"), undefined);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
