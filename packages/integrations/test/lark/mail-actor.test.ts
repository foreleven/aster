import { DurableContext } from "@aster/core";
import { ContextQueries } from "@aster/core";
// Tests inject internal completion and timer messages through untyped selections.
import { ActorSystem } from "@aster/actor";
import { contextSpawnOptions } from "@aster/core";
import { makeTestContext } from "@aster/core/testing";
import {
  LarkCliError,
  LarkSettings,
  LarkMailActor,
  LarkMailClient,
  type EmailData,
} from "../../src/lark/testing.js";
import { Clock, Deferred, Effect, Layer, Queue, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import assert from "node:assert/strict";
import { test } from "node:test";

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
          const registry = yield* makeTestContext();
          const controlledRegistry: typeof registry = {
            ...registry,
            backend: {
              ...registry.backend,
              open: (options) =>
                registry.backend.open(options).pipe(
                  Effect.map((owner) => ({
                    ...owner,
                    state: {
                      ...owner.state,
                      set: (state, commitOptions) =>
                        Effect.gen(function* () {
                          if (options.path === "/lark/mail/me/one") yield* Deferred.await(release);
                          const result = yield* owner.state.set(state, commitOptions);
                          if (options.path === "/lark/mail/me/one")
                            yield* Deferred.succeed(saved, undefined);
                          return result;
                        }),
                    },
                  })),
                ),
            },
          };
          let attempts = 0;
          let fetches = 0;
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              ContextQueries.layer,
              Layer.mergeAll(Layer.succeed(DurableContext, controlledRegistry.backend)),
              Layer.succeed(LarkSettings, {
                description: "Mail",
                mail: { mailbox: "me", description: "Mail", pollIntervalMs: 30_000 },
              }),
              Layer.succeed(LarkMailClient, {
                listMessages: () => Effect.die("Unexpected mail query"),
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
            LarkMailActor,
            contextSpawnOptions("/lark/mail"),
          );
          assert.deepEqual(yield* Queue.take(windows), [midnight, midnight + 30 * 60_000]);
          yield* clock.adjust(30_000);
          assert.deepEqual(yield* Queue.take(windows), [midnight, midnight + 30 * 60_000 + 30_000]);
          yield* Deferred.await(fetched);
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
          const through = Date.parse(
            (yield* registry.backend.readState(
              "/lark/mail",
              Schema.Struct({ through: Schema.String }),
            )).through,
          );
          yield* system.stop(actor);
          yield* system.spawn("mail", LarkMailActor, contextSpawnOptions("/lark/mail"));
          assert.equal((yield* Queue.take(windows))[0], Math.max(midnight, through - 60_000));
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
            const registry = yield* makeTestContext();
            const system = yield* ActorSystem.make().pipe(
              ActorSystem.provide(
                ContextQueries.layer,
                Layer.mergeAll(Layer.succeed(DurableContext, registry.backend)),
                Layer.succeed(LarkSettings, {
                  description: "Mail",
                  mail: { mailbox: "me", description: "Mail", pollIntervalMs: 30_000 },
                }),
                Layer.succeed(LarkMailClient, {
                  listMessages: () => Effect.die("Unexpected mail query"),
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
            yield* system.spawn("mail", LarkMailActor, contextSpawnOptions("/lark/mail"));
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
          const registry = yield* makeTestContext();
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              ContextQueries.layer,
              Layer.mergeAll(Layer.succeed(DurableContext, registry.backend)),
              Layer.succeed(LarkSettings, {
                description: "Mail",
                mail: { mailbox: "me", description: "Mail", pollIntervalMs: 30_000 },
              }),
              Layer.succeed(LarkMailClient, {
                listMessages: () => Effect.die("Unexpected mail query"),
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
          yield* system.spawn("mail", LarkMailActor, contextSpawnOptions("/lark/mail"));
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
        const registry = yield* makeTestContext();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            ContextQueries.layer,
            Layer.mergeAll(Layer.succeed(DurableContext, registry.backend)),
            Layer.succeed(LarkSettings, {
              description: "Mail",
              mail: { mailbox: "me", description: "Mail", pollIntervalMs: 30_000 },
            }),
            Layer.succeed(LarkMailClient, {
              listMessages: () => Effect.die("Unexpected mail query"),
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
        const actor = yield* system.spawn("mail", LarkMailActor, contextSpawnOptions("/lark/mail"));
        yield* Deferred.await(entered);
        yield* system.stop(actor);
        assert.equal(interrupted, true);
        assert.equal(registry.get("/lark/mail/me/one"), undefined);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

for (const mailbox of ["me", "other"])
  test(`mailbox restart preserves only the matching profile before remote refresh: ${mailbox}`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const saved = {
            text: JSON.stringify({ address: "saved@example.com", name: "Saved" }),
            path: "/lark/mail",
            description: "Mailbox",
            revision: 7,
            state: { mailbox: "me", profile: { address: "saved@example.com", name: "Saved" } },
            messages: [],
          };
          const registry = yield* makeTestContext({
            loadAll: () => [{ snapshot: saved, events: [] }],
            save: () => {},
          });
          const entered = yield* Deferred.make<void>();
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.mergeAll(Layer.succeed(DurableContext, registry.backend)),
              ContextQueries.layer,
              Layer.succeed(LarkSettings, {
                description: "Account",
                mail: { mailbox, description: "Mailbox", pollIntervalMs: 60_000 },
              }),
              Layer.succeed(LarkMailClient, {
                listMessages: () => Effect.die("Unexpected mail query"),
                getMailboxProfile: () =>
                  Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
                listIds: () => Effect.succeed([]),
                getMessages: () => Effect.succeed([]),
              }),
            ),
          );
          yield* system.spawn("mail", LarkMailActor, contextSpawnOptions("/lark/mail"));
          yield* Deferred.await(entered);
          const restored = registry.get("/lark/mail")!;
          if (mailbox === "me") assert.deepEqual(restored, saved);
          else {
            assert.deepEqual(restored.state, { mailbox: "other" });
            assert.equal(restored.revision, 8);
          }
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
