import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Effect, Fiber, Layer } from "effect";
import { ContextQueries, ContextRegistry, RuntimeIntegrations } from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import { MailIntegration } from "../src/mail/integration.js";

const configuration = ConfigProvider.fromUnknown({
  contexts: {
    "/mail": {
      description: "Connected mailboxes",
      config: {
        mailboxes: [
          { id: "work", host: "imap.example.com", username: "alice", password: "secret" },
        ],
      },
    },
  },
});

test("configured mail installs a runtime source without opening connections", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* Effect.gen(function* () {
          const modules = yield* RuntimeIntegrations;
          yield* Effect.void.pipe(Effect.provide(MailIntegration.layer));
          assert.deepEqual(
            modules.installed().map((module) => module.name),
            ["mail"],
          );
          assert.equal(Object.keys(registry.snapshot()).length, 0);
        }).pipe(
          Effect.provide(Layer.merge(RuntimeIntegrations.layer, ContextQueries.layer)),
          Effect.provideService(ContextRegistry, registry),
          Effect.provide(ConfigProvider.layer(configuration)),
        );
      }),
    ),
  );
});

import { ActorSystem } from "@aster/actor";
import { Clock, Deferred, Logger, Queue, Redacted, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { type StoredContext } from "@aster/core";
import { MailFetcher } from "../src/mail/client.js";
import { MailSettings } from "../src/mail/config.js";
import { MailFetchError } from "../src/mail/errors.js";
import { mailboxPath, mailMessagePath } from "../src/mail/contexts.js";
import { MailboxSnapshot as MailboxState } from "../src/mail/state/snapshot.js";
import { type MailMessage } from "../src/mail/model.js";

const settings: MailSettings["Service"] = {
  description: "Connected mailboxes",
  pollIntervalMs: 1_000,
  mailboxes: [
    { id: "work", host: "imap.example.com", username: "alice", password: Redacted.make("secret") },
  ],
};
const email: MailMessage = {
  id: "imap:INBOX:1:42/a",
  mailbox: "work",
  from: "Alice",
  to: ["Bob"],
  subject: "Review",
  text: "Please review",
  date: "1970-01-01T00:00:00.000Z",
};

const install = Effect.fnUntraced(function* (
  registry: ContextRegistry["Service"],
  fetcher: MailFetcher["Service"],
  config = settings,
) {
  return yield* Effect.gen(function* () {
    const modules = yield* RuntimeIntegrations;
    const queries = yield* ContextQueries;
    yield* Effect.void.pipe(Effect.provide(MailIntegration.installation));
    const module = modules.installed()[0]!;
    const logs: unknown[] = [];
    const system = yield* ActorSystem.make().pipe(
      ActorSystem.provide(
        Layer.succeedContext(module.services),
        Logger.layer([
          Logger.make((options) => {
            logs.push(options.message);
          }),
        ]),
      ),
    );
    const completed = yield* Queue.unbounded<void>();
    yield* Stream.runForEach(system.events, (event) =>
      event._tag === "CommandProcessed" && event.commandTag === "Published"
        ? Queue.offer(completed, undefined)
        : Effect.void,
    ).pipe(Effect.forkScoped);
    yield* Effect.yieldNow;
    return { system, completed, logs, queries, handle: yield* module.activate(system) };
  }).pipe(
    Effect.provide(Layer.merge(RuntimeIntegrations.layer, ContextQueries.layer)),
    Effect.provideService(ContextRegistry, registry),
    Effect.provideService(MailSettings, config),
    Effect.provideService(MailFetcher, fetcher),
  );
});

const fakeFetcher = (
  pull: (
    mailbox: import("../src/mail/model.js").Mailbox,
  ) => Effect.Effect<readonly MailMessage[], MailFetchError>,
): MailFetcher["Service"] => ({
  inventory: () => Effect.succeed([]),
  pull: (mailbox) =>
    pull(mailbox).pipe(
      Effect.map((messages) => ({
        ids: [...new Set(messages.map((email) => email.id))],
        messages,
        undated: 0,
      })),
    ),
  list: () => Effect.die("Unexpected query"),
  read: () => Effect.die("Unexpected read"),
});

test("mail publishes its tree before retrieval and acknowledges persistence before readiness", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* Effect.gen(function* () {
          const registry = yield* makeContextRegistry();
          const fetching = yield* Deferred.make<void>();
          const releaseFetch = yield* Deferred.make<void>();
          const saving = yield* Deferred.make<void>();
          const releaseSave = yield* Deferred.make<void>();
          const controlled: typeof registry = {
            ...registry,
            commit: (record, options) =>
              record.path === mailMessagePath(email)
                ? Deferred.succeed(saving, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseSave)),
                    Effect.andThen(registry.commit(record, options)),
                  )
                : registry.commit(record, options),
          };
          const { handle, completed } = yield* install(
            controlled,
            fakeFetcher(() =>
              Deferred.succeed(fetching, undefined).pipe(
                Effect.andThen(Deferred.await(releaseFetch)),
                Effect.as([email, email]),
              ),
            ),
          );
          const readyObserved = yield* Deferred.make<void>();
          yield* handle.ready.pipe(
            Effect.andThen(Deferred.succeed(readyObserved, undefined)),
            Effect.forkScoped,
          );
          yield* Deferred.await(fetching);
          const publicRecords = registry.reader.snapshot();
          assert.equal(publicRecords["/mail"]?.projection?.visibility, "public");
          assert.equal(publicRecords[mailboxPath("work")]?.projection?.visibility, "public");
          assert.ok(!JSON.stringify(publicRecords).includes("secret"));
          assert.ok(!JSON.stringify(publicRecords).includes("imap.example.com"));
          yield* Deferred.succeed(releaseFetch, undefined);
          yield* Deferred.await(saving);
          assert.equal(registry.get(mailMessagePath(email)), undefined);
          const progress = Schema.decodeUnknownSync(MailboxState)(
            registry.get(mailboxPath("work"))!.state,
          );
          assert.equal(progress.through, undefined);
          assert.deepEqual(progress.known, []);
          assert.equal(yield* Deferred.isDone(readyObserved), false);
          assert.equal(
            Schema.decodeUnknownSync(MailboxState)(registry.get(mailboxPath("work"))!.state).status,
            "syncing",
          );
          yield* Deferred.succeed(releaseSave, undefined);
          yield* handle.ready;
          yield* Queue.take(completed);
          const saved = registry.get(mailMessagePath(email))!;
          assert.equal(saved.revision, 1);
          assert.equal(saved.description, `Email from ${email.from}: ${email.subject}`);
          assert.deepEqual(saved.state, email);
          assert.equal(registry.views.project(saved).projection?.visibility, "public");
          yield* clock.adjust(1_000);
          yield* Queue.take(completed);
          assert.equal(registry.get(mailMessagePath(email))?.revision, 1);
          yield* handle.stop;
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ).pipe(Effect.timeout("10 seconds")),
  );
});

test("mail retries retrieval errors, isolates mailboxes, and interrupts active polls on stop", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* Effect.gen(function* () {
          const registry = yield* makeContextRegistry();
          const failed = yield* Deferred.make<void>();
          const waiting = yield* Deferred.make<void>();
          const cancelled = yield* Deferred.make<void>();
          let attempts = 0;
          const changes = yield* registry.subscribe;
          yield* Stream.runForEach(changes, (change) =>
            change.record.path === mailboxPath("work") &&
            Schema.is(MailboxState)(change.record.state) &&
            change.record.state.status === "error"
              ? Deferred.succeed(failed, undefined)
              : Effect.void,
          ).pipe(Effect.forkScoped);
          const { handle, completed, logs } = yield* install(
            registry,
            fakeFetcher((mailbox) =>
              Effect.gen(function* () {
                if (mailbox.id === "other") return [];
                attempts++;
                if (attempts === 1)
                  return yield* new MailFetchError({
                    mailbox: "work",
                    message: "secret server response",
                    cause: Object.assign(new Error("secret server response"), {
                      code: "ECONNRESET",
                    }),
                  });
                if (attempts === 2) return [email];
                return yield* Deferred.succeed(waiting, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.ensuring(Deferred.succeed(cancelled, undefined)),
                );
              }),
            ),
            {
              ...settings,
              mailboxes: [...settings.mailboxes, { ...settings.mailboxes[0]!, id: "other" }],
            },
          );
          yield* Deferred.await(failed);
          yield* Queue.take(completed);
          assert.equal(
            Schema.decodeUnknownSync(MailboxState)(registry.get(mailboxPath("other"))!.state)
              .status,
            "ready",
          );
          assert.ok(!JSON.stringify(registry.reader.snapshot()).includes("secret"));
          assert.match(JSON.stringify(logs), /mail.poll.failed/);
          assert.match(JSON.stringify(logs), /ECONNRESET/);
          assert.ok(!JSON.stringify(logs).includes("secret"));
          assert.match(
            Schema.decodeUnknownSync(MailboxState)(registry.get(mailboxPath("work"))!.state)
              .lastFailure!.code!,
            /ECONNRESET/,
          );
          yield* clock.adjust(1_000);
          yield* handle.ready;
          yield* Queue.take(completed);
          yield* Queue.take(completed);
          assert.match(JSON.stringify(logs), /mail.poll.recovered/);
          assert.equal(
            Schema.decodeUnknownSync(MailboxState)(registry.get(mailboxPath("work"))!.state)
              .lastFailure,
            undefined,
          );
          yield* clock.adjust(1_000);
          yield* Deferred.await(waiting);
          yield* handle.stop;
          yield* Deferred.await(cancelled);
          yield* clock.adjust(10_000);
          assert.equal(attempts, 3);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ).pipe(Effect.timeout("10 seconds")),
  );
});

test("mail restart preserves email revisions and waits for a fresh initial poll", async () => {
  const records = new Map<string, StoredContext>();
  const store = {
    loadAll: () => [...records.values()],
    save: (record: StoredContext) => {
      records.set(record.snapshot.path, structuredClone(record));
    },
  };
  for (let run = 0; run < 2; run++) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry(store);
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const { handle } = yield* install(
            registry,
            fakeFetcher(() =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as([email]),
              ),
            ),
          );
          yield* Deferred.await(entered);
          assert.equal(
            Schema.decodeUnknownSync(MailboxState)(registry.get(mailboxPath("work"))!.state).status,
            "syncing",
          );
          if (run === 1)
            assert.equal(
              registry.views.project(registry.get(mailMessagePath(email))!).projection?.visibility,
              "public",
            );
          yield* Deferred.succeed(release, undefined);
          yield* handle.ready;
          assert.equal(registry.get(mailMessagePath(email))?.revision, 1);
          yield* handle.stop;
        }),
      ).pipe(Effect.timeout("10 seconds")),
    );
  }
});

test("missing generic mail config installs no source and requires no credentials", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* Effect.gen(function* () {
          const modules = yield* RuntimeIntegrations;
          yield* Effect.void.pipe(Effect.provide(MailIntegration.layer));
          assert.deepEqual(modules.installed(), []);
        }).pipe(
          Effect.provide(Layer.merge(RuntimeIntegrations.layer, ContextQueries.layer)),
          Effect.provideService(ContextRegistry, registry),
          Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
        );
      }),
    ),
  );
});

for (const config of [
  { mailboxes: [] },
  {
    mailboxes: [
      { id: "same", host: "unused", username: "u", password: "p" },
      { id: "same", host: "unused", username: "u", password: "p" },
    ],
  },
  { pollIntervalMs: 0, mailboxes: [{ id: "work", host: "unused", username: "u", password: "p" }] },
  {
    mailboxes: [
      { id: "work", host: "unused", username: "u", password: "p", timeZone: "Invalid/Zone" },
    ],
  },
])
  test(`invalid mail config fails acquisition: ${JSON.stringify(config)}`, async () => {
    await assert.rejects(
      Effect.runPromise(
        Effect.gen(function* () {
          return yield* MailSettings;
        }).pipe(
          Effect.provide(
            MailSettings.layer.pipe(
              Layer.provide(
                ConfigProvider.layer(
                  ConfigProvider.fromUnknown({ contexts: { "/mail": { config } } }),
                ),
              ),
            ),
          ),
        ),
      ),
    );
  });

test("mail recovery keeps its baseline, covers missed days, and indexes only today's messages", async () => {
  const records = new Map<string, StoredContext>();
  const store = {
    loadAll: () => [...records.values()],
    save: (record: StoredContext) => {
      records.set(record.snapshot.path, structuredClone(record));
    },
  };
  let inventories = 0;
  for (let run = 0; run < 2; run++) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const clock = yield* TestClock.make();
          yield* clock.setTime(
            Date.parse(run === 0 ? "2026-10-07T04:00:00Z" : "2026-10-09T04:00:00Z"),
          );
          yield* Effect.gen(function* () {
            const registry = yield* makeContextRegistry(store);
            const windows: import("../src/mail/model.js").MailboxWindow[] = [];
            const late = { ...email, id: "late", date: "2026-10-06T04:00:00Z" };
            const current = { ...email, id: "current", date: "2026-10-09T03:00:00Z" };
            const fetcher: MailFetcher["Service"] = {
              ...fakeFetcher(() => Effect.succeed([])),
              inventory: () =>
                Effect.sync(() => {
                  inventories++;
                  return ["old"];
                }),
              pull: (_, window, known) =>
                Effect.sync(() => {
                  windows.push(window);
                  assert.ok(known.includes("old"));
                  const messages = run === 1 && windows.length === 1 ? [late, current] : [];
                  return {
                    ids: run === 1 ? ["old", "late", "current"] : ["old"],
                    messages,
                    undated: 0,
                  };
                }),
            };
            const { handle } = yield* install(registry, fetcher);
            yield* handle.ready;
            const state = Schema.decodeUnknownSync(MailboxState)(
              registry.get(mailboxPath("work"))!.state,
            );
            if (run === 0) {
              assert.deepEqual(windows, [
                { from: "2026-10-07T00:00:00.000+08:00", through: "2026-10-07T12:00:00.000+08:00" },
              ]);
            } else {
              assert.equal(windows.length, 3);
              assert.equal(windows[0]!.from, "2026-10-07T12:00:00.000+08:00");
              assert.equal(windows[1]!.from, windows[0]!.through);
              assert.equal(windows[2]!.from, windows[1]!.through);
              assert.equal(state.through, "2026-10-09T12:00:00.000+08:00");
              assert.deepEqual(
                state.today.emails.map((m) => m.id),
                ["current"],
              );
              assert.equal(registry.get(mailMessagePath(late))?.revision, 1);
            }
            yield* handle.stop;
          }).pipe(Effect.provideService(Clock.Clock, clock));
        }),
      ).pipe(Effect.timeout("10 seconds")),
    );
  }
  assert.equal(inventories, 1);
});

test("mail historical queries leave discovery, cursor, Contexts and source events unchanged", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const historical = { ...email, id: "history", date: "2026-10-06T04:00:00Z" };
        let remoteReads = 0;
        const { handle, queries } = yield* install(registry, {
          ...fakeFetcher(() => Effect.succeed([email])),
          list: (_, window) =>
            Effect.sync(() => {
              assert.equal(window.from, "2026-10-06T00:00:00.000+08:00");
              return { ids: [historical.id], messages: [historical], undated: 2 };
            }),
          read: () =>
            Effect.sync(() => {
              remoteReads++;
              return historical;
            }),
        });
        yield* handle.ready;
        const before = registry.snapshot();
        const result = yield* queries.query({
          path: mailboxPath("work"),
          command: "list",
          args: { date: "2026-10-06" },
        });
        assert.doesNotMatch(JSON.stringify(result.data), /Please review/);
        assert.match(JSON.stringify(result.data), /undatedObserved/);
        yield* queries.query({
          path: mailboxPath("work"),
          command: "read",
          args: { id: historical.id },
        });
        yield* queries.query({
          path: mailboxPath("work"),
          command: "read",
          args: { id: email.id },
        });
        assert.equal(remoteReads, 1);
        assert.deepEqual(registry.snapshot(), before);
        assert.equal(
          (yield* Effect.flip(
            queries.query({
              path: mailboxPath("work"),
              command: "list",
              args: { date: "2026-02-30" },
            }),
          )).kind,
          "invalid-input",
        );
        yield* handle.stop;
        assert.equal((yield* queries.list()).total, 0);
      }),
    ).pipe(Effect.timeout("10 seconds")),
  );
});

test("mail rotates the default index while retrieval is blocked across midnight", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.setTime(Date.parse("2026-10-07T15:59:50Z"));
        yield* Effect.gen(function* () {
          const registry = yield* makeContextRegistry();
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const rotated = yield* Deferred.make<void>();
          yield* Stream.runForEach(yield* registry.subscribe, ({ record }) =>
            record.path === mailboxPath("work") &&
            Schema.is(MailboxState)(record.state) &&
            record.state.today.date === "2026-10-08"
              ? Deferred.succeed(rotated, undefined)
              : Effect.void,
          ).pipe(Effect.forkScoped);
          const previousDay = { ...email, date: "2026-10-07T14:00:00Z" };
          const { handle } = yield* install(
            registry,
            fakeFetcher(() =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as([previousDay]),
              ),
            ),
          );
          yield* Deferred.await(entered);
          yield* clock.adjust(20_000);
          yield* Deferred.await(rotated);
          yield* Deferred.succeed(release, undefined);
          yield* handle.ready;
          assert.deepEqual(
            Schema.decodeUnknownSync(MailboxState)(registry.get(mailboxPath("work"))!.state).today,
            { date: "2026-10-08", emails: [] },
          );
          assert.equal(registry.get(mailMessagePath(previousDay))?.revision, 1);
          yield* handle.stop;
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ).pipe(Effect.timeout("10 seconds")),
  );
});

test("mail failure and restart retain the initial population and retrieval boundary", async () => {
  const records = new Map<string, StoredContext>();
  const store = {
    loadAll: () => [...records.values()],
    save: (record: StoredContext) => {
      records.set(record.snapshot.path, structuredClone(record));
    },
  };
  let inventories = 0;
  for (let run = 0; run < 2; run++) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const clock = yield* TestClock.make();
          yield* clock.setTime(
            Date.parse(run === 0 ? "2026-10-07T04:00:00Z" : "2026-10-08T04:00:00Z"),
          );
          yield* Effect.gen(function* () {
            const registry = yield* makeContextRegistry(store);
            const failed = yield* Deferred.make<void>();
            yield* Stream.runForEach(yield* registry.subscribe, ({ record }) =>
              record.path === mailboxPath("work") &&
              Schema.is(MailboxState)(record.state) &&
              record.state.status === "error"
                ? Deferred.succeed(failed, undefined)
                : Effect.void,
            ).pipe(Effect.forkScoped);
            const { handle } = yield* install(registry, {
              ...fakeFetcher(() => Effect.succeed([])),
              inventory: () =>
                Effect.sync(() => {
                  inventories++;
                  return ["initial-old"];
                }),
              pull: (_, window, known) =>
                Effect.gen(function* () {
                  assert.deepEqual(known, ["initial-old"]);
                  if (run === 0)
                    return yield* new MailFetchError({
                      mailbox: "work",
                      message: "offline",
                      cause: undefined,
                    });
                  assert.ok(Date.parse(window.from) >= Date.parse("2026-10-06T16:00:00Z"));
                  return { ids: known, messages: [], undated: 0 };
                }),
            });
            if (run === 0) {
              yield* Deferred.await(failed);
              const state = Schema.decodeUnknownSync(MailboxState)(
                registry.get(mailboxPath("work"))!.state,
              );
              assert.equal(state.through, undefined);
              assert.equal(state.startedAt, "2026-10-07T12:00:00.000+08:00");
            } else yield* handle.ready;
            yield* handle.stop;
          }).pipe(Effect.provideService(Clock.Clock, clock));
        }),
      ).pipe(Effect.timeout("10 seconds")),
    );
  }
  assert.equal(inventories, 1);
});

test("mail queries share the polling connection permit and owner shutdown releases waiting queries", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const entered = yield* Deferred.make<void>();
        let reads = 0;
        const { handle, queries } = yield* install(registry, {
          ...fakeFetcher(() =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          ),
          read: () =>
            Effect.sync(() => {
              reads++;
              return email;
            }),
        });
        yield* Deferred.await(entered);
        const waiting = yield* queries
          .query({ path: mailboxPath("work"), command: "read", args: { id: email.id } })
          .pipe(Effect.flip, Effect.forkScoped({ startImmediately: true }));
        yield* handle.stop;
        assert.equal((yield* Fiber.join(waiting)).kind, "unavailable");
        assert.equal(reads, 0);
      }),
    ).pipe(Effect.timeout("10 seconds")),
  );
});
