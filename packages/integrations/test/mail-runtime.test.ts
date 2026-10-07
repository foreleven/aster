import { ContextDescriptions } from "@aster/core";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Effect, Layer } from "effect";
import { ContextRegistry, RuntimeIntegrations } from "@aster/core";
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
          yield* Effect.void.pipe(
            Effect.provide(MailIntegration.layer.pipe(Layer.provide(ContextDescriptions.layer))),
          );
          assert.deepEqual(
            modules.installed().map((module) => module.name),
            ["mail"],
          );
          assert.equal(Object.keys(registry.snapshot()).length, 0);
        }).pipe(
          Effect.provide(RuntimeIntegrations.layer),
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
import { mailboxPath, mailMessagePath, MailboxState } from "../src/mail/contexts.js";
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
};

const install = Effect.fnUntraced(function* (
  registry: ContextRegistry["Service"],
  fetcher: MailFetcher["Service"],
  config = settings,
) {
  return yield* Effect.gen(function* () {
    const modules = yield* RuntimeIntegrations;
    yield* Effect.void.pipe(
      Effect.provide(MailIntegration.installation.pipe(Layer.provide(ContextDescriptions.layer))),
    );
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
    return { system, completed, logs, handle: yield* module.activate(system) };
  }).pipe(
    Effect.provide(RuntimeIntegrations.layer),
    Effect.provideService(ContextRegistry, registry),
    Effect.provideService(MailSettings, config),
    Effect.provideService(MailFetcher, fetcher),
  );
});

const fakeFetcher = (pull: MailFetcher["Service"]["pull"]): MailFetcher["Service"] => ({
  pull,
  pullAll: () => Effect.die("Runtime must poll each mailbox independently"),
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
          assert.equal(yield* Deferred.isDone(readyObserved), false);
          assert.equal(
            Schema.decodeUnknownSync(MailboxState)(registry.get(mailboxPath("work"))!.state).status,
            "starting",
          );
          yield* Deferred.succeed(releaseSave, undefined);
          yield* handle.ready;
          yield* Queue.take(completed);
          const saved = registry.get(mailMessagePath(email))!;
          assert.equal(saved.revision, 1);
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
              .lastError!,
            /ECONNRESET/,
          );
          yield* clock.adjust(1_000);
          yield* handle.ready;
          yield* Queue.take(completed);
          yield* Queue.take(completed);
          assert.match(JSON.stringify(logs), /mail.poll.recovered/);
          assert.equal(
            Schema.decodeUnknownSync(MailboxState)(registry.get(mailboxPath("work"))!.state)
              .lastError,
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
            "starting",
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
          yield* Effect.void.pipe(
            Effect.provide(MailIntegration.layer.pipe(Layer.provide(ContextDescriptions.layer))),
          );
          assert.deepEqual(modules.installed(), []);
        }).pipe(
          Effect.provide(RuntimeIntegrations.layer),
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
  { mailboxes: [{ id: "work", host: "unused", username: "u", password: "p", maxMessages: 0 }] },
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
