import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect } from "effect";
import { makeAccountClient } from "../../src/lark/account/client.js";
import { makeMailClient } from "../../src/lark/mail/client.js";

test("profile clients decode actual envelopes and select only public identity", async () => {
  const account = makeAccountClient({
    run: () =>
      Effect.succeed(
        '{"ok":true,"data":{"user":{"name":"Test","open_id":"ou_test","access_token":"private"}}}',
      ),
  });
  assert.deepEqual(await Effect.runPromise(account.getAccount()), {
    openId: "ou_test",
    name: "Test",
    email: "",
    enterpriseEmail: "",
  });
  const mail = makeMailClient(() =>
    Effect.succeed('{"ok":true,"data":{"primary_email_address":"test@example.com"}}'),
  );
  assert.deepEqual(await Effect.runPromise(mail.getMailboxProfile("me")), {
    address: "test@example.com",
    name: "",
  });
});
for (const body of [
  '{"ok":false,"error":{"message":"upstream unavailable"}}',
  "invalid JSON",
  "{}",
  '{"data":{"messages":[{}]}}',
]) {
  test(`invalid provider payload fails instead of becoming an empty batch: ${body}`, async () => {
    const mail = makeMailClient(() => Effect.succeed(body));
    for (const operation of [
      mail.getMailboxProfile("me").pipe(Effect.asVoid),
      mail.listIds("me", 0, 1000).pipe(Effect.asVoid),
      mail.getMessages("me", ["id"]).pipe(Effect.asVoid),
    ]) {
      assert.equal((await Effect.runPromise(Effect.result(operation)))._tag, "Failure");
    }
    assert.equal(
      (
        await Effect.runPromise(
          Effect.result(makeAccountClient({ run: () => Effect.succeed(body) }).getAccount()),
        )
      )._tag,
      "Failure",
    );
  });
}

import { Clock, Deferred, Fiber, Stream } from "effect";
import { TestClock } from "effect/testing";
import { makeTestContext } from "@aster/core/testing";
import { AccountState, serializeAccount } from "../../src/lark/account/model.js";
import { MailboxState, serializeMailbox } from "../../src/lark/mail/model.js";
import { LarkAccountError, LarkCliError } from "../../src/lark/errors.js";
import { Schema } from "effect";
import { installLark } from "./fixtures.js";
import { larkSettings } from "../../src/lark/testing.js";

test("cached profiles survive a failed startup refresh, retry on Clock, then end the refresh cycle", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(Date.parse("2026-10-10T01:00:00Z"));
        yield* Effect.gen(function* () {
          const registry = yield* makeTestContext();
          const cachedAccount = { openId: "me", name: "Cached", email: "", enterpriseEmail: "" };
          const cachedMail = { address: "cached@example.com", name: "Cached" };
          yield* Effect.scoped(
            registry.backend.open({
              path: "/lark",
              state: AccountState,
              message: Schema.Never,
              serialize: serializeAccount,
              initial: { state: { account: cachedAccount }, description: "Account" },
            }),
          );
          yield* Effect.scoped(
            registry.backend.open({
              path: "/lark/mail",
              state: MailboxState,
              message: Schema.Never,
              serialize: serializeMailbox,
              initial: {
                state: { mailbox: "me", profile: cachedMail, through: "2026-10-10T01:00:00Z" },
                description: "Mailbox",
              },
            }),
          );
          const accountFailed = yield* Deferred.make<void>();
          const mailFailed = yield* Deferred.make<void>();
          let accountCalls = 0;
          let mailCalls = 0;
          const { queries } = yield* installLark({
            registry,
            settings: larkSettings({
              children: { "/mail": { config: { pollIntervalMs: 90_000 } } },
            }),
            account: {
              getAccount: () =>
                Effect.gen(function* () {
                  if (++accountCalls === 1) {
                    yield* Deferred.succeed(accountFailed, undefined);
                    return yield* new LarkAccountError({ message: "offline", cause: "offline" });
                  }
                  return { ...cachedAccount, name: "Fresh" };
                }),
            },
            mail: {
              getMailboxProfile: () =>
                Effect.gen(function* () {
                  if (++mailCalls === 1) {
                    yield* Deferred.succeed(mailFailed, undefined);
                    return yield* new LarkCliError({ message: "offline", cause: "offline" });
                  }
                  return { ...cachedMail, name: "Fresh" };
                }),
              listIds: () => Effect.succeed([]),
              getMessages: () => Effect.succeed([]),
              listMessages: () => Effect.die("Unexpected query"),
            },
          });
          yield* Deferred.await(accountFailed);
          yield* Deferred.await(mailFailed);
          assert.deepEqual(
            yield* queries.query({ path: "/lark", command: "profile", args: {} }),
            cachedAccount,
          );
          assert.deepEqual(
            yield* queries.query({ path: "/lark/mail", command: "profile", args: {} }),
            cachedMail,
          );
          const through = (yield* registry.backend.readState("/lark/mail", MailboxState)).through;
          const refreshed = yield* registry.backend.watch("/lark/mail").pipe(
            Stream.filterEffect(() =>
              registry.backend
                .readState("/lark/mail", MailboxState)
                .pipe(Effect.map((state) => state.profile?.name === "Fresh")),
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* clock.adjust("30 seconds");
          yield* Fiber.join(refreshed);
          assert.deepEqual(yield* queries.query({ path: "/lark", command: "profile", args: {} }), {
            ...cachedAccount,
            name: "Fresh",
          });
          assert.equal(
            (yield* registry.backend.readState("/lark/mail", MailboxState)).through,
            through,
          );
          yield* clock.adjust("60 seconds");
          assert.equal(accountCalls, 2);
          assert.equal(mailCalls, 2);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ).pipe(Effect.timeout("3 seconds")),
  );
});
