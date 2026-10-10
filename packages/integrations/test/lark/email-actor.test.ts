import { DurableContext } from "@aster/core";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { makeTestContext } from "@aster/core/testing";
import { Effect, Layer } from "effect";
import { LarkEmailActor } from "../../src/lark/mail/email-actor.js";

test("email updates retain the owner-supplied Context description and identical replay stays unchanged", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeTestContext();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.mergeAll(Layer.succeed(DurableContext, registry.backend))),
        );
        const path = "/lark/mail/me/message";
        const actor = yield* system.spawn("mail", LarkEmailActor, {
          metadata: { contextPath: path },
        });
        const email = {
          messageId: "message",
          mailbox: "me",
          from: "Alice",
          subject: "First",
          bodyPlainText: "Evidence",
          attachments: [],
        };
        yield* actor.ask<void>((replyTo) => ({ _tag: "SetEmail", email, replyTo }));
        const initialized = registry.get(path)!;
        assert.equal(initialized.description, "An email in Lark mailbox me");
        yield* actor.ask<void>((replyTo) => ({ _tag: "SetEmail", email, replyTo }));
        assert.deepEqual(registry.get(path), initialized);
        yield* actor.ask<void>((replyTo) => ({
          _tag: "SetEmail",
          email: { ...email, subject: "Updated" },
          replyTo,
        }));
        const updated = registry.get(path)!;
        assert.equal(updated.description, initialized.description);
        assert.equal(updated.revision, initialized.revision + 1);
        assert.deepEqual(updated.state, { ...email, subject: "Updated" });
      }),
    ),
  );
});
