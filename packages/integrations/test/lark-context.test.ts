import { DurableContext } from "@aster/core";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { ContextRegistry } from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import { Effect, Layer } from "effect";
import { LarkMailMessageActor } from "../src/lark/mail/message-actor.js";

test("email updates retain the owner-supplied Context description and identical replay stays unchanged", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.merge(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(DurableContext, registry.backend),
            ),
          ),
        );
        const path = "/lark/mail/me/message";
        const actor = yield* system.spawn("mail", LarkMailMessageActor, {
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
