import { ActorSystem } from "@aster/actor";
import {
  ContextActor,
  ContextQueries,
  ContextRegistry,
  contextSpawnOptions,
  defineContext,
  type StoredContext,
} from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import { Effect, Layer, Schema } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { LarkConfig } from "../src/lark/config.js";
import { LarkMailCli } from "../src/lark/mail/client.js";
import { larkContextViews } from "../src/lark/public-views.js";
import {
  LarkAccountCommands,
  LarkImCommands,
  LarkMailCommands,
  makeLarkAccountQuery,
  makeLarkImQuery,
  makeLarkMailQuery,
} from "../src/lark/queries.js";

test("Lark queries expose selected evidence, identify partial chat coverage and keep historical reads local", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const email = {
          messageId: "retained",
          mailbox: "me",
          from: "Alice",
          subject: "Review",
          bodyPlainText: "Draft",
          attachments: [],
        };
        const records: StoredContext[] = [
          {
            snapshot: {
              path: "/lark",
              revision: 1,
              description: "Account",
              state: {
                account: {
                  openId: "me",
                  name: "Alice",
                  email: "alice@example.com",
                  enterpriseEmail: "",
                  secret: "PRIVATE",
                },
                private: "PRIVATE",
              },
              messages: [],
            },
            events: [],
          },
          {
            snapshot: {
              path: "/lark/im/chats/team",
              revision: 1,
              description: "Team",
              state: {
                chat: { id: "team", name: "Team", mode: "group", description: "Project" },
                summary: { text: "Latest work", references: [] },
                checkpoint: "PRIVATE",
              },
              messages: [
                {
                  id: "m1",
                  at: "2026-10-07T00:00:00Z",
                  content: "Hello",
                  sender: { name: "Alice", private: "PRIVATE" },
                  url: "url",
                  deleted: false,
                },
              ],
            },
            events: [],
          },
          {
            snapshot: {
              path: "/lark/mail/me/retained",
              revision: 1,
              description: "Mail",
              state: email,
              messages: [],
            },
            events: [],
          },
        ];
        const registry = yield* makeContextRegistry({
          loadAll: () => records,
          save: () => {
            throw new Error("Queries must not persist Contexts");
          },
        });
        yield* registry.views.register(larkContextViews);
        const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
        let reads = 0;
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ContextQueries, queries),
            Layer.succeed(LarkConfig, {
              description: "Account",
              im: {},
              mail: { mailbox: "me", description: "Inbox", pollIntervalMs: 30_000 },
            }),
            Layer.succeed(LarkMailCli, {
              getMailboxProfile: () =>
                Effect.succeed({ address: "alice@example.com", name: "Inbox" }),
              listIds: () => Effect.succeed(["remote"]),
              getMessages: () =>
                Effect.sync(() => {
                  reads++;
                  return [{ ...email, messageId: "remote" }];
                }),
            }),
          ),
        );
        const context = defineContext({
          state: Schema.Record(Schema.String, Schema.Unknown),
          message: Schema.Unknown,
        });
        const Account = ContextActor.define("test/query/account", {
          commands: LarkAccountCommands,
          context,
        })(Effect.map(makeLarkAccountQuery, (query) => ({ query, receive: () => Effect.void })));
        const Im = ContextActor.define("test/query/im", { commands: LarkImCommands, context })(
          Effect.map(makeLarkImQuery, (query) => ({ query, receive: () => Effect.void })),
        );
        const Mail = ContextActor.define("test/query/mail", {
          commands: LarkMailCommands,
          context,
        })(Effect.map(makeLarkMailQuery, (query) => ({ query, receive: () => Effect.void })));
        yield* (yield* system.spawn("account", Account, contextSpawnOptions("/lark"))).awaitStarted;
        yield* (yield* system.spawn("im", Im, contextSpawnOptions("/lark/im"))).awaitStarted;
        yield* (yield* system.spawn("mail", Mail, contextSpawnOptions("/lark/mail"))).awaitStarted;
        const before = registry.snapshot();
        assert.equal((yield* queries.list()).total, 3);
        for (const [path, command, args] of [
          ["/lark", "profile", {}],
          ["/lark/im", "list_chats", {}],
          ["/lark/im", "summary", { id: "team" }],
          ["/lark/im", "messages", { id: "team" }],
        ] as const) {
          const result = yield* queries.query({ path, command, args });
          assert.doesNotMatch(JSON.stringify(result.data), /PRIVATE|checkpoint/);
          if (command === "messages") assert.match(JSON.stringify(result.data), /"complete":false/);
        }
        const retained = yield* queries.query({
          path: "/lark/mail",
          command: "read",
          args: { id: "retained" },
        });
        assert.deepEqual(retained.data, email);
        assert.equal(reads, 0);
        const listed = yield* queries.query({
          path: "/lark/mail",
          command: "list",
          args: { date: "2026-10-06" },
        });
        assert.doesNotMatch(JSON.stringify(listed.data), /Draft|bodyPlainText/);
        assert.equal(reads, 1);
        assert.deepEqual(registry.snapshot(), before);
      }),
    ),
  );
});
