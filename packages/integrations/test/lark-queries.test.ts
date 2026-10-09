import { DurableContext } from "@aster/core";
import { ActorSystem } from "@aster/actor";
import {
  ContextQueries,
  ContextRegistry,
  ContextQueryError,
  childActorName,
  type ContextQueryInput,
} from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import { Clock, Effect, Layer, Queue, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import assert from "node:assert/strict";
import { test } from "node:test";
import { LarkRootActor } from "../src/lark/account/root-actor.js";
import { LarkAccountCli } from "../src/lark/account/client.js";
import { LarkConfig, parseLarkConfig } from "../src/lark/config.js";
import { LarkChatQueryError, LarkChatService } from "../src/lark/im/service/chat-service.js";
import { ChatSummaryGate } from "../src/lark/im/summary/gate.js";
import { ChatSummarizer } from "../src/lark/im/summary/summarizer.js";
import { ImAgentQueue, type AgentAdmission } from "../src/lark/im/summary/agent-queue.js";
import { ReadChatMessages } from "../src/lark/im/queries.js";
import { LarkMailCli, LarkResponseError } from "../src/lark/mail/client.js";
import { LarkCliError } from "../src/lark/shared/errors.js";
import { larkContextViews } from "../src/lark/public-views.js";

const email = {
  messageId: "retained",
  mailbox: "me",
  from: "Alice",
  subject: "Review",
  bodyPlainText: "Draft",
  attachments: [],
};
const chat = { id: "team", name: "Team", mode: "group", description: "Project" };
const summary = { text: "Latest work", references: [] };
const admission: AgentAdmission = { run: (_id, execute) => execute };

test("Lark queries use owner state and child Actors; provider history stays independent of summary retention", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(Date.parse("2026-10-09T04:00:00Z"));
        yield* Effect.gen(function* () {
          const registry = yield* makeContextRegistry({
            loadAll: () => [
              {
                snapshot: {
                  path: "/lark/im/chats/team",
                  revision: 1,
                  description: "Team",
                  state: { chat, summary, seen: {}, checkpoint: "PRIVATE" },
                  messages: [],
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
            ],
            save: () => {},
          });
          yield* registry.views.register(larkContextViews);
          const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
          let profileReads = 0,
            messageReads = 0,
            listReads = 0,
            historyReads = 0;
          let chatFailure: LarkChatQueryError | LarkCliError | undefined;
          let mailFailure: LarkResponseError | LarkCliError | undefined;
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.merge(
                Layer.succeed(ContextRegistry, registry),
                Layer.succeed(DurableContext, registry.backend),
              ),
              Layer.succeed(ContextQueries, queries),
              Layer.succeed(LarkConfig, { ...parseLarkConfig({}), im: {} }),
              Layer.succeed(LarkAccountCli, {
                getAccount: () =>
                  Effect.succeed({
                    openId: "me",
                    name: "Alice",
                    email: "alice@example.com",
                    enterpriseEmail: "",
                    secret: "PRIVATE",
                  }),
              }),
              Layer.succeed(LarkMailCli, {
                getMailboxProfile: () =>
                  Effect.sync(() => {
                    profileReads++;
                    return { address: "alice@example.com", name: "Inbox" };
                  }),
                listIds: () => Effect.never,
                listMessages: (_mailbox, args) =>
                  Effect.gen(function* () {
                    if (mailFailure) return yield* mailFailure;
                    listReads++;
                    assert.equal(args.from, "alice@example.com");
                    return {
                      items: [{ messageId: "remote", from: "Alice", subject: "History" }],
                      hasMore: true,
                      nextPageToken: "search:next",
                    };
                  }),
                getMessages: (_mailbox, ids) =>
                  Effect.gen(function* () {
                    if (mailFailure) return yield* mailFailure;
                    messageReads++;
                    return [{ ...email, messageId: ids[0]! }];
                  }),
              }),
              Layer.succeed(LarkChatService, {
                searchMessages: () => Effect.never,
                getChatSettings: () => Effect.die("Unexpected mute query"),
                listMessages: (args) =>
                  Effect.gen(function* () {
                    if (chatFailure) return yield* chatFailure;
                    historyReads++;
                    assert.equal(args.chatId, "unretained");
                    return {
                      items: [
                        {
                          id: "history",
                          at: "2026-01-01T00:00:00Z",
                          content: "Historical",
                          sender: {},
                          url: "",
                          deleted: false,
                        },
                      ],
                      hasMore: false,
                      nextPageToken: null,
                      coverage: { source: "provider", complete: true },
                    };
                  }),
              }),
              Layer.succeed(ChatSummaryGate, { needed: () => Effect.succeed(false) }),
              Layer.succeed(ChatSummarizer, {
                summarize: () => Effect.die("Queries must not summarize"),
              }),
              Layer.succeed(ImAgentQueue, admission),
            ),
          );
          const completed = (tag: string) =>
            system.events.pipe(
              Stream.filter(
                (event) => event._tag === "CommandProcessed" && event.commandTag === tag,
              ),
              Stream.toQueue({ capacity: "unbounded" }),
            );
          const accountLoaded = yield* completed("AccountLoaded"),
            profileLoaded = yield* completed("ProfileLoaded");
          const root = yield* system.spawn("lark", LarkRootActor);
          yield* root.awaitStarted;
          yield* Queue.take(accountLoaded);
          yield* Queue.take(profileLoaded);
          const before = registry.snapshot();
          const account = yield* queries.query({ path: "/lark", command: "profile", args: {} });
          assert.doesNotMatch(JSON.stringify(account), /PRIVATE/);
          const listedChats = yield* queries.query({
            path: "/lark/im",
            command: "list_chats",
            args: { query: "Project" },
          });
          assert.match(JSON.stringify(listedChats), /Team/);
          const current = yield* queries.query({
            path: "/lark/im",
            command: "summary",
            args: { chatId: "team" },
          });
          assert.deepEqual(current, { chat, summary });
          const history = yield* queries.query({
            path: "/lark/im",
            command: "messages",
            args: { chatId: "unretained", order: "asc", pageSize: 10 },
          });
          assert.match(JSON.stringify(history), /Historical/);
          assert.equal(historyReads, 1);
          const profile = yield* queries.query({
            path: "/lark/mail",
            command: "profile",
            args: {},
          });
          assert.deepEqual(profile, { address: "alice@example.com", name: "Inbox" });
          assert.equal(
            yield* queries.text({ path: "/lark/mail", command: "profile", args: {} }),
            "Inbox <alice@example.com>",
          );
          assert.equal(profileReads, 1);
          const retained = yield* queries.query({
            path: "/lark/mail",
            command: "read",
            args: { messageId: "retained" },
          });
          assert.deepEqual(retained, email);
          assert.equal(messageReads, 0);
          assert.ok(
            yield* system.select(`/user/lark/mail/${childActorName("me/retained")}`).resolve(),
          );
          const listed = yield* queries.query({
            path: "/lark/mail",
            command: "list",
            args: { from: "alice@example.com", limit: 10 },
          });
          assert.doesNotMatch(JSON.stringify(listed), /Draft|bodyPlainText/);
          assert.match(JSON.stringify(listed), /search:next/);
          assert.equal(listReads, 1);
          const remote = yield* queries.query({
            path: "/lark/mail",
            command: "read",
            args: { messageId: "remote" },
          });
          assert.deepEqual(remote, { ...email, messageId: "remote" });
          assert.equal(messageReads, 1);
          const invalid = yield* queries
            .query({ path: "/lark/im", command: "summary", args: { chatId: "team/invalid" } })
            .pipe(Effect.flip);
          assert.equal(Schema.decodeUnknownSync(ContextQueryError)(invalid).kind, "invalid-input");
          assert.deepEqual(registry.snapshot(), before);
          assert.equal(Schema.is(Schema.ObjectKeyword)(current), true);

          // Query routing preserves service error identity and local diagnostic causes.
          const cliError = new LarkCliError({
            message: "CLI unavailable",
            cause: new Error("transport"),
          });
          for (const error of [
            cliError,
            new LarkChatQueryError({ message: "Invalid history window", kind: "invalid-input" }),
          ]) {
            chatFailure = error;
            const failure = yield* queries
              .query({
                path: "/lark/im",
                command: "messages",
                args: { chatId: "unretained" },
              })
              .pipe(Effect.flip);
            assert.equal(failure, error);
            // The public transport exposes error fields, excluding private causes.
            const encoded = Schema.encodeUnknownSync(ReadChatMessages.errorSchema)(failure);
            assert.equal(encoded._tag, error._tag);
            assert.equal(encoded.message, error.message);
            assert.equal(Object.hasOwn(encoded, "cause"), false);
            assert.deepEqual(
              Schema.decodeUnknownSync(ReadChatMessages.errorSchema)(encoded),
              encoded,
            );
            assert.deepEqual(
              yield* queries
                .json({
                  path: "/lark/im",
                  command: "messages",
                  args: { chatId: "unretained" },
                })
                .pipe(Effect.flip),
              encoded,
            );
          }
          for (const error of [
            cliError,
            new LarkResponseError({ cause: "Invalid mail response", kind: "invalid-input" }),
          ]) {
            mailFailure = error;
            const inputs: readonly ContextQueryInput[] = [
              { path: "/lark/mail", command: "list", args: {} },
              { path: "/lark/mail", command: "read", args: { messageId: "remote" } },
            ];
            for (const input of inputs) {
              assert.equal(yield* queries.query(input).pipe(Effect.flip), error);
            }
          }
          assert.deepEqual(registry.snapshot(), before);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});
