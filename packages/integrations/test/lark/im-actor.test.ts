import assert from "node:assert/strict";
import { test } from "node:test";
import { makeTestContext, makeTestContextWithBackend } from "@aster/core/testing";
import { Clock, Deferred, Effect, Fiber, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ChatState, serializeChat } from "../../src/lark/im/chat/state.js";
import { ChatMessage, ImState } from "../../src/lark/im/model.js";
import { installLark } from "./fixtures.js";
import { chatPath } from "../../src/lark/paths.js";
const chat = { id: "team", name: "Team", mode: "group", description: "" };
const message = {
  id: "m",
  at: "2026-10-10T01:00:00.000Z",
  content: "Decision",
  sender: {},
  url: "",
  deleted: false,
};
const now = Date.parse("2026-10-10T01:10:00Z");

test("IM advances one checkpoint only after every Chat durably acknowledges ingress", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(now);
        yield* Effect.gen(function* () {
          const registry = yield* makeTestContext();
          const saving = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const controlled = makeTestContextWithBackend(
            {
              ...registry.backend,
              open: (options) =>
                registry.backend.open(options).pipe(
                  Effect.map((owner) => ({
                    ...owner,
                    commit: (change, config) =>
                      Effect.gen(function* () {
                        if (options.path === chatPath(chat.id)) {
                          yield* Deferred.succeed(saving, undefined);
                          yield* Deferred.await(release);
                        }
                        return yield* owner.commit(change, config);
                      }),
                  })),
                ),
            },
            registry.records,
          );
          const { handle } = yield* installLark({
            registry: controlled,
            im: {
              searchMessages: () => Effect.succeed([{ chat, messages: [message] }]),
              getChatSettings: () => Effect.succeed([]),
              listMessages: () => Effect.die("Unexpected query"),
            },
          });
          yield* Deferred.await(saving);
          assert.deepEqual(yield* registry.backend.readState("/lark/im", ImState), {});
          assert.equal(
            (yield* registry.backend.readOwnerSnapshot(chatPath(chat.id), {
              state: ChatState,
              message: ChatMessage,
            }))?.messages.length,
            0,
          );
          const completed = yield* registry.backend.watch("/lark/im").pipe(
            Stream.filterEffect(() =>
              registry.backend
                .readState("/lark/im", ImState)
                .pipe(Effect.map((state) => state.through !== undefined)),
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(completed);
          assert.equal(
            (yield* registry.backend.readOwnerSnapshot(chatPath(chat.id), {
              state: ChatState,
              message: ChatMessage,
            }))?.messages.length,
            1,
          );
          assert.deepEqual(Object.keys(yield* registry.backend.readState("/lark/im", ImState)), [
            "through",
          ]);
          assert.ok(
            !registry.backend
              .directory()
              .some(({ path }) => path.includes("/retrieval/") || path.endsWith("/admission")),
          );
          yield* handle.stop;
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ).pipe(Effect.timeout("3 seconds")),
  );
});

test("restart activates pending prior-day Chats, keeps completed records dormant, and retrieves only today", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(now);
        yield* Effect.gen(function* () {
          const registry = yield* makeTestContext();
          for (const id of ["pending", "completed"])
            yield* Effect.scoped(
              registry.backend.open({
                path: chatPath(id),
                state: ChatState,
                message: ChatMessage,
                keyOf: (m) => m.id,
                serialize: serializeChat,
                initial: {
                  description: "Chat",
                  state: { chat: { ...chat, id } },
                  messages: id === "pending" ? [{ ...message, at: "2026-10-09T01:00:00Z" }] : [],
                },
              }),
            );
          yield* Effect.scoped(
            registry.backend.open({
              path: "/lark/im",
              state: ImState,
              message: Schema.Never,
              serialize: JSON.stringify,
              initial: { description: "IM", state: { through: "2026-10-08T10:00:00Z" } },
            }),
          );
          const entered = yield* Deferred.make<{ start: string; end: string }>();
          const summarized = yield* Deferred.make<void>();
          const { system, handle } = yield* installLark({
            registry,
            im: {
              searchMessages: (query) => Deferred.succeed(entered, query).pipe(Effect.as([])),
              getChatSettings: () => Effect.succeed([]),
              listMessages: () => Effect.die("Unexpected query"),
            },
            summarizer: {
              summarize: () =>
                Deferred.succeed(summarized, undefined).pipe(
                  Effect.as({ text: "Recovered", references: [] }),
                ),
            },
          });
          const query = yield* Deferred.await(entered);
          assert.equal(query.start, "2026-10-09T16:00:00.000Z");
          yield* Deferred.await(summarized);
          const actors = yield* system.inspect();
          assert.ok(actors.some(({ path }) => path.includes("/im/~") || path.endsWith("/pending")));
          assert.ok(!actors.some(({ path }) => path.endsWith("/completed")));
          assert.ok(!registry.backend.get("/lark/im/retrieval/days/2026-10-09"));
          yield* handle.stop;
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ).pipe(Effect.timeout("3 seconds")),
  );
});
