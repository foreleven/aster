import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { Clock, Deferred, Effect, Fiber, Layer, Ref, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ActorSystem } from "@aster/actor";
import { ContextRegistry, contextSpawnOptions } from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import {
  ChatSummarizer,
  ImAgentQueue,
  ImStorage,
  ImSummaryGate,
  LarkChatActor,
  makeImStorage,
  makeImSummaryGate,
  imDate,
  type ChatSummaryInput,
} from "@aster/integrations";

const chat = { id: "review", name: "Review", mode: "group", description: "" };
const message = {
  id: "one",
  at: new Date().toISOString(),
  content: "Evidence",
  sender: {},
  url: "",
  deleted: false,
};
const path = "/lark/im/chats/review";

test("IM model defects reach Actor supervision without a retry checkpoint", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "aster-im-defect-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const storage = makeImStorage(dir);
  storage.ingest({ chat, messages: [message] });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            Layer.succeed(ImAgentQueue, { run: (_id, work) => work }),
            Layer.succeed(
              ImSummaryGate,
              makeImSummaryGate({ systemOne: () => Effect.die(new Error("Model defect")) }),
            ),
            Layer.succeed(ChatSummarizer, { summarize: () => Effect.never }),
          ),
        );
        const stopped = yield* Stream.runHead(
          system.events.pipe(Stream.filter((event) => event._tag === "ActorStopped")),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* system.spawn(
          "chat",
          LarkChatActor,
          contextSpawnOptions(path, { supervision: () => "stop" }),
        );
        const event = yield* Fiber.join(stopped).pipe(Effect.timeout("2 seconds"));
        assert.equal(event._tag, "Some");
        if (event._tag === "Some" && event.value._tag === "ActorStopped")
          assert.match(event.value.cause!.message, /Model defect/);
        const day = storage.get(imDate(message.at), chat.id)!;
        assert.equal(day.retryAt, undefined);
        assert.equal(day.lastError, undefined);
        assert.equal(day.pending.length, 1);
      }),
    ),
  );
});

test("Chat stop interrupts model work and stale checkpoint commands cannot mutate the inbox", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "aster-im-interrupt-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const storage = makeImStorage(dir);
  storage.ingest({ chat, messages: [message] });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        let released = false;
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            Layer.succeed(ImAgentQueue, { run: (_id, work) => work }),
            Layer.succeed(ImSummaryGate, { needed: () => Effect.succeed(true) }),
            Layer.succeed(ChatSummarizer, {
              summarize: () =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.ensuring(
                    Effect.sync(() => {
                      released = true;
                    }),
                  ),
                ),
            }),
          ),
        );
        const actor = yield* system.spawn("chat", LarkChatActor, contextSpawnOptions(path));
        yield* Deferred.await(entered);
        const date = imDate(message.at);
        const before = storage.get(date, chat.id);
        yield* actor.tell({
          _tag: "Summarized",
          date,
          generation: "retired",
          result: {
            _tag: "Success",
            value: {
              batch: [message],
              daily: { text: "Stale daily", references: [] },
              rolling: { text: "Stale rolling", references: [] },
              evaluate: true,
              updatedAt: message.at,
            },
          },
        });
        // This mailbox acknowledgement also fences the preceding stale completion.
        const reply = yield* actor.ask((replyTo) => ({
          _tag: "Checkpoint",
          date,
          generation: "retired",
          change: { _tag: "Daily", summary: { text: "Stale", references: [] } },
          replyTo,
        }));
        assert.equal(reply, undefined);
        assert.deepEqual(storage.get(date, chat.id), before);
        assert.deepEqual(registry.get(path)?.state, { chat });
        yield* system.stop(actor);
        assert.equal(released, true);
        assert.equal(storage.get(date, chat.id)!.stage!.daily, undefined);
        assert.equal(storage.get(date, chat.id)!.pending.length, 1);
      }),
    ),
  );
});

test("concurrent Chat Actors keep summary state isolated and retain arrivals during execution", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "aster-im-isolation-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        const at = "2026-10-04T00:00:00.000Z";
        yield* clock.adjust(Date.parse(at));
        const date = imDate(at);
        const other = { ...chat, id: "other" };
        const otherPath = "/lark/im/chats/other";
        const first = { ...message, at };
        const second = { ...first, id: "two", content: "Arrived during execution" };
        const otherMessage = { ...first, id: "other-one" };
        const firstStarted = yield* Deferred.make<void>();
        const otherStarted = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const firstFinished = yield* Deferred.make<void>();
        const otherFinished = yield* Deferred.make<void>();
        const inputs = yield* Ref.make<ReadonlyArray<ChatSummaryInput>>([]);
        const base = makeImStorage(dir);
        base.ingest({ chat, messages: [first] });
        base.ingest({ chat: other, messages: [otherMessage] });
        const storage: ImStorage["Service"] = {
          ...base,
          finish: (date, id, commit) => {
            base.finish(date, id, commit);
            if (base.get(date, id)?.pending.length === 0)
              Deferred.doneUnsafe(id === chat.id ? firstFinished : otherFinished, Effect.void);
          },
        };
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(Clock.Clock, clock),
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            Layer.succeed(ImAgentQueue, { run: (_id, work) => work }),
            Layer.succeed(ImSummaryGate, { needed: () => Effect.succeed(true) }),
            Layer.succeed(ChatSummarizer, {
              summarize: Effect.fnUntraced(function* (input) {
                yield* Ref.update(inputs, (current) => [...current, input]);
                yield* Deferred.succeed(
                  input.chat.id === chat.id ? firstStarted : otherStarted,
                  undefined,
                );
                yield* Deferred.await(release);
                return { text: input.chat.id, references: [] };
              }),
            }),
          ),
        );
        const actor = yield* system.spawn("chat", LarkChatActor, contextSpawnOptions(path));
        yield* system.spawn("other", LarkChatActor, contextSpawnOptions(otherPath));
        yield* Deferred.await(firstStarted);
        yield* Deferred.await(otherStarted);
        yield* actor.tell({ _tag: "Update", chat, messages: [second] });
        yield* actor.ask((replyTo) => ({
          _tag: "Checkpoint",
          date,
          generation: "mailbox-barrier",
          change: { _tag: "Stage" },
          replyTo,
        }));
        assert.equal(base.get(date, chat.id)?.pending.length, 2);
        yield* Deferred.succeed(release, undefined);
        yield* Deferred.await(firstFinished);
        yield* Deferred.await(otherFinished);
        const completed = yield* Ref.get(inputs);
        assert.deepEqual(
          completed
            .filter((input) => input.path === path)
            .map((input) => input.messages.map((message) => message.id)),
          [[first.id], [first.id], [second.id], [second.id]],
        );
        assert.deepEqual(
          completed
            .filter((input) => input.path === otherPath)
            .map((input) => input.messages.map((message) => message.id)),
          [[otherMessage.id], [otherMessage.id]],
        );
        assert.deepEqual(registry.get(path)?.state, {
          chat,
          summary: { text: chat.id, references: [] },
        });
        assert.deepEqual(registry.get(otherPath)?.state, {
          chat: other,
          summary: { text: other.id, references: [] },
        });
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("summary screening reads the caller Clock without creating a second runtime", async () => {
  const gate = makeImSummaryGate({
    systemOne: () =>
      Clock.currentTimeMillis.pipe(
        Effect.map((now) => ({
          answers: { summarize: { type: "choice" as const, choice: now === 123 ? "yes" : "no" } },
        })),
      ),
  });
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(123);
        return yield* gate
          .needed({ path, chat, messages: [message] })
          .pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
  assert.equal(result, true);
});

test("IM private storage decodes operational checkpoints before returning them", (t) => {
  const dir = mkdtempSync(join(tmpdir(), "aster-im-schema-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const storage = makeImStorage(dir);
  storage.ingest({ chat, messages: [message] });
  const date = imDate(message.at);
  const record = storage.get(date, chat.id)!;
  writeFileSync(
    join(dir, date, "chats", `${chat.id}.json`),
    JSON.stringify({ ...record, assessment: { fingerprint: "f", needed: "false" } }),
  );
  assert.throws(() => storage.get(date, chat.id));
});
