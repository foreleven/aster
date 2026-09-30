import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { Clock, Deferred, Effect, Fiber, Layer, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ActorSystem } from "@aster/actor";
import { ContextRegistry, contextSpawnOptions, makeContextRegistry } from "@aster/core";
import {
  ChatSummarizer,
  ImAgentQueue,
  ImStorage,
  ImSummaryGate,
  LarkChatActor,
  makeImStorage,
  makeImSummaryGate,
  imDate,
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
        const reply = yield* actor.ask((replyTo) => ({
          _tag: "Checkpoint",
          date,
          generation: "retired",
          change: { _tag: "Daily", summary: { text: "Stale", references: [] } },
          replyTo,
        }));
        assert.equal(reply, undefined);
        assert.deepEqual(storage.get(date, chat.id), before);
        yield* system.stop(actor);
        assert.equal(released, true);
        assert.equal(storage.get(date, chat.id)!.stage!.daily, undefined);
        assert.equal(storage.get(date, chat.id)!.pending.length, 1);
      }),
    ),
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
