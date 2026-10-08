import { conversationDriver } from "../src/harness/conversations.js";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Exit, Option, Scope } from "effect";
import { TestClock } from "effect/testing";
import { AgentConversations } from "../src/harness/index.js";

test("retired conversation writers reject access after their scope closes", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const scope = yield* Scope.fork(yield* Scope.Scope);
        const messages = yield* AgentConversations.makeMemory().pipe(
          Effect.provideService(Scope.Scope, scope),
        );
        yield* messages.append("/goals/a", "one", "goal.input", { text: "Hello" });
        yield* Scope.close(scope, Exit.void);
        const error = yield* messages.read("/goals/a").pipe(Effect.flip);
        assert.equal(error.kind, "unavailable");
      }),
    ),
  );
});

test("Pi messages deduplicate exact inputs, reject changed identities and isolate owners", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const messages = yield* AgentConversations;
        assert.equal("driver" in messages, false);
        const first = yield* messages.append("/goals/a", "one", "goal.input", { text: "Hello" });
        const retry = yield* messages.append("/goals/a", "one", "goal.input", { text: "Hello" });
        assert.deepEqual(retry, first);
        const conflict = yield* messages
          .append("/goals/a", "one", "goal.input", { text: "Different" })
          .pipe(Effect.result);
        assert.equal(conflict._tag, "Failure");
        if (conflict._tag === "Failure") assert.equal(conflict.failure.kind, "conflict");
        yield* messages.append("/tasks/b", "one", "task.input", { text: "Other owner" });
        assert.deepEqual(
          (yield* messages.read("/goals/a")).map((entry) => entry.data),
          [{ text: "Hello" }],
        );
        assert.deepEqual((yield* messages.get("/tasks/b", 0).pipe(Effect.result))._tag, "Failure");
      }).pipe(Effect.provide(AgentConversations.memory)),
    ),
  );
});

test("Pi entry lookup stays within its owner and does not decode unrelated history", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const messages = yield* AgentConversations.makeMemory();
        const first = yield* messages.append("/goals/a", "one", "goal.input", { text: "Hello" });
        const { harness } = yield* messages[conversationDriver]("/goals/a");
        const hidden = yield* Effect.promise(async () => {
          const root = await harness.root(BACKGROUND_CONTEXT);
          return harness.commit(async (tx) => {
            const other = await tx.createConversation({ ownership: { kind: "ownerless" } });
            const foreign = await tx.appendEntry(other.id, {
              kind: "app.aster.message",
              data: { requestId: "foreign", kind: "goal.input", data: {}, at: first.at },
            });
            const native = await tx.appendEntry(root.id, { kind: "pi.test", data: {} });
            await tx.appendEntry(root.id, { kind: "app.aster.message", data: { malformed: true } });
            return [foreign.id, native.id];
          }, BACKGROUND_CONTEXT);
        });
        assert.deepEqual(yield* messages.get("/goals/a", first.id), first);
        assert.deepEqual(yield* messages.find("/goals/a", "one"), Option.some(first));
        assert.deepEqual(yield* messages.find("/goals/a", "missing"), Option.none());
        assert.deepEqual(yield* messages.find("/tasks/other", "one"), Option.none());
        assert.deepEqual(
          yield* messages.append("/goals/a", "one", "goal.input", { text: "Hello" }),
          first,
        );
        for (const id of hidden) {
          const error = yield* messages.get("/goals/a", id).pipe(Effect.flip);
          assert.equal(error.kind, "not-found");
        }
      }),
    ),
  );
});

test("Pi message commits survive reopen, concurrent admission and changed retry payloads", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "aster-conversations-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const open = <A, E>(use: (messages: AgentConversations["Service"]) => Effect.Effect<A, E>) =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          return yield* use(yield* AgentConversations.make({ root }));
        }),
      ),
    );
  const entries = await open((messages) =>
    Effect.all(
      Array.from({ length: 80 }, (_, index) =>
        messages.append("/goals/a", `input-${index}`, "goal.input", { text: `Evidence ${index}` }),
      ),
      { concurrency: "unbounded" },
    ),
  );
  await open((messages) =>
    Effect.gen(function* () {
      assert.equal((yield* messages.read("/goals/a")).length, 80);
      assert.deepEqual(yield* messages.find("/goals/a", "input-0"), Option.some(entries[0]));
      assert.deepEqual(
        yield* messages.append("/goals/a", "input-0", "goal.input", { text: "Evidence 0" }),
        entries[0],
      );
      assert.equal(
        (yield* messages
          .append("/goals/a", "input-0", "goal.input", { text: "Changed" })
          .pipe(Effect.result))._tag,
        "Failure",
      );
      assert.deepEqual(yield* messages.read("/tasks/a"), []);
    }),
  );
});

test("message timestamps use the caller Clock and storage failures remain unavailable", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const messages = yield* AgentConversations.makeMemory();
        const at = Date.parse("2026-10-05T01:00:00Z");
        yield* TestClock.setTime(at);
        const first = yield* messages.append("/goals/clock", "one", "goal.input", {
          text: "Hello",
        });
        assert.equal(first.at, new Date(at).toISOString());
        yield* TestClock.setTime(at + 10000);
        assert.deepEqual(
          yield* messages.append("/goals/clock", "one", "goal.input", { text: "Hello" }),
          first,
        );
        const broken = yield* AgentConversations.make({
          openStorage: async () => {
            throw new Error("Disk offline");
          },
        });
        const failure = yield* broken
          .append("/tasks/offline", "one", "task.input", { text: "Hello" })
          .pipe(Effect.flip);
        assert.equal(failure.kind, "unavailable");
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );
});
