import { testConversations } from "./conversation-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect } from "effect";
import { GoalActor, makeApplicationApi } from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { goalWorkingState } from "../src/goals/working-state.js";

test("Goal keeps message references while public conversation reads Pi history", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const conversations = testConversations();
        const path = "/goals/demo";
        yield* registry.register(path, GoalActor.context);
        yield* registry.commit(
          {
            path,
            description: "Goal",
            messages: [],
            state: {
              definition: { slug: "demo", description: "Goal" },
              status: "active",
              summary: "",
              inputs: [],
              receipts: [],
            },
          },
          { expectedRevision: 0 },
        );
        for (let index = 0; index < 105; index++)
          yield* conversations.append(path, `input-${index}`, "goal.input", {
            payload: { _tag: "UserInput", text: `Input ${index}` },
          });
        yield* conversations.append(path, "evidence", "goal.input", {
          payload: { _tag: "GoalStarted", pursuit: "internal" },
        });
        yield* conversations.append(path, "reply", "goal.reply", {
          inputId: "input-104",
          text: "Here is the result",
        });
        yield* goalWorkingState(registry, () => path).save({ summary: "Current understanding" });
        assert.deepEqual(registry.get(path)!.messages, []);
        const api = makeApplicationApi({ registry, conversations, inspect: Effect.succeed(null) });
        const page = yield* api.goals.timeline("demo", { limit: 100 });
        assert.equal(page.total, 106);
        assert.equal(page.messages.length, 100);
        assert.equal(page.messages.at(-1)?.text, "Here is the result");
        assert.ok(page.nextBefore !== null);
        const previous = yield* api.goals.timeline("demo", { before: page.nextBefore! });
        assert.equal(previous.messages.length, 6);
        assert.ok(
          page.messages.every((message) => message.role === "user" || message.role === "assistant"),
        );
      }),
    ),
  );
});

test("Goal public error reflects the latest settled input without storing an error cache", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      const path = "/goals/errors";
      yield* registry.register(path, GoalActor.context);
      const api = makeApplicationApi({
        registry,
        conversations: testConversations(),
        inspect: Effect.succeed(null),
      });
      const definition = { slug: "errors", description: "Observe errors" };
      const first = {
        inputId: "first",
        goalSlug: "errors",
        ordinal: 1,
        receivedAt: "2026-10-05T00:00:00Z",
        kind: "UserInput",
        entryId: 1,
        status: "failed",
        error: "Model failed",
      };
      for (const status of ["failed", "running", "completed"]) {
        const current = registry.get(path);
        yield* registry.commit(
          {
            path,
            description: definition.description,
            messages: [],
            state: {
              definition,
              status: "active",
              summary: "",
              receipts: [],
              inputs:
                status === "failed"
                  ? [first]
                  : [
                      first,
                      {
                        ...first,
                        inputId: "second",
                        ordinal: 2,
                        status,
                        error: "Old uncertain outcome",
                      },
                    ],
            },
          },
          { expectedRevision: current?.revision ?? 0 },
        );
        const view = yield* api.context(path);
        const projected = view.state as { lastError?: string };
        assert.equal(projected.lastError, status === "completed" ? undefined : "Model failed");
        assert.equal("lastError" in registry.get(path)!.state, false);
        assert.equal("historyCount" in view.state, false);
        assert.equal("receipts" in view.state, false);
      }
    }),
  );
});
