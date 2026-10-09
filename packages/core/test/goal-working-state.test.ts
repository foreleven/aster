import { DurableContext } from "@aster/core";
import { Effect, Schema } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ContextRegistry } from "../src/context/registry.js";
import { GoalSnapshot } from "../src/goals/state/snapshot.js";
import { makeGoalStore } from "../src/goals/state/store.js";
import { goalTimeline } from "../src/goals/view.js";

import { makeContextRegistry } from "../src/testing/context.js";
import { testConversations } from "./conversation-fixtures.js";

test("Goal keeps message references while public conversation reads Pi history", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const conversations = testConversations();
        const path = "/goals/demo";
        yield* registry.register(path, { state: GoalSnapshot, message: Schema.Never });
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
              tasks: [],
            },
          },
          { expectedRevision: 0 },
        );
        for (let index = 0; index < 105; index++)
          yield* conversations.append(path, `input-${index}`, "goal.input", {
            payload: { _tag: "UserInput", text: `Input ${index}` },
          });
        yield* conversations.append(path, "evidence", "goal.input", {
          payload: { _tag: "GoalStarted" },
        });
        yield* conversations.append(path, "reply", "goal.reply", {
          inputId: "input-104",
          text: "Here is the result",
        });
        const store = yield* makeGoalStore(
          path,
          Schema.decodeUnknownSync(GoalSnapshot)(registry.get(path)!.state),
        ).pipe(
          Effect.provideService(ContextRegistry, registry),
          Effect.provideService(DurableContext, registry.backend),
        );
        yield* store.save({ summary: "Current understanding" });
        assert.deepEqual(registry.get(path)!.messages, []);
        const page = yield* goalTimeline(registry, conversations, "demo", { limit: 100 });
        assert.equal(page.total, 106);
        assert.equal(page.messages.length, 100);
        assert.equal(page.messages.at(-1)?.text, "Here is the result");
        assert.ok(page.nextBefore !== null);
        const previous = yield* goalTimeline(registry, conversations, "demo", {
          before: page.nextBefore!,
        });
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
      yield* registry.register(path, { state: GoalSnapshot, message: Schema.Never });
      const definition = { slug: "errors", description: "Observe errors" };
      const first = {
        inputId: "first",
        remainingAgentTurns: 4,
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
              tasks: [],
              inputs:
                status === "failed"
                  ? [first]
                  : [
                      first,
                      {
                        ...first,
                        inputId: "second",
                        status,
                        error: "Old uncertain outcome",
                      },
                    ],
            },
          },
          { expectedRevision: current?.revision ?? 0 },
        );
        const view = registry.reader.get(path)!;
        const projected = view.state as { lastError?: string };
        assert.equal(projected.lastError, status === "completed" ? undefined : "Model failed");
        assert.equal("lastError" in registry.get(path)!.state, false);
        assert.equal("historyCount" in view.state, false);
        assert.equal("receipts" in view.state, false);
      }
    }),
  );
});
