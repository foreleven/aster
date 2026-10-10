import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect } from "effect";
import { GoalScreeningSnapshot, makeDurableContext, makeGoalIntent, matchGoal } from "@aster/core";
import { Schema } from "effect";
import { ChatState } from "../../src/lark/im/chat/state.js";
import { ChatMessage } from "../../src/lark/im/model.js";

test("chat serialization freezes summary and identity without receipts or pending bodies", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* makeDurableContext({
          load: Effect.succeed([]),
          save: () => Effect.void,
        });
        const owner = yield* durable.open({
          path: "/lark/im/chats/project",
          state: ChatState,
          message: ChatMessage,
          keyOf: (message) => message.id,
          serialize: ({ chat, summary }) => JSON.stringify({ chat, summary }),
          events: "state",
          initial: {
            description: "Release team",
            state: {
              chat: {
                id: "project",
                name: "Release team",
                mode: "group",
                description: "Owns Project A",
              },
              flushThrough: "2026-10-08",
            },
            messages: [
              {
                id: "two",
                at: "2026-10-09T00:00:00Z",
                content: "PRIVATE_PENDING_BODY",
                sender: {},
                url: "",
                deleted: false,
              },
            ],
          },
        });
        yield* owner.state.update((state) => ({
          ...state,
          summary: { text: "Launch blocked", references: [] },
        }));
        const evidence = durable.journal()[0]!.record;
        assert.equal(evidence.description, "Release team");
        assert.equal(evidence.text.includes("Launch blocked"), true);
        assert.equal(JSON.stringify(evidence).includes("flushThrough"), false);
        assert.equal(JSON.stringify(evidence).includes("PRIVATE_PENDING_BODY"), false);
        let requests = 0;
        const result = yield* matchGoal(
          {
            systemOne: (request) =>
              Effect.sync(() => {
                requests++;
                const input = Schema.decodeUnknownSync(GoalScreeningSnapshot)(request.state);
                assert.deepEqual(input.context, evidence);
                return { answers: { relevance: { type: "score", score: 9 } } };
              }),
          },
          evidence,
          {
            definition: { slug: "release", description: "Deliver Project A" },
            title: "Release",
            summary: "Preparing launch",
          },
        );
        assert.equal(requests, 1);
        assert.equal(result._tag, "Matched");
        if (result._tag === "Matched")
          assert.equal(
            makeGoalIntent(evidence, result.relevance, "2026-10-09T00:00:00Z").source.name,
            evidence.description,
          );
      }),
    ),
  );
});
