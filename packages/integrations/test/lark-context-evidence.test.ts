import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Schema } from "effect";
import { GoalScreeningSnapshot, makeGoalIntent, matchGoal } from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import { chatView } from "../src/lark/public-views.js";

test("chat views carry current identity and public evidence into Goal screening", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      const path = "/lark/im/chats/project";
      yield* registry.register(path, {
        state: Schema.ObjectKeyword,
        message: Schema.Unknown,
        view: chatView,
        changes: "durable-state" as const,
      });
      const initial = yield* registry.commit(
        {
          path,
          description: "Old chat name",
          state: {
            chat: {
              id: "project",
              name: "Release team",
              mode: "group",
              description: "Owns Project A",
            },
            summary: {
              text: "Launch blocked",
              references: [{ id: "one", url: "https://example.com/one" }],
            },
            token: "PRIVATE_SENTINEL",
          },
          messages: [
            {
              id: "two",
              at: "2026-10-09T00:00:00Z",
              content: "Dependency confirmed",
              sender: { name: "Alex", token: "PRIVATE_SENTINEL" },
              url: "https://example.com/two",
              deleted: false,
            },
          ],
        },
        { expectedRevision: 0 },
      );
      const renamed = yield* registry.commit(
        {
          ...initial,
          state: {
            ...initial.state,
            chat: {
              id: "project",
              name: "New release team",
              mode: "group",
              description: "Owns Project A",
            },
          },
        },
        { expectedRevision: initial.revision },
      );
      const evidence = registry.backend.journal()[1]!.record;
      assert.equal(
        evidence.description,
        "Work Lark conversation: New release team\nOwns Project A",
      );
      assert.deepEqual(registry.reader.get(path), evidence);
      assert.equal(registry.get(path)!.description, "Old chat name");
      assert.equal(renamed.revision, evidence.revision);
      assert.equal(JSON.stringify(evidence).includes("PRIVATE_SENTINEL"), false);
      let requests = 0;
      const result = yield* matchGoal(
        {
          systemOne: (request) =>
            Effect.sync(() => {
              requests++;
              const input = Schema.decodeUnknownSync(GoalScreeningSnapshot)(request.state);
              assert.deepEqual(input.context, evidence);
              assert.equal(JSON.stringify(request).includes("PRIVATE_SENTINEL"), false);
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
      if (result._tag === "Matched") {
        assert.equal(
          makeGoalIntent(evidence, result.relevance, "2026-10-09T00:00:00Z").source.name,
          evidence.description,
        );
      }
      assert.equal(chatView.project({ ...initial, state: { chat: { id: "project" } } }), undefined);
    }),
  );
});
