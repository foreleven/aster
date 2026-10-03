import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect } from "effect";
import {
  relevantGoals,
  type GoalScreeningRecord,
  type SystemOneClient,
  type ContextRecord,
} from "../src/index.js";

const source: ContextRecord = {
  path: "/lark/im/chats/project",
  description: "Project chat",
  state: { summary: "The launch is blocked by a backend API delay." },
  messages: [],
};

test("Goal screening scores each Goal independently and records admitted evidence", async () => {
  const requests: string[] = [];
  const records: GoalScreeningRecord[] = [];
  const client: SystemOneClient = {
    systemOne: (request) =>
      Effect.sync(() => {
        const input = JSON.stringify(request.state);
        requests.push(input);
        const scoreValue = input.includes("release") ? 9 : 2;
        return {
          answers: {
            relevance: {
              type: "score",
              score: scoreValue,
              legend: {
                [String(scoreValue)]: scoreValue === 9 ? "Direct release risk" : "Weak overlap",
              },
            },
          },
        };
      }),
  };
  const result = await Effect.runPromise(
    relevantGoals(
      client,
      source,
      [
        { slug: "release", title: "Release readiness", description: "Release risks" },
        { slug: "hiring", title: "Hiring", description: "Hiring plans" },
      ],
      {
        goalRecords: {
          "/goals/release": {
            path: "/goals/release",
            description: "Release Goal",
            state: { title: "Release readiness", summary: "Prepare this release." },
            messages: [],
          },
          "/goals/hiring": {
            path: "/goals/hiring",
            description: "Hiring Goal",
            state: { title: "Hiring", summary: "Build the team." },
            messages: [],
          },
        },
        screening: { append: (record) => Effect.sync(() => records.push(record)) },
      },
    ),
  );
  assert.equal(requests.length, 2);
  assert.deepEqual(
    result.map((goal) => goal.slug),
    ["release"],
  );
  assert.equal(result[0]?.score, 0.9);
  assert.equal(result[0]?.rationale, "Direct release risk");
  assert.equal(records.length, 2);
  assert.deepEqual(
    records.map((record) => record.admitted),
    [true, false],
  );
  assert.ok(records.every((record) => record.input.chatSummary.includes("launch")));
});

test("invalid Goal screening scores fail closed", async () => {
  const client: SystemOneClient = {
    systemOne: () =>
      Effect.succeed({ answers: { relevance: { type: "score", score: Number.NaN } } }),
  };
  const records: GoalScreeningRecord[] = [];
  const result = await Effect.runPromise(
    relevantGoals(client, source, [{ slug: "release", description: "Release risks" }], {
      screening: { append: (record) => Effect.sync(() => records.push(record)) },
    }),
  );
  assert.deepEqual(result, []);
  assert.equal(records[0]?.admitted, false);
  assert.equal(records[0]?.error, "invalid-score");
});
