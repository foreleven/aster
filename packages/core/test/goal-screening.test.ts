import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Exit, Logger } from "effect";
import {
  relevantGoals,
  DecisionError,
  GoalScreeningStoreError,
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

test("Goal screening sends project identity rules with overlapping dataset evidence", async () => {
  let inspected = false;
  await Effect.runPromise(
    relevantGoals(
      {
        systemOne: (request) =>
          Effect.sync(() => {
            const question = request.questions.relevance!;
            assert.equal(question.type, "score");
            assert.match(
              question.instructions,
              /same project or an explicitly evidenced dependency/,
            );
            assert.match(question.instructions, /Without that link, score at most 3/);
            assert.match(question.instructions, /P0 severity, overdue bugs/);
            assert.match(question.instructions, /Aliases must be established/);
            assert.match(question.instructions, /explicit dependency can be relevant/);
            assert.match(JSON.stringify(request.state), /DataAgent/);
            assert.match(JSON.stringify(request.state), /high-quality dataset project/);
            inspected = true;
            return { answers: { relevance: { type: "score", score: 2 } } };
          }),
      },
      {
        ...source,
        state: {
          summary:
            "The high-quality dataset project has overdue P0 labeling permission bugs and P1 agent node bugs, with a daily 18:00 standup. No dependency on DataAgent is stated.",
        },
      },
      [
        {
          slug: "data-agent",
          title: "DataAgent (iDA)",
          description: "Track DataAgent project progress.",
        },
      ],
    ),
  );
  assert.equal(inspected, true);
});

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
  assert.equal(result[0]?.score, 1);
  assert.equal(result[0]?.rationale, "Direct release risk");
  assert.equal(records.length, 2);
  assert.deepEqual(
    records.map((record) => record.admitted),
    [true, false],
  );
  assert.ok(records.every((record) => record.input.contextSummary.includes("launch")));
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

test("ten-level relevance preserves continuous scores and the normalized admission threshold", async () => {
  for (const [score, admitted] of [
    [0, false],
    [6.29, false],
    [6.3, true],
    [9, true],
  ] as const) {
    const records: GoalScreeningRecord[] = [];
    const result = await Effect.runPromise(
      relevantGoals(
        { systemOne: () => Effect.succeed({ answers: { relevance: { type: "score", score } } }) },
        source,
        [{ slug: "release", description: "Release risks" }],
        { screening: { append: (record) => Effect.sync(() => records.push(record)) } },
      ),
    );
    assert.equal(result.length, admitted ? 1 : 0);
    assert.equal(records[0]?.score, score / 9);
    assert.equal(records[0]?.threshold, 0.7);
    assert.equal(records[0]?.policyVersion, "goal-relevance-v3");
    assert.match(records[0]?.rationale ?? "", /\/9\.$/);
  }
});

test("scores outside the ten-level rubric and non-score answers fail closed", async () => {
  for (const answer of [
    ...[-1, 9.1, 10, Infinity, NaN].map((score) => ({ type: "score", score })),
    { type: "choice", score: 9 },
  ]) {
    const records: GoalScreeningRecord[] = [];
    const result = await Effect.runPromise(
      relevantGoals(
        { systemOne: () => Effect.succeed({ answers: { relevance: answer } }) },
        source,
        [{ slug: "release", description: "Release risks" }],
        { screening: { append: (record) => Effect.sync(() => records.push(record)) } },
      ),
    );
    assert.deepEqual(result, []);
    assert.equal(records[0]?.error, "invalid-score");
  }
});

test("screening audits typed failures before returning the original error", async () => {
  const failure = new DecisionError({ message: "Service unavailable" });
  const records: GoalScreeningRecord[] = [];
  const result = await Effect.runPromise(
    relevantGoals(
      { systemOne: () => Effect.fail(failure) },
      source,
      [{ slug: "release", description: "Release risks" }],
      { screening: { append: (record) => Effect.sync(() => records.push(record)) } },
    ).pipe(Effect.result, Effect.provide(Logger.layer([]))),
  );
  assert.equal(result._tag, "Failure");
  if (result._tag === "Failure") assert.equal(result.failure, failure);
  assert.equal(records.length, 1);
  assert.equal(records[0]?.error, failure.message);
  assert.equal(records[0]?.admitted, false);
});

test("screening audit storage failures remain failures", async () => {
  const failure = new GoalScreeningStoreError({ message: "Disk full" });
  const result = await Effect.runPromise(
    relevantGoals(
      { systemOne: () => Effect.succeed({ answers: { relevance: { type: "score", score: 9 } } }) },
      source,
      [{ slug: "release", description: "Release risks" }],
      { screening: { append: () => Effect.fail(failure) } },
    ).pipe(Effect.result),
  );
  assert.equal(result._tag, "Failure");
  if (result._tag === "Failure") assert.equal(result.failure, failure);
});

test("screening does not turn defects or interruption into audit decisions", async () => {
  for (const failure of [Effect.die("Unexpected defect"), Effect.interrupt]) {
    const records: GoalScreeningRecord[] = [];
    const result = await Effect.runPromiseExit(
      relevantGoals(
        { systemOne: () => failure },
        source,
        [{ slug: "release", description: "Release risks" }],
        { screening: { append: (record) => Effect.sync(() => records.push(record)) } },
      ),
    );
    assert.ok(Exit.hasDies(result) || Exit.hasInterrupts(result));
    assert.deepEqual(records, []);
  }
});
