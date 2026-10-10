import { makeGoalIntent } from "../src/goals/screening/intent.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect } from "effect";
import {
  DecisionError,
  matchGoal,
  normalizeScore,
  relevanceQuestion,
  goalTitleText,
  goalSummaryText,
  type ContextText,
  type SystemOneClient,
} from "../src/index.js";
const source: ContextText = {
  path: "/chat",
  revision: 1,
  description: "Chat",
  text: "Release is blocked",
};
const candidate = {
  definition: { slug: "release", description: "Release readiness" },
  title: "Release",
  summary: "Current progress",
};
const client = (value: number): SystemOneClient => ({
  systemOne: () => Effect.succeed({ answers: { relevance: { type: "score", score: value } } }),
});
test("screening keeps identity rules and applies the domain output language", async () => {
  await Effect.runPromise(
    matchGoal(
      {
        systemOne: (request) =>
          Effect.sync(() => {
            assert.match(
              request.questions.relevance!.instructions,
              /same project or an explicitly evidenced dependency/,
            );
            assert.match(
              request.questions.relevance!.instructions,
              /Without that link, score at most 3/,
            );
            assert.match(request.questions.relevance!.instructions, /en-US/);
            assert.equal(
              request.questions.relevance!.type === "score"
                ? request.questions.relevance!.criteria.length
                : 0,
              10,
            );
            return { answers: { relevance: { type: "score", score: 9 } } };
          }),
      },
      source,
      candidate,
      "en-US",
    ),
  );
});
test("independent Goal matches use a continuous score and frozen intent evidence", async () => {
  const matched = await Effect.runPromise(matchGoal(client(9), source, candidate));
  assert.equal(matched._tag, "Matched");
  if (matched._tag === "Matched") {
    const intent = makeGoalIntent(source, matched.relevance, "2026-10-10T00:00:00Z");
    assert.equal(intent.relevance.score, 1);
    assert.equal(intent.relevance.threshold, 0.7);
    assert.equal(intent.content.summary, source.text);
    assert.equal(intent.source.contextPath, source.path);
  }
  assert.equal(
    (await Effect.runPromise(matchGoal(client(6), source, candidate)))._tag,
    "NotMatched",
  );
});
test("screening failures and invalid scores remain failures, never negative admission", async () => {
  const failure = new DecisionError({ message: "Unavailable" });
  assert.equal(
    await Effect.runPromise(
      matchGoal({ systemOne: () => Effect.fail(failure) }, source, candidate).pipe(Effect.flip),
    ),
    failure,
  );
  for (const value of [-1, 10, NaN, Infinity])
    assert.equal(
      (await Effect.runPromise(matchGoal(client(value), source, candidate).pipe(Effect.result)))
        ._tag,
      "Failure",
    );
});
test("source identity excludes revision but includes public description and text", async () => {
  const hashes: string[] = [];
  for (const record of [
    source,
    { ...source, revision: 2 },
    { ...source, text: "Changed" },
    { ...source, description: "Other" },
  ]) {
    const result = await Effect.runPromise(matchGoal(client(9), record, candidate));
    if (result._tag === "Matched") hashes.push(result.relevance.summaryFingerprint);
  }
  assert.equal(hashes[0], hashes[1]);
  assert.notEqual(hashes[0], hashes[2]);
  assert.notEqual(hashes[0], hashes[3]);
});
test("empty source evidence makes no model request and helpers retain Goal identity", async () => {
  assert.equal(
    (
      await Effect.runPromise(
        matchGoal(
          { systemOne: () => Effect.die("Unexpected model") },
          { ...source, text: " " },
          candidate,
        ),
      )
    )._tag,
    "NotMatched",
  );
  assert.equal(goalTitleText(candidate.definition, undefined), candidate.definition.description);
  assert.equal(goalSummaryText(undefined), "");
  assert.equal(normalizeScore(9), 1);
  assert.equal(
    relevanceQuestion({
      goalTitle: candidate.title,
      goalDescription: candidate.definition.description,
      goalSummary: candidate.summary,
    }).type,
    "score",
  );
});
