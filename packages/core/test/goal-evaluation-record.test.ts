import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Exit } from "effect";
import { GoalActor, makeContextRegistry, type ContextRecord } from "../src/index.js";

test("Goal recovery rejects duplicate, unbound result and invalid retry journal entries before writing", async () => {
  const completed = {
    evaluationId: "one",
    reason: "Committed input",
    historyThrough: 1,
    startedAt: "2026-10-02T00:00:00Z",
    status: "completed",
    resultId: "one",
    result: { progress: "Reviewed", completed: false, evidence: [], signals: [] },
    appliedAt: "2026-10-02T00:01:00Z",
  };
  for (const evaluations of [
    [completed, completed],
    [{ ...completed, resultId: "another-evaluation" }],
    [{ ...completed, retryOf: "missing" }],
    [completed, { ...completed, evaluationId: "two", resultId: "two", retryOf: "one" }],
  ]) {
    let writes = 0;
    const record: ContextRecord = {
      path: "/goals/project",
      revision: 1,
      description: "Project",
      messages: [],
      state: {
        slug: "project",
        status: "active",
        description: "Project",
        summary: "Reviewed",
        progress: "Reviewed",
        tasks: [],
        historyThrough: 0,
        agentThrough: 1,
        historyCount: 1,
        pendingEvaluation: false,
        receivedEvents: [],
        evaluations,
      },
    };
    const before = structuredClone(record);
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({
          loadAll: () => [record],
          save: () => {
            writes++;
          },
        });
        return yield* Effect.exit(registry.register(record.path, GoalActor.context));
      }),
    );
    assert.ok(Exit.isFailure(exit));
    assert.equal(writes, 0);
    assert.deepEqual(record, before);
  }
});
