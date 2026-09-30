import assert from "node:assert/strict";
import { test } from "node:test";
import { Result, Schema } from "effect";
import {
  GoalToolRequest,
  decideTaskOperation,
  type GoalTask,
  type TaskDecision,
} from "../src/index.js";

const at = "2026-09-30T00:00:00Z";
const task: GoalTask = {
  id: "review",
  title: "Review",
  instructions: "Read evidence",
  status: "open",
  revision: 1,
  evidence: [],
  createdAt: at,
  updatedAt: at,
};
const success = (result: ReturnType<typeof decideTaskOperation>): TaskDecision => {
  assert.ok(Result.isSuccess(result));
  return result.success;
};

test("Task creation deduplicates IDs and titles; stale writes leave input unchanged", () => {
  const tasks = [task];
  const before = structuredClone(tasks);
  for (const id of ["review", "another"])
    assert.deepEqual(
      success(
        decideTaskOperation(
          tasks,
          {
            operation: "task_create",
            id,
            title: " REVIEW ",
            instructions: "New instructions",
          },
          { at },
        ),
      ),
      { _tag: "Read", value: task },
    );
  const stale = decideTaskOperation(
    tasks,
    {
      operation: "task_update",
      id: task.id,
      revision: 0,
      instructions: "Changed",
    },
    { at },
  );
  assert.ok(Result.isFailure(stale));
  assert.deepEqual(tasks, before);
});

test("a persisted proposal reserves execution before its Run is registered", () => {
  const execution = { runPath: "/goals/project/runs/original", status: "preparing", revision: 1 };
  const decision = success(
    decideTaskOperation(
      [{ ...task, execution }],
      {
        operation: "task_execute",
        id: task.id,
        revision: 1,
      },
      { at, runPath: "/goals/project/runs/duplicate" },
    ),
  );
  assert.deepEqual(decision, { _tag: "Read", value: { ...execution, reused: true } });
});

test("revising a pending proposal cancels it and allows a replacement at the new revision", () => {
  const execution = {
    runPath: "/goals/project/runs/old",
    status: "awaiting-confirmation",
    revision: 1,
  };
  const tasks = [{ ...task, execution }];
  const before = structuredClone(tasks);
  const changed = success(
    decideTaskOperation(
      tasks,
      {
        operation: "task_update",
        id: task.id,
        revision: 1,
        instructions: "Changed scope",
      },
      { at },
    ),
  );
  assert.equal(changed._tag, "Save");
  if (changed._tag !== "Save") return;
  assert.equal(changed.cancelRun, execution.runPath);
  assert.equal(changed.value.revision, 2);
  assert.equal(changed.value.execution?.revision, 1);
  const next = success(
    decideTaskOperation(
      changed.tasks,
      {
        operation: "task_execute",
        id: task.id,
        revision: 2,
      },
      { at, runPath: "/goals/project/runs/new" },
    ),
  );
  assert.equal(next._tag, "Execute");
  if (next._tag === "Execute") {
    assert.equal(next.task.instructions, "Changed scope");
    assert.equal(next.tasks[0]!.execution?.revision, 2);
  }
  assert.deepEqual(tasks, before);
});

test("revisions and deletion preserve started execution and existing results", () => {
  for (const status of ["submitting", "running", "waiting_input", "uncertain"]) {
    const execution = { runPath: "/goals/project/runs/started", status, revision: 1 };
    const changed = success(
      decideTaskOperation(
        [{ ...task, execution, result: "Prior evidence" }],
        {
          operation: "task_update",
          id: task.id,
          revision: 1,
          instructions: "Next scope",
        },
        { at },
      ),
    );
    assert.equal(changed._tag, "Save");
    if (changed._tag !== "Save") continue;
    assert.equal(changed.cancelRun, undefined);
    assert.equal(changed.value.result, "Prior evidence");
    assert.deepEqual(
      success(
        decideTaskOperation(
          changed.tasks,
          {
            operation: "task_execute",
            id: task.id,
            revision: 2,
          },
          { at, runPath: "/goals/project/runs/duplicate" },
        ),
      ),
      {
        _tag: "Read",
        value: { ...execution, reused: true },
      },
    );
    const deleted = success(
      decideTaskOperation(
        changed.tasks,
        {
          operation: "task_delete",
          id: task.id,
          revision: 2,
        },
        { at },
      ),
    );
    assert.equal(deleted._tag, "Save");
    if (deleted._tag === "Save") {
      assert.equal(deleted.cancelRun, undefined);
      assert.equal(deleted.value.status, "deleted");
      assert.deepEqual(deleted.value.execution, execution);
    }
  }
});

test("live Run status overrides stale proposal status; legacy proposals are conservatively reused", () => {
  const execution = { runPath: "/goals/project/runs/old", status: "preparing" };
  const request = { operation: "task_execute", id: task.id, revision: 1 } as const;
  assert.equal(
    success(decideTaskOperation([{ ...task, execution }], request, { at }))._tag,
    "Read",
  );
  assert.equal(
    success(
      decideTaskOperation([{ ...task, execution }], request, {
        at,
        runPath: "/goals/project/runs/new",
        execution: { status: "cancelled", revision: 1 },
      }),
    )._tag,
    "Execute",
  );
});

test("Goal tool decoding requires each operation's own fields", () => {
  const decode = Schema.decodeUnknownResult(GoalToolRequest);
  for (const input of [
    { operation: "task_create", id: "review", title: "Review" },
    { operation: "task_update", id: "review", title: "Updated" },
    { operation: "task_execute", id: "review" },
    { operation: "signal_update", id: "watch", definition: {} },
    { operation: "signal_create", id: "watch", definition: { schedule: "tomorrow" } },
  ])
    assert.ok(Result.isFailure(decode(input)));
  assert.ok(Result.isSuccess(decode({ operation: "task_list" })));
  assert.ok(
    Result.isSuccess(
      decode({
        operation: "signal_update",
        id: "watch",
        revision: 1,
        definition: { schedule: null },
      }),
    ),
  );
});
