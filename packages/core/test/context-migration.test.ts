import assert from "node:assert/strict";
import { test } from "node:test";
import { Cause, Data, Effect, Exit, Schema } from "effect";
import { makeContextRegistry } from "../src/testing/context.js";
import { defineContext } from "../src/context/definition.js";
import { contextView } from "../src/context/view.js";
import { ReactionState } from "../src/reactions/state.js";
import { isolateContextChange } from "../src/runtime/context-consumers.js";

test("legacy reaction work decodes into phase-specific work without changing identities or frozen evidence", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      yield* registry.register(
        "/source",
        defineContext({
          state: Schema.Struct({ value: Schema.Number }),
          message: Schema.String,
          changes: "durable-state",
          view: contextView({ state: Schema.Struct({ value: Schema.Number }) }),
        }),
      );
      const snapshot = yield* registry.commit(
        { path: "/source", description: "Source", state: { value: 1 }, messages: [] },
        { expectedRevision: 0 },
      );
      assert.equal("reactionEvents" in snapshot, false);
      const event = registry.backend.exportRecords()[0]!.reactionEvents![0]!;
      const target = {
        path: "/signals/review",
        revision: 7,
        description: "Review",
        state: { active: true },
        messages: [],
      };
      for (const status of ["pending", "planning", "failed", "completed"] as const) {
        const old = {
          work: [
            {
              event,
              snapshot:
                status === "pending" ? {} : { [event.source]: event.record, [target.path]: target },
              goals: [{ slug: "review", description: "Review work" }],
              admittedAt: event.createdAt,
              status,
              attempts: status === "pending" ? 0 : 2,
              error: "Previous failure",
              screenings: [],
              deliveries: [],
            },
          ],
        };
        const migrated = Schema.decodeUnknownSync(ReactionState)(old).work[0]!;
        assert.equal(migrated.event.id, event.requestId);
        assert.equal(migrated.event.record.revision, event.revision);
        if ("input" in migrated) {
          assert.deepEqual(migrated.input.evidence, { [target.path]: target });
          assert.equal(migrated.input.screeningAt, event.createdAt);
          assert.deepEqual(migrated.input.goals, old.work[0]!.goals);
        } else assert.equal("input" in migrated, false);
        assert.equal("snapshot" in migrated, false);
        assert.throws(() =>
          Schema.decodeUnknownSync(ReactionState)({
            work: [{ ...old.work[0], event: { ...event, causationId: "forged" } }],
          }),
        );
      }
    }),
  );
});

test("Context maintenance isolates typed item failures but preserves defects and interruption", async () => {
  const change = {
    record: { path: "/source", revision: 1, description: "Source", state: {}, messages: [] },
  };
  class ItemFailure extends Data.TaggedError("ItemFailure")<object> {}
  await Effect.runPromise(isolateContextChange(() => Effect.fail(new ItemFailure()))(change));
  const defect = new Error("Invariant broken");
  const exit = await Effect.runPromise(
    Effect.exit(isolateContextChange(() => Effect.die(defect))(change)),
  );
  assert.ok(Exit.isFailure(exit));
  assert.equal(Cause.squash(exit.cause), defect);
  const interrupted = await Effect.runPromise(
    Effect.exit(isolateContextChange(() => Effect.interrupt)(change)),
  );
  assert.ok(Exit.isFailure(interrupted));
  assert.ok(Cause.hasInterrupts(interrupted.cause));
});
