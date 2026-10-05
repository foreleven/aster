import assert from "node:assert/strict";
import { test } from "node:test";
import { Cause, Data, Effect, Exit } from "effect";

import { isolateContextChange } from "../src/runtime/context-consumers.js";

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
