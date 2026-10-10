import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Schema } from "effect";
import { ChildProcessSpawner } from "effect/process";
import { makeCodexRpc } from "../src/executors/codex/rpc.js";
import { Frame, ThreadRead } from "../src/executors/codex/protocol.js";
test("native RPC frames and thread data reject malformed identities before projection", async () => {
  for (const value of [{ id: {} }, { method: 42 }, { params: "invalid" }])
    assert.equal(
      (await Effect.runPromise(Schema.decodeUnknownEffect(Frame)(value).pipe(Effect.result)))._tag,
      "Failure",
    );
  assert.equal(
    (
      await Effect.runPromise(
        Schema.decodeUnknownEffect(ThreadRead)({
          thread: { turns: [{ id: "run", status: "completed", items: "invalid" }] },
        }).pipe(Effect.result),
      )
    )._tag,
    "Failure",
  );
});

test("retired RPC capability rejects calls without restarting app-server", async () => {
  const rpc = await Effect.runPromise(
    Effect.scoped(
      makeCodexRpc({}).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("Retired transport spawned a process")),
        ),
      ),
    ),
  );
  const result = await Effect.runPromise(rpc.call("thread/start", {}).pipe(Effect.result));
  assert.equal(result._tag, "Failure");
  if (result._tag === "Failure") assert.match(result.failure.message, /closed/);
});
