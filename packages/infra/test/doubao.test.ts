import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect } from "effect";
import { taskPrompt } from "@aster/core";
import { makeDoubaoExecutor } from "../src/executors/doubao/executor.js";
import { doubaoStatus } from "../src/executors/doubao/protocol.js";
import { DoubaoWaitTimeout } from "../src/executors/doubao/cli.js";
import { testPlatform } from "./executor-fixtures.js";
const settings = { prompt: "Custom policy", endpoint: "http://captured:9226", taskRoot: "/unused" };
test("Doubao submits exactly the prepared instructions and keeps original run control identity", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "aster-doubao-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const calls: string[][] = [];
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const task = { instructions: "Custom policy\nReviewed task", input: [] };
        const agent = yield* makeDoubaoExecutor(
          {},
          { ...settings, taskRoot: root },
          (args) =>
            Effect.sync(() => {
              calls.push([...args]);
              return args[1] === "create"
                ? { conversationId: "session", runId: "run" }
                : {
                    status: "waiting_input",
                    pending: [
                      {
                        threadId: "thread",
                        messageId: "m",
                        blockId: "b",
                        kind: "approval",
                        items: [],
                      },
                    ],
                  };
            }),
          (request, response) =>
            Effect.sync(() => {
              assert.ok(request);
              assert.equal(response.decision, "approve");
            }),
        );
        const session = yield* agent.submit(task);
        assert.equal(calls.find((args) => args[1] === "create")?.[2], taskPrompt(task));
        const current = yield* agent.status(session);
        yield* agent.respond(session, current.requests![0]!, { decision: "approve" });
        assert.equal(
          (yield* agent.followUp(session, { requestId: "id", text: "follow-up" }).pipe(Effect.flip))
            .outcome,
          "rejected",
        );
      }).pipe(Effect.provide(testPlatform)),
    ),
  );
});
test("only recognized wait timeouts may re-query authoritative status", async () => {
  for (const timeout of [false, true])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          let statusReads = 0;
          const agent = yield* makeDoubaoExecutor({}, settings, (args) =>
            args[1] === "wait"
              ? timeout
                ? Effect.fail(new DoubaoWaitTimeout())
                : Effect.succeed({ unexpected: true })
              : Effect.sync(() => {
                  statusReads++;
                  return { status: "running" };
                }),
          );
          const result = yield* agent
            .wait({ sessionId: "session", runId: "run" })
            .pipe(Effect.result);
          assert.equal(result._tag, timeout ? "Success" : "Failure");
          assert.equal(statusReads, timeout ? 1 : 0);
        }).pipe(Effect.provide(testPlatform)),
      ),
    );
  assert.equal(doubaoStatus({ status: "completed", reply: { text: "Done" } }).result?.text, "Done");
});
