import { Cause, Effect, Exit } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { makeDoubaoAgent, doubaoStatus } from "../src/index.js";

test("Doubao retains session/run IDs, checks existing work, and routes native controls separately", async () => {
  const calls: string[][] = [];
  const pending = {
    threadId: "child",
    messageId: "m",
    blockId: "b",
    kind: "approval",
    items: ["Allow"],
  };
  const agent = makeDoubaoAgent(
    undefined,
    async (args) => {
      calls.push(args);
      if (args[1] === "create") return { conversationId: "session", runId: "run" };
      if (args[1] === "status") return { status: "waiting_input", pending: [pending] };
      if (args[1] === "wait") return { status: "waiting_input", pending: [pending] };
      return {};
    },
    async (request, response) => {
      assert.deepEqual(request, pending);
      assert.equal(response.decision, "approve");
      calls.push(["native-response"]);
    },
  );
  const session = await Effect.runPromise(
    agent.submit({
      instructions: "Analyze",
      input: [{ content: "Evidence", sources: ["/source"] }],
    }),
  );
  assert.equal(session.sessionId, "session");
  assert.equal(session.runId, "run");
  assert.deepEqual(await Effect.runPromise(agent.resume(session)), session);
  const state = await Effect.runPromise(agent.wait(session));
  assert.equal(state.state, "waiting_input");
  await Effect.runPromise(agent.respond(session, state.requests![0]!, { decision: "approve" }));
  assert.ok(calls.some((call) => call[0] === "native-response"));
  assert.ok(!calls.some((call) => call[1] === "send"));
  assert.equal(calls.filter((call) => call[1] === "create").length, 1);
  assert.deepEqual(doubaoStatus({ status: "completed", reply: { text: "done" } }), {
    state: "completed",
    result: { text: "done" },
  });
});

test("Doubao cancellation prevents follow-up effects even when the active transport ignores abort", async () => {
  for (const operation of ["submit", "respond", "wait"] as const) {
    const entered = Promise.withResolvers<void>();
    const pending = Promise.withResolvers<any>();
    const calls: string[][] = [];
    let responses = 0;
    const agent = makeDoubaoAgent(
      undefined,
      async (args) => {
        calls.push(args);
        entered.resolve();
        return pending.promise;
      },
      async () => {
        responses++;
      },
    );
    const session = { sessionId: "original", runId: "run" };
    const controller = new AbortController();
    const running = Effect.runPromiseExit(
      operation === "submit"
        ? agent.submit({ instructions: "Read evidence", input: [] })
        : operation === "wait"
          ? agent.wait(session)
          : agent.respond(
              session,
              { id: "child:m:b", kind: "approval", prompt: "Allow?" },
              { decision: "approve" },
            ),
      { signal: controller.signal },
    );
    await entered.promise;
    controller.abort();
    const exit = await running;
    assert.ok(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause));
    if (operation === "wait") pending.reject(new Error("CLI wait ended late"));
    else
      pending.resolve({
        status: "waiting_input",
        pending: [{ threadId: "child", messageId: "m", blockId: "b", kind: "approval" }],
      });
    // Let the ignored transport completion run its Promise continuation before checking
    // that no session creation, approval response or status fallback followed it.
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(calls.length, 1);
    assert.equal(responses, 0);
  }
});

test("Doubao validates pending controls without stripping native response metadata", () => {
  const pending = {
    kind: "input",
    messageId: "m",
    clarifyId: "c",
    nativeBinding: { token: "binding" },
    questions: [
      {
        question_id: "q",
        type: 2,
        title: "Choose",
        nativeField: "retain",
        options: [
          { option_id: "a", text: "A", nativeOption: true },
          { option_id: "b", title: "B" },
        ],
      },
      { question_id: "text", type: 3, question: "Explain" },
    ],
  };
  const status = doubaoStatus({ status: "waiting_input", pending: [pending] });
  assert.deepEqual(status.requests?.[0]?.metadata, pending);
  assert.deepEqual(status.requests?.[0]?.questions, [
    { id: "q", prompt: "Choose", allowOther: false, multiple: true, options: ["A", "B"] },
    { id: "text", prompt: "Explain" },
  ]);
  assert.deepEqual(doubaoStatus({ status: "new-provider-state" }), { state: "unknown" });
  assert.throws(() =>
    doubaoStatus({
      status: "waiting_input",
      pending: [{ kind: "input", messageId: "m", questions: [{ title: "Missing ID" }] }],
    }),
  );
});

test("Malformed Doubao status is a typed adapter failure", async () => {
  const agent = makeDoubaoAgent(undefined, async () => ({
    status: "waiting_input",
    pending: "invalid",
  }));
  const result = await Effect.runPromise(Effect.result(agent.status({ sessionId: "s" })));
  assert.equal(result._tag, "Failure");
  if (result._tag === "Failure") {
    assert.equal(result.failure._tag, "ExternalAgentError");
    assert.equal(result.failure.operation, "status");
  }
});
