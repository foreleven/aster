import assert from "node:assert/strict";
import { test } from "node:test";
import { respondInRenderer } from "../src/doubao/native-response.js";

const host = (modules: Record<string, { source: string; exports: unknown }>) => {
  const req = Object.assign((id: string) => modules[id]!.exports, {
    m: Object.fromEntries(
      Object.entries(modules).map(([id, entry]) => [id, { toString: () => entry.source }]),
    ),
  });
  return { "@flow-web/desktop:stable": { push: (chunk: any[]) => chunk[2](req) } };
};
test("native command approval resolves tool identity and grants only the explicit action", async () => {
  const calls: unknown[] = [];
  const runtime = host({
    "1": { source: "cua.local_file.sandbox_instance.update_conversation; var c=r(2)", exports: {} },
    "2": {
      source: "communication",
      exports: {
        _: () => ({
          invoke: async (method: string, payload: unknown) => {
            calls.push({ method, payload });
            return { handled: true, taskId: "native-task" };
          },
        }),
      },
    },
  });
  const request = {
    kind: "approval",
    items: [
      { action_type: 1010, schema_payload: JSON.stringify({ tool_call_id: "tool" }) },
      { action_type: 1011, schema_payload: JSON.stringify({ tool_call_id: "tool" }) },
    ],
  };
  await respondInRenderer({ request, response: { decision: "approve" } }, runtime);
  assert.deepEqual(calls[1], {
    method: "cua.bash_escalation.quick_reply.submit_click",
    payload: { taskId: "native-task", toolCallId: "tool", actionId: "allow" },
  });
  await respondInRenderer({ request, response: { decision: "reject" } }, runtime);
  assert.deepEqual(calls[3], {
    method: "cua.bash_escalation.quick_reply.submit_click",
    payload: { taskId: "native-task", toolCallId: "tool", actionId: "reject" },
  });
  await assert.rejects(
    respondInRenderer(
      {
        request: { kind: "approval", items: [{ action_type: 1017 }] },
        response: { decision: "approve" },
      },
      runtime,
    ),
    /unambiguous/,
  );
});
test("native clarification preserves question IDs and validates choices before submitting", async () => {
  const blocks: any[] = [];
  const runtime = host({
    "3": {
      source: "mH: interaction_ask_submit_rpc_accepted",
      exports: {
        mH: async (block: unknown) => {
          blocks.push(block);
          return true;
        },
      },
    },
  });
  const request = {
    kind: "input",
    clarifyId: "clarify",
    questions: [
      { question_id: "choice", type: 1, options: [{ option_id: "a", text: "A" }] },
      { question_id: "text", type: 3 },
    ],
  };
  await respondInRenderer(
    { request, response: { answers: { choice: ["A"], text: ["Details"] } } },
    runtime,
  );
  assert.equal(blocks[0].clarify_id, "clarify");
  assert.deepEqual(blocks[0].questions[0].answer.selected_option_ids, ["a"]);
  assert.equal(blocks[0].questions[1].answer.capability_answer.text, "Details");
  await assert.rejects(
    respondInRenderer(
      { request, response: { answers: { choice: ["invented"], text: ["Details"] } } },
      runtime,
    ),
    /offered option/,
  );
  assert.equal(blocks.length, 1);
});
