import assert from "node:assert/strict";
import { test } from "node:test";
import { taskConversation } from "../src/agent/task/agent.js";

test("Task prompt renders Context metadata as readable text", () => {
  const prompt = taskConversation({
    path: "/goals/project/tasks/example",
    requestId: "request",
    model: "test",
    language: "en-US",
    contexts: [{ path: "/apps/search", description: "Search source" }],
    task: {
      title: "Research",
      instructions: "Find the answer",
      contextPaths: ["/apps/search"],
      input: [],
    },
  }).content;

  assert.match(prompt, /- \/apps\/search/);
  assert.match(prompt, /- \/apps\/search: Search source/);
  assert.doesNotMatch(prompt, /\[\{"path"/);
});
