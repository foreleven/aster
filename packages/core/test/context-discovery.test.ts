import assert from "node:assert/strict";
import { test } from "node:test";
import { contextCatalogue, contextTools } from "../src/reasoning/context-tools.js";
import type { ContextRecord } from "../src/context/storage-format.js";
test("large Context registries stay out of initial prompts and are discoverable in bounded pages", async () => {
  const records: Record<string, ContextRecord> = Object.fromEntries(
    Array.from({ length: 6000 }, (_, i) => {
      const path = `/lark/im/chats/${i}`;
      return [
        path,
        {
          path,
          description: "Knowledge Engine ".repeat(100),
          state: { summary: "message".repeat(5000) },
          messages: [],
        },
      ];
    }),
  );
  assert.ok(JSON.stringify(contextCatalogue(records)).length < 1000);
  const [search, read] = contextTools(records);
  const response = await search.execute("1", { query: "Knowledge Engine" });
  const page = JSON.parse(response.content.map((c) => (c.type === "text" ? c.text : "")).join(""));
  assert.equal(page.total, 6000);
  assert.equal(page.items.length, 20);
  assert.equal(page.nextOffset, 20);
  assert.ok(JSON.stringify(page).length < 10000);
  const result = await read!.execute("2", { path: "/lark/im/chats/1" });
  const chunk = JSON.parse(result.content.map((c) => (c.type === "text" ? c.text : "")).join(""));
  assert.equal(chunk.content.length, 12000);
  assert.equal(chunk.nextOffset, 12000);
  const next = await read!.execute("3", { path: "/lark/im/chats/1", offset: chunk.nextOffset });
  const nextChunk = JSON.parse(next.content.map((c) => (c.type === "text" ? c.text : "")).join(""));
  assert.equal(nextChunk.content, JSON.stringify(records["/lark/im/chats/1"]).slice(12000, 24000));
});
