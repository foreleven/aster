import { CurrentActors } from "../src/tools/actors.js";
import type { CoreTool } from "../src/tools/define.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Schema } from "effect";
import { contextTools } from "../src/tools/catalogues.js";
import { contextView } from "../src/context/view.js";
import { defineContext } from "../src/context/definition.js";
import { makeContextRegistry } from "../src/testing/context.js";
import type { ContextRecord } from "../src/context/storage-format.js";
import { toolSystem } from "./tool-fixtures.js";

const page = (result: Effect.Success<ReturnType<CoreTool["execute"]>>) =>
  JSON.parse(result.content.map((c) => (c.type === "text" ? c.text : "")).join(""));

test("Context tools ask for bounded public pages, include dormant records and detect changed revisions", async () => {
  const records: ContextRecord[] = Array.from({ length: 6000 }, (_, i) => ({
    path: `/lark/im/chats/${i}`,
    description: "Knowledge Engine ".repeat(100),
    state: { summary: "message".repeat(5000), secret: "PRIVATE" },
    messages: [],
  }));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({ loadAll: () => records, save: () => {} });
        const view = contextView({
          matches: (path) => path.startsWith("/lark/"),
          state: Schema.Struct({ summary: Schema.String }),
        });
        yield* registry.views.register([view]);
        const { system } = yield* toolSystem({ registry });
        const [search, read] = contextTools();
        const execute = (tool: CoreTool, args: object) =>
          tool.execute("call", args).pipe(Effect.provideService(CurrentActors, system));
        const found = page(yield* execute(search!, { query: "Knowledge Engine" }));
        assert.equal(found.total, 6000);
        assert.equal(found.items.length, 20);
        assert.equal(found.nextOffset, 20);
        assert.ok(JSON.stringify(found).length < 10000);
        const first = page(yield* execute(read!, { path: records[1]!.path }));
        assert.equal(first.content.length, 12000);
        assert.equal(first.nextOffset, 12000);
        const next = page(
          yield* execute(read!, {
            path: records[1]!.path,
            offset: first.nextOffset,
            revision: first.revision,
          }),
        );
        assert.equal(
          next.content,
          JSON.stringify(registry.reader.get(records[1]!.path)).slice(12000, 24000),
        );
        assert.doesNotMatch(JSON.stringify(registry.reader.get(records[1]!.path)), /PRIVATE/);
        yield* registry.register(
          records[1]!.path,
          defineContext({
            state: Schema.Struct({ summary: Schema.String }),
            message: Schema.Never,
            view,
          }),
        );
        yield* registry.commit(
          { ...records[1]!, state: { summary: "Changed" } },
          { expectedRevision: 0 },
        );
        const conflict = yield* execute(read!, {
          path: records[1]!.path,
          offset: 12000,
          revision: first.revision,
        });
        assert.equal(conflict.isError, true);
        const fresh = page(yield* execute(read!, { path: records[1]!.path }));
        assert.match(fresh.content, /Changed/);
        assert.equal((yield* execute(read!, { path: "/missing" })).isError, true);
      }),
    ),
  );
});
