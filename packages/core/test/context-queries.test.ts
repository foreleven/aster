import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Schema, Scope, Exit } from "effect";
import { ContextQueries } from "../src/context/queries.js";
import { contextQueryTools } from "../src/context/query-tools.js";
import type { AgentTool } from "@aster/agent";

const input = { path: "/apps/ctrip", command: "search", args: { query: "Sanya" } };
test("Context query routes follow ownership scopes and reject unavailable/invalid requests", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const queries = yield* ContextQueries;
        const owner = yield* Scope.make();
        const query = () =>
          Effect.succeed({
            path: input.path,
            command: input.command,
            queriedAt: "2026-10-04T00:00:00.000Z",
            data: [],
          });
        yield* queries.register(input.path, query).pipe(Effect.provideService(Scope.Scope, owner));
        assert.deepEqual((yield* queries.query(input)).data, []);
        assert.equal((yield* Effect.flip(queries.register(input.path, query))).kind, "unavailable");
        assert.equal(
          (yield* Effect.flip(queries.query({ ...input, command: "" }))).kind,
          "invalid-input",
        );
        yield* Scope.close(owner, Exit.void);
        assert.equal((yield* Effect.flip(queries.query(input))).kind, "unavailable");
      }),
    ).pipe(Effect.provide(ContextQueries.layer)),
  );
});

test("query tools retain paginated results per evaluation without repeating external calls", async () => {
  let calls = 0;
  const data = { note: "Travel evidence ".repeat(2500) };
  const tools: readonly AgentTool[] = contextQueryTools(
    {
      register: () => Effect.void,
      query: (input) =>
        Effect.sync(() => {
          calls++;
          return { ...input, queriedAt: "2026-10-04T00:00:00.000Z", data };
        }),
    },
    (effect, signal) => Effect.runPromise(effect, { signal }),
  );
  const pageSchema = Schema.fromJsonString(
    Schema.Struct({ content: Schema.String, nextOffset: Schema.NullOr(Schema.Number) }),
  );
  const page = (result: Awaited<ReturnType<AgentTool["execute"]>>) =>
    Schema.decodeUnknownSync(pageSchema)(
      result.content.map((entry) => (entry.type === "text" ? entry.text : "")).join(""),
    );
  const first = page(await tools[0]!.execute("1", input));
  assert.equal(first.nextOffset, 2000);
  let text = first.content;
  let offset: number | null = first.nextOffset;
  while (offset !== null) {
    const next = page(await tools[1]!.execute("2", { path: input.path, offset }));
    text += next.content;
    offset = next.nextOffset;
  }
  assert.equal(calls, 1);
  assert.deepEqual(JSON.parse(text).data, data);
  assert.deepEqual(
    contextQueryTools(undefined, (effect, signal) => Effect.runPromise(effect, { signal })),
    [],
  );
});
