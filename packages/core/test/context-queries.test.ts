import { ContextQueryError } from "../src/context/contracts.js";
import { AgentConversations } from "@aster/agent/harness";
import { CurrentActors } from "../src/services/actors.js";
import type { CoreTool } from "../src/tools/define.js";
import { toolSystem } from "./tool-fixtures.js";
import { ContextsActor } from "../src/context/queries/actor.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Schema, Scope, Exit } from "effect";
import { ContextQueries } from "../src/context/queries/routes.js";
import { contextQueryTools } from "../src/tools/catalogues.js";

const input = { path: "/apps/ctrip", command: "search", args: { query: "Sanya" } };

test("Agent tools retain formatted text and replay it after the command owner retires", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
        const owner = yield* Scope.make();
        const rendered = "## Mail profile\nInbox <alice@example.com>\n".repeat(100);
        let calls = 0;
        yield* queries
          .register(
            "/mail/profile",
            {
              description: "Mail profile",
              commands: {
                profile: {
                  description: "Read profile",
                  schema: Schema.Struct({}),
                  success: Schema.Number,
                  error: Schema.Never,
                  text: () => rendered,
                },
              },
            },
            () => Effect.sync(() => ++calls),
          )
          .pipe(Effect.provideService(Scope.Scope, owner));
        const env = yield* toolSystem({ queries });
        const tools = contextQueryTools("/goals/one", (id) => id);
        const execute = (tool: CoreTool, id: string, args: object) =>
          tool.execute(id, args).pipe(Effect.provideService(CurrentActors, env.system));
        const decodePage = Schema.decodeUnknownSync(
          Schema.Struct({
            resultId: Schema.Int,
            content: Schema.String,
            nextOffset: Schema.NullOr(Schema.Int),
          }),
        );
        const request = { path: "/mail/profile", command: "profile", args: {} };
        const output = yield* execute(tools[0]!, "profile", request);
        const first = decodePage(output.details);
        assert.match(
          output.content[0]!.type === "text" ? output.content[0]!.text : "",
          /\n\n## Mail profile\nInbox/,
        );
        yield* Scope.close(owner, Exit.void);
        assert.deepEqual(
          decodePage((yield* execute(tools[0]!, "profile", request)).details),
          first,
        );
        let text = first.content;
        let offset = first.nextOffset;
        while (offset !== null) {
          const page = decodePage(
            (yield* execute(tools[1]!, "page", {
              resultId: first.resultId,
              offset,
            })).details,
          );
          text += page.content;
          offset = page.nextOffset;
        }
        assert.equal(text, rendered);
        assert.equal(calls, 1);
      }),
    ),
  );
});

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
        yield* queries
          .register(
            input.path,
            {
              description: "Test",
              commands: {
                search: {
                  description: "Search",
                  schema: Schema.Struct({ query: Schema.String }),
                  success: Schema.Json,
                  error: Schema.Never,
                },
              },
            },
            query,
          )
          .pipe(Effect.provideService(Scope.Scope, owner));
        assert.deepEqual(
          Schema.decodeUnknownSync(Schema.Struct({ data: Schema.Array(Schema.Unknown) }))(
            yield* queries.query(input),
          ).data,
          [],
        );
        assert.equal(
          Schema.decodeUnknownSync(ContextQueryError)(
            yield* Effect.flip(
              queries.register(
                input.path,
                {
                  description: "Test",
                  commands: {
                    search: {
                      description: "Search",
                      schema: Schema.Struct({ query: Schema.String }),
                      success: Schema.Json,
                      error: Schema.Never,
                    },
                  },
                },
                query,
              ),
            ),
          ).kind,
          "unavailable",
        );
        assert.equal(
          Schema.decodeUnknownSync(ContextQueryError)(
            yield* Effect.flip(queries.query({ ...input, command: "" })),
          ).kind,
          "invalid-input",
        );
        yield* Scope.close(owner, Exit.void);
        assert.equal(
          Schema.decodeUnknownSync(ContextQueryError)(yield* Effect.flip(queries.query(input)))
            .kind,
          "unavailable",
        );
      }),
    ).pipe(Effect.provide(ContextQueries.layer)),
  );
});

test("query tools retain isolated pages across Actor restart and reject changed operation identity", async () => {
  let calls = 0;
  const data = { note: "Travel evidence ".repeat(2500) };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const messages = yield* AgentConversations.makeMemory();
        const env = yield* toolSystem({
          messages: {
            ...messages,
            read: () => Effect.die("Retained request lookup must not scan the full conversation"),
          },
          queries: {
            register: () => Effect.void,
            list: () => Effect.succeed({ items: [], total: 0, nextOffset: null }),
            describe: () => Effect.succeed({ path: "/test", description: "Test", commands: [] }),
            query: () => Effect.die("Unexpected raw query"),
            json: () => Effect.die("Unexpected transport query"),
            text: (input) =>
              Effect.sync(() => {
                calls++;
                return JSON.stringify({ ...input, queriedAt: "2026-10-04T00:00:00.000Z", data });
              }),
          },
        });
        const makeTools = (owner = "/goals/one") => contextQueryTools(owner, (id) => `query:${id}`);
        const execute = (tool: CoreTool, id: string, args: object) =>
          tool.execute(id, args).pipe(Effect.provideService(CurrentActors, env.system));
        const page = (result: Effect.Success<ReturnType<CoreTool["execute"]>>) =>
          Schema.decodeUnknownSync(
            Schema.Struct({
              resultId: Schema.Int,
              content: Schema.String,
              nextOffset: Schema.NullOr(Schema.Int),
            }),
          )(result.details);
        const tools = makeTools();
        const first = page(yield* execute(tools[0]!, "one", input));
        assert.equal(first.nextOffset, 2000);
        assert.equal(page(yield* execute(tools[0]!, "one", input)).resultId, first.resultId);
        assert.equal(calls, 1);
        assert.equal(
          (yield* execute(tools[0]!, "one", { ...input, args: { query: "changed" } })).isError,
          true,
        );
        const second = page(
          yield* execute(tools[0]!, "two", { ...input, args: { query: "another" } }),
        );
        assert.notEqual(second.resultId, first.resultId);
        yield* env.system.stop(env.contexts);
        const contexts = yield* env.system.spawn("contexts", ContextsActor);
        yield* contexts.awaitStarted;
        const reopened = makeTools();
        let text = first.content;
        let offset: number | null = first.nextOffset;
        while (offset !== null) {
          const next = page(
            yield* execute(reopened[1]!, "page", { resultId: first.resultId, offset }),
          );
          text += next.content;
          offset = next.nextOffset;
        }
        assert.equal(calls, 2);
        assert.deepEqual(JSON.parse(text).data, data);
        // Pi checks both conversation ownership and the custom entry kind.
        const foreign = makeTools("/goals/two");
        const denied = yield* execute(foreign[1]!, "page", {
          resultId: first.resultId,
          offset: 0,
        }).pipe(Effect.exit);
        assert.ok(Exit.isFailure(denied) || denied.value.isError);
        const reply = yield* env.messages.append("/goals/one", "reply", "goal.reply", {
          text: "PRIVATE",
        });
        assert.equal(
          (yield* execute(reopened[1]!, "page", { resultId: reply.id, offset: 0 })).isError,
          true,
        );
      }),
    ),
  );
});
