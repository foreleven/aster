import assert from "node:assert/strict";
import { test } from "node:test";
import { AgentConversations } from "@aster/agent/harness";
import { Context, Deferred, Effect, Exit, Fiber, Layer, Schema, Scope } from "effect";
import { ContextQueries } from "../src/queries/registry.js";
import { ContextQueryError, ContextQueryInput, type QueryPage } from "../src/queries/contracts.js";
import { ApplicationError } from "../src/operations.js";
import { queryContext, readQueryResult } from "../src/tools/task/contexts.js";
import { ToolInvocation } from "../src/tools/invocation.js";
import { QueryExecution } from "../src/queries/execution.js";

const input = { path: "/apps/test", command: "search", args: { query: "first" } };
const definition = {
  description: "Test",
  commands: {
    search: {
      description: "Search",
      schema: Schema.Struct({ query: Schema.String }),
      success: Schema.String,
      error: Schema.Never,
      text: (value: unknown) => String(value),
    },
  },
};
const setup = Effect.gen(function* () {
  const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
  const messages = yield* AgentConversations.makeMemory();
  const execution = yield* Layer.build(QueryExecution.layer).pipe(
    Effect.map(Context.get(QueryExecution)),
    Effect.provideService(ContextQueries, queries),
    Effect.provideService(AgentConversations, messages),
  );
  return { queries, messages, execution };
});
const run = <A, E>(work: Effect.Effect<A, E, Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(work).pipe(Effect.timeout("8 seconds")));

test("Context evidence commits before acknowledgement, replays after retirement and isolates owners", async () => {
  await run(
    Effect.gen(function* () {
      const { queries, messages, execution } = yield* setup;
      const owner = yield* Scope.make();
      let calls = 0;
      const text = "## Evidence\n".repeat(500);
      yield* queries
        .register(input.path, definition, () =>
          Effect.sync(() => {
            calls++;
            return text;
          }),
        )
        .pipe(Effect.provideService(Scope.Scope, owner));
      const first = yield* execution.query("/tasks/one", "query:1", input);
      assert.equal((yield* messages.get("/tasks/one", first.resultId)).kind, "tool.query-result");
      assert.equal(first.nextOffset, 2000);
      yield* Scope.close(owner, Exit.void);
      assert.deepEqual(yield* execution.query("/tasks/one", "query:1", input), first);
      let collected = first.content;
      let offset: number | null = first.nextOffset;
      while (offset !== null) {
        const page: QueryPage = yield* execution.page("/tasks/one", first.resultId, offset);
        collected += page.content;
        offset = page.nextOffset;
      }
      assert.equal(collected, text);
      assert.equal(calls, 1);
      assert.equal(
        Schema.decodeUnknownSync(ApplicationError)(
          yield* Effect.flip(
            execution.query("/tasks/one", "query:1", { ...input, args: { query: "changed" } }),
          ),
        ).kind,
        "conflict",
      );
      assert.ok(
        Exit.isFailure(yield* Effect.exit(execution.page("/tasks/two", first.resultId, 0))),
      );
      for (const invalid of [-1, 0.5, text.length + 1])
        assert.equal(
          (yield* Effect.flip(execution.page("/tasks/one", first.resultId, invalid))).kind,
          "invalid-input",
        );
      const privateEntry = yield* messages.append("/tasks/one", "reply", "task.reply", {
        text: "private",
      });
      assert.equal(
        (yield* Effect.flip(execution.page("/tasks/one", privateEntry.id, 0))).kind,
        "not-found",
      );
    }),
  );
});

test("Context capacity distinguishes duplicate identities and releases on caller interruption", async () => {
  await run(
    Effect.gen(function* () {
      const { queries, execution, messages } = yield* setup;
      const entered = yield* Deferred.make<void>();
      let calls = 0;
      yield* queries.register(input.path, definition, () =>
        Effect.gen(function* () {
          calls++;
          if (calls === 4) yield* Deferred.succeed(entered, undefined);
          return yield* Effect.never;
        }),
      );
      const workers = yield* Effect.forEach([1, 2, 3, 4], (id) =>
        execution.query("/tasks/one", `query:${id}`, input).pipe(Effect.forkScoped),
      );
      yield* Deferred.await(entered);
      assert.equal(
        Schema.decodeUnknownSync(ContextQueryError)(
          yield* Effect.flip(execution.query("/tasks/one", "query:1", input)),
        ).kind,
        "duplicate",
      );
      assert.equal(
        Schema.decodeUnknownSync(ContextQueryError)(
          yield* Effect.flip(execution.query("/tasks/one", "query:5", input)),
        ).kind,
        "busy",
      );
      assert.equal((yield* execution.list()).total, 1);
      yield* Fiber.interrupt(workers[0]!);
      yield* queries.register("/apps/fast", definition, () => Effect.succeed("fast"));
      assert.equal(
        (yield* execution.query("/tasks/one", "query:5", { ...input, path: "/apps/fast" })).content,
        "fast",
      );
      assert.equal((yield* messages.read("/tasks/one")).length, 1);
      yield* Effect.forEach(workers.slice(1), Fiber.interrupt);
    }),
  );
});

test("Owner retirement cancels source work; empty and oversized evidence have explicit behavior", async () => {
  await run(
    Effect.gen(function* () {
      const { queries, execution, messages } = yield* setup;
      const owner = yield* Scope.make();
      const entered = yield* Deferred.make<void>();
      const released = yield* Deferred.make<void>();
      yield* queries
        .register(input.path, definition, () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(released, undefined)),
          ),
        )
        .pipe(Effect.provideService(Scope.Scope, owner));
      const worker = yield* execution.query("/tasks/one", "retired", input).pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      yield* Scope.close(owner, Exit.void);
      yield* Deferred.await(released);
      assert.equal(
        Schema.decodeUnknownSync(ContextQueryError)(yield* Effect.flip(Fiber.join(worker))).kind,
        "unavailable",
      );
      assert.equal((yield* messages.read("/tasks/one")).length, 0);
      yield* queries.register(input.path, definition, (raw) =>
        Effect.succeed(raw.args.query === "large" ? "a".repeat(1_000_001) : ""),
      );
      const empty = yield* execution.query("/tasks/one", "empty", input);
      assert.equal(empty.content, "");
      assert.equal(empty.nextOffset, null);
      assert.equal(
        Schema.decodeUnknownSync(ApplicationError)(
          yield* Effect.flip(
            execution.query("/tasks/one", "large", { ...input, args: { query: "large" } }),
          ),
        ).kind,
        "invalid-input",
      );
    }),
  );
});

test("Task query tools bind ownership and default empty arguments without Actor envelopes", async () => {
  await run(
    Effect.gen(function* () {
      const { queries, execution } = yield* setup;
      yield* queries.register(
        "/apps/empty",
        {
          description: "Empty",
          commands: {
            inspect: {
              description: "Read",
              schema: Schema.Struct({}),
              success: Schema.String,
              error: Schema.Never,
              text: (value) => String(value),
            },
          },
        },
        (raw) => {
          assert.deepEqual(raw.args, {});
          return Effect.succeed("retained");
        },
      );
      const binding = {
        ownerPath: "/tasks/reader",
        inputId: "accepted",
        acceptedAt: "2026-10-11T00:00:00Z",
        remainingAgentTurns: 1,
      };
      const output = yield* queryContext()
        .execute("query", { path: "/apps/empty", command: "inspect" })
        .pipe(
          Effect.provideService(ToolInvocation, binding),
          Effect.provideService(QueryExecution, execution),
        );
      assert.match(output.content[0]!.type === "text" ? output.content[0]!.text : "", /retained/);
      const page = Schema.decodeUnknownSync(Schema.Struct({ resultId: Schema.Int }))(
        output.details,
      );
      const denied = yield* readQueryResult()
        .execute("page", { resultId: page.resultId, offset: 0 })
        .pipe(
          Effect.provideService(ToolInvocation, { ...binding, ownerPath: "/tasks/other" }),
          Effect.provideService(QueryExecution, execution),
        );
      assert.equal(denied.isError, true);
    }),
  );
});

test("Runtime query scope retirement interrupts reads without committing evidence", async () => {
  await run(
    Effect.gen(function* () {
      const runtimeScope = yield* Scope.make();
      const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
      const messages = yield* AgentConversations.makeMemory();
      const execution = yield* Layer.build(QueryExecution.layer).pipe(
        Effect.map(Context.get(QueryExecution)),
        Effect.provideService(ContextQueries, queries),
        Effect.provideService(AgentConversations, messages),
        Effect.provideService(Scope.Scope, runtimeScope),
      );
      const entered = yield* Deferred.make<void>();
      const released = yield* Deferred.make<void>();
      yield* queries.register(input.path, definition, () =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(Deferred.succeed(released, undefined)),
        ),
      );
      const caller = yield* execution
        .query("/tasks/one", "shutdown", input)
        .pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      yield* Scope.close(runtimeScope, Exit.void);
      yield* Deferred.await(released);
      assert.ok(Exit.isFailure(yield* Fiber.await(caller)));
      assert.deepEqual(yield* messages.read("/tasks/one"), []);
    }),
  );
});

test("Context queries canonicalize defaults and codecs once and replay both committed input forms", async () => {
  await run(
    Effect.gen(function* () {
      const { queries, execution, messages } = yield* setup;
      const routeScope = yield* Scope.make();
      const argumentsSchema = Schema.Struct({
        query: Schema.Trim,
        count: Schema.NumberFromString,
        limit: Schema.Int.pipe(Schema.withDecodingDefaultKey(Effect.succeed(20))),
      });
      const original = {
        path: "/apps/normalize",
        command: "read",
        args: { query: "  first  ", count: "01" },
      };
      const canonical = { ...original, args: { query: "first", count: "1", limit: 20 } };
      let reads = 0;
      yield* queries
        .register(
          original.path,
          {
            description: "Canonical source",
            commands: {
              read: {
                description: "Read",
                schema: argumentsSchema,
                success: Schema.String,
                error: Schema.Never,
                text: (value) => String(value),
              },
            },
          },
          (received) =>
            Effect.gen(function* () {
              reads++;
              assert.deepEqual(received, canonical);
              // The owner decodes the encoded command once; transformed numbers must not be passed here.
              assert.deepEqual(yield* Schema.decodeUnknownEffect(argumentsSchema)(received.args), {
                query: "first",
                count: 1,
                limit: 20,
              });
              return "canonical evidence";
            }),
        )
        .pipe(Effect.provideService(Scope.Scope, routeScope));
      assert.deepEqual(yield* queries.normalize(original), canonical);
      const first = yield* execution.query("/tasks/canonical", "one", original);
      assert.deepEqual(
        yield* execution.query("/tasks/canonical", "one", {
          ...original,
          args: { ...original.args, limit: 20 },
        }),
        first,
      );
      assert.deepEqual(
        yield* execution.query("/tasks/canonical", "one", {
          ...original,
          args: { query: " first ", count: "1.0" },
        }),
        first,
      );
      assert.equal(reads, 1);
      const saved = Schema.decodeUnknownSync(
        Schema.Struct({ input: ContextQueryInput, originalInput: ContextQueryInput }),
      )((yield* messages.get("/tasks/canonical", first.resultId)).data);
      assert.deepEqual(saved, { input: canonical, originalInput: original });
      assert.equal(
        Schema.decodeUnknownSync(ApplicationError)(
          yield* Effect.flip(
            execution.query("/tasks/canonical", "one", {
              ...canonical,
              args: { ...canonical.args, count: "2" },
            }),
          ),
        ).kind,
        "conflict",
      );
      yield* Scope.close(routeScope, Exit.void);
      assert.deepEqual(yield* execution.query("/tasks/canonical", "one", original), first);
      assert.deepEqual(yield* execution.query("/tasks/canonical", "one", canonical), first);
      const retired = Schema.decodeUnknownSync(ApplicationError)(
        yield* Effect.flip(
          execution.query("/tasks/canonical", "one", {
            ...canonical,
            args: { query: "first ", count: "1e0", limit: 20 },
          }),
        ),
      );
      assert.equal(retired.kind, "conflict");
      assert.match(retired.message, /exact original or canonical/);
      assert.equal(reads, 1);
    }),
  );
});
